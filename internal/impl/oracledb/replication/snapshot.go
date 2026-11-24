// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	goora "github.com/sijms/go-ora/v2/network"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/sqlutil"
)

const (
	// errCodeInvalidIdentifier (ORA-00904) flags a custom snapshot filter that doesn't
	// select every primary key column.
	errCodeInvalidIdentifier = 904
	// errCodeFlashbackTableChanged (ORA-01466) is a transient AS OF SCN error raised just
	// after the SCN is captured; see withFlashbackRetry.
	errCodeFlashbackTableChanged = 1466
	// errCodeInsufficientPrivileges (ORA-01031) on an AS OF SCN read usually means a missing
	// FLASHBACK grant; see isMissingFlashbackPrivilege.
	errCodeInsufficientPrivileges = 1031
	// errCodeMissingFlashbackPrivilege (ORA-41900) is how Oracle 23ai reports a missing FLASHBACK grant.
	errCodeMissingFlashbackPrivilege = 41900

	// maxPlanningWorkers caps the worker count chunk planning sizes its queries for, bounding them
	// to 4,096 interpolated ranges or 8,192 sampled keys (few DBs support more sessions than this).
	maxPlanningWorkers = 256
	// minRowsPerChunk is the fewest rows a chunk is planned to hold, so a chunk's connection
	// setup and planning share stay small next to the rows it reads.
	minRowsPerChunk = 10_000
)

// errNoPrimaryKey means a table has no primary key, so planTableTasks falls back to a full scan.
var errNoPrimaryKey = errors.New("no primary key found")

// Snapshot reads the existing rows of the configured tables as of a single SCN.
type Snapshot struct {
	tables                  []UserTable
	filters                 map[string]string
	publisher               ChangePublisher
	db                      *snapshotDB
	log                     *service.Logger
	snapshotStatusMetric    *service.MetricGauge
	snapshotRowsTotalMetric *service.MetricCounter
	lobEnabled              bool
	scn                     SCN       // published as part of snapshot read metadata
	commitTimestamp         time.Time // Oracle server time at snapshot prepare
}

// NewSnapshot returns a Snapshot of tables that publishes rows to publisher.
func NewSnapshot(ctx context.Context,
	connectionString string,
	tables []UserTable,
	filters map[string]string,
	publisher ChangePublisher,
	lobEnabled bool,
	pdbName string,
	connBackoff *backoff.ExponentialBackOff,
	logger *service.Logger,
	metrics *service.Metrics,
) (*Snapshot, error) {
	db, err := newSnapshotDB(ctx, connectionString, pdbName, connBackoff, logger)
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		db:                      db,
		tables:                  tables,
		filters:                 filters,
		publisher:               publisher,
		lobEnabled:              lobEnabled,
		snapshotStatusMetric:    metrics.NewGauge("oracledb_cdc_snapshot_status", "table"),
		snapshotRowsTotalMetric: metrics.NewCounter("oracledb_cdc_snapshot_rows_total", "table"),
		log:                     logger,
		scn:                     InvalidSCN,
	}, nil
}

// Prepare captures and returns the current SCN that every snapshot read is pinned to.
func (s *Snapshot) Prepare(ctx context.Context) (SCN, error) {
	if len(s.tables) == 0 {
		return InvalidSCN, errors.New("no tables provided")
	}

	scn, ts, err := s.db.currentSCN(ctx)
	if err != nil {
		return InvalidSCN, fmt.Errorf("getting current SCN for snapshot: %w", err)
	}
	s.scn, s.commitTimestamp = scn, ts

	s.log.Infof("Captured SCN before snapshot at SCN: %s", s.scn)
	return s.scn, nil
}

// Read snapshots every table on up to maxWorkers workers, publishing each row as a
// MessageEvent, with large tables split into key-range tasks that share the workers.
func (s *Snapshot) Read(ctx context.Context, maxWorkers, maxBatchSize int) error {
	if maxWorkers <= 0 {
		return fmt.Errorf("snapshot max workers must be greater than 0, got %d", maxWorkers)
	}
	if maxBatchSize <= 0 {
		return fmt.Errorf("snapshot max batch size must be greater than 0, got %d", maxBatchSize)
	}

	s.log.Infof("Starting snapshot of %d table(s) using %d configured readers", len(s.tables), maxWorkers)

	for _, table := range s.tables {
		s.snapshotStatusMetric.Set(0, table.FullName())
	}

	// Tasks start in list order, so tables that can't be chunked go first: their single
	// serial scan then overlaps the chunk work instead of running alone at the tail.
	var serialTasks, chunkTasks []func(context.Context) error
	for _, table := range s.tables {
		tableTasks, err := s.planTableTasks(ctx, table, maxWorkers, maxBatchSize)
		if err != nil {
			return fmt.Errorf("planning snapshot for table '%s': %w", table.FullName(), err)
		}
		if len(tableTasks) == 1 {
			serialTasks = append(serialTasks, tableTasks...)
		} else {
			chunkTasks = append(chunkTasks, tableTasks...)
		}
	}
	snapshotTasks := append(serialTasks, chunkTasks...)

	s.log.Debugf("Snapshot planned as %d task(s) across %d table(s), running on %d of %d configured workers", len(snapshotTasks), len(s.tables), min(len(snapshotTasks), maxWorkers), maxWorkers)

	s.db.setWorkers(maxWorkers)

	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(maxWorkers)
	for _, task := range snapshotTasks {
		wg.Go(func() error { return task(ctx) })
	}

	if err := wg.Wait(); err != nil {
		return fmt.Errorf("processing snapshots: %w", err)
	}

	return nil
}

// Close closes the snapshot's database connections.
func (s *Snapshot) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// tableCompletionTracker marks a table complete once all of its tasks have succeeded.
type tableCompletionTracker struct {
	remaining atomic.Int32
	rows      atomic.Int64
	snapshot  *Snapshot
	table     UserTable
}

func newTableCompletionTracker(s *Snapshot, table UserTable, taskCount int) *tableCompletionTracker {
	t := &tableCompletionTracker{snapshot: s, table: table}
	t.remaining.Store(int32(taskCount))
	return t
}

// done records a successful task's rows, marking the table complete after its last task.
func (t *tableCompletionTracker) done(rowsProcessed int) {
	t.rows.Add(int64(rowsProcessed))
	if t.remaining.Add(-1) == 0 {
		t.snapshot.snapshotStatusMetric.Set(1, t.table.FullName())
		t.snapshot.log.With("src_table", t.table.FullName()).Infof("Table snapshot completed, %d rows processed", t.rows.Load())
	}
}

// planTableTasks returns the tasks that snapshot table: one keyset task for a filtered
// table, key-range chunks for an enforced primary key that can be split, else one full scan.
func (s *Snapshot) planTableTasks(ctx context.Context, table UserTable, maxWorkers, maxBatchSize int) ([]func(context.Context) error, error) {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	customQuery, hasFilter := s.filters[tableName]

	if hasFilter {
		l.Infof("Launching snapshot of table '%s' with snapshot filter", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableWithFilterTask(table, maxBatchSize, customQuery, tracker)}, nil
	}

	// fullScan is the single-task fallback for tables that can't be chunked (1 worker config, no pks etc)
	fullScan := func() []func(context.Context) error {
		l.Infof("Launching snapshot of table '%s'", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableFullScanTask(table, maxBatchSize, tracker)}
	}

	// Skip PK lookup and keyspace sampling entirely for single worker
	if maxWorkers <= 1 {
		return fullScan(), nil
	}

	var (
		pks      []string
		enforced bool
	)
	if err := s.db.withConn(ctx, func(db *sql.Conn) (err error) {
		pks, enforced, err = getTablePrimaryKeys(ctx, db, table)
		return err
	}); err != nil {
		if !errors.Is(err, errNoPrimaryKey) {
			return nil, err
		}
		return fullScan(), nil
	}

	// Chunk bounds and keyset pages assume every row has a unique, non-null key. Oracle
	// only guarantees that for an ENABLED VALIDATED primary key: a DISABLE or NOVALIDATE
	// key (e.g. the RELY DISABLE NOVALIDATE keys common in warehouse schemas) can hold
	// NULLs, which match no range bound and would be silently dropped, and duplicates,
	// which the strict lower bound skips when they straddle a page boundary.
	if !enforced {
		l.Infof("Primary key of table '%s' is not enabled and validated, so it will be snapshotted without parallel chunking", tableName)
		return fullScan(), nil
	}

	ranges, method, err := s.planChunkRanges(ctx, table, pks, maxWorkers)
	if err != nil {
		// Without FLASHBACK the table can still be read without AS OF SCN, as when chunking is off.
		if oraErr, ok := isMissingFlashbackPrivilege(err); ok {
			l.Warnf("Table '%s' can't be split into parallel chunks without the FLASHBACK privilege, so it will be snapshotted with a full scan. "+
				"Grant it with GRANT FLASHBACK ON %s TO <user> or GRANT FLASHBACK ANY TABLE TO <user> to split it: %v", tableName, tableName, oraErr)
			return fullScan(), nil
		}
		return nil, err
	}
	if len(ranges) <= 1 {
		return fullScan(), nil
	}

	l.Infof("Launching snapshot of table '%s' split into %d parallel chunks by %s", tableName, len(ranges), method)

	tracker := newTableCompletionTracker(s, table, len(ranges))
	tasks := make([]func(context.Context) error, 0, len(ranges))
	for _, r := range ranges {
		tasks = append(tasks, s.scanTableRangeTask(table, pks, r.min, r.max, maxBatchSize, tracker))
	}
	return tasks, nil
}

// chunkRange is a (min, max] primary key range of a table, where a nil bound is unbounded.
// The lower bound is exclusive so the first page of a chunk uses the same "> lastSeen" keyset
// predicate as every later page.
type chunkRange struct {
	min map[string]any
	max map[string]any
}

// planChunkRanges splits table's keyspace into ranges, interpolating single numeric or date
// keys between MIN and MAX and sampling the rest, and returns the method used for logs.
func (s *Snapshot) planChunkRanges(ctx context.Context, table UserTable, pkCols []string, maxWorkers int) (ranges []chunkRange, method string, err error) {
	const interpolatedRangesPerWorker = 16
	plan := func(db *sql.Conn) error {
		numRows, err := getTableRowCount(ctx, db, table)
		if err != nil {
			return err
		}
		keyType, keyTypeKnown := keyTypeNotInterpolable, false
		lookupKeyType := func() (err error) {
			if !keyTypeKnown && len(pkCols) == 1 {
				keyType, err = getInterpolableKeyType(ctx, db, table, pkCols[0])
			}
			keyTypeKnown = true
			return err
		}
		// Without statistics a key that must be sampled can't be split, so don't count its rows.
		if numRows == 0 {
			if err := lookupKeyType(); err != nil {
				return err
			}
			if keyType == keyTypeNotInterpolable {
				ranges, err = s.sampleChunkRanges(ctx, db, table, pkCols, numRows, maxWorkers)
				return err
			}
		}
		// When statistics are missing or say the table is too small to chunk, which they can if
		// they're stale, count rows up to the most that every planned range could need.
		wantRanges := min(maxWorkers, maxPlanningWorkers) * interpolatedRangesPerWorker
		maxRanges := maxChunkRanges(numRows)
		if maxRanges < 2 {
			probed, err := countRowsUpTo(ctx, db, table, s.scn, wantRanges*minRowsPerChunk)
			if err != nil {
				return err
			}
			maxRanges = maxChunkRanges(probed)
			// Size any sample from the count, which is exact below its limit and a far closer lower
			// bound than stale statistics above it. Tables without statistics still aren't sampled.
			if numRows > 0 {
				numRows = probed
			}
		}
		if maxRanges < 2 {
			s.log.With("src_table", table.FullName()).Debugf("Table '%s' has fewer than %d rows, so it will be snapshotted without parallel chunking", table.FullName(), 2*minRowsPerChunk)
			ranges = []chunkRange{{}}
			return nil
		}

		if err := lookupKeyType(); err != nil {
			return err
		}
		if keyType != keyTypeNotInterpolable {
			numRanges := min(wantRanges, maxRanges)
			bounds, err := interpolateKeyBounds(ctx, db, table, pkCols[0], keyType, s.scn, numRanges)
			if err != nil {
				return err
			}
			occupied, err := countOccupiedRanges(ctx, db, table, pkCols[0], keyType, s.scn, numRanges, len(bounds))
			if err != nil {
				return err
			}
			// Equal-width ranges only balance when keys are spread across MIN..MAX. An
			// outlier (e.g. a sentinel ID far beyond the rest) leaves most ranges empty
			// and every real row in a few of them, so sample the keyspace instead.
			if occupied*2 >= len(bounds)+1 {
				ranges, method = rangesFromBounds(bounds), "interpolating its key range"
				return nil
			}
			s.log.With("src_table", table.FullName()).Infof("Only %d of %d interpolated key ranges of table '%s' hold rows, so its key range will be sampled instead", occupied, len(bounds)+1, table.FullName())
		}
		ranges, err = s.sampleChunkRanges(ctx, db, table, pkCols, numRows, min(maxWorkers, maxRanges))
		method = "sampling its key range"
		return err
	}

	if err = s.db.withConn(ctx, plan); err != nil {
		return nil, "", fmt.Errorf("planning chunk ranges: %w", err)
	}
	return ranges, method, nil
}

// sampleChunkRanges splits table's keyspace into up to maxWorkers even ranges by sampling
// rows, returning one unbounded range when the table has no optimizer statistics (numRows 0).
func (s *Snapshot) sampleChunkRanges(ctx context.Context, db DBQuerier, table UserTable, pkCols []string, numRows int64, maxWorkers int) ([]chunkRange, error) {
	const (
		// overSampleFactor is the number of primary key samples drawn per worker when planning
		// chunks: enough to split the keyspace evenly while keeping the sample query cheap.
		overSampleFactor = 32
	)
	numSamples := min(maxWorkers, maxPlanningWorkers) * overSampleFactor

	if numRows <= 0 {
		s.log.With("src_table", table.FullName()).Infof("Table '%s' has no optimizer statistics, so it will be snapshotted without parallel chunking; gather statistics with DBMS_STATS.GATHER_TABLE_STATS to enable it", table.FullName())
		return []chunkRange{{}}, nil
	}
	sampleClause := fmt.Sprintf("SAMPLE (%.4f)", samplePercent(numSamples, numRows))
	samples, err := sampleTableKeyspace(ctx, db, table, pkCols, s.scn, sampleClause, numSamples)
	if err != nil {
		return nil, err
	}
	return splitChunkRanges(samples, maxWorkers), nil
}

// splitChunkRanges splits sorted key samples into at most maxWorkers ranges covering the whole keyspace.
func splitChunkRanges(samples []map[string]any, maxWorkers int) []chunkRange {
	// Round up so there are at most maxWorkers ranges. max(1, ...) avoids chunkSize=0
	// when there are no samples, which would otherwise loop forever below.
	chunkSize := max(1, (len(samples)+maxWorkers-1)/maxWorkers)
	var bounds []map[string]any
	for i := chunkSize; i < len(samples); i += chunkSize {
		bounds = append(bounds, samples[i])
	}
	return rangesFromBounds(bounds)
}

// rangesFromBounds turns sorted, distinct bounds into contiguous ranges covering the whole keyspace.
func rangesFromBounds(bounds []map[string]any) []chunkRange {
	ranges := make([]chunkRange, 0, len(bounds)+1)
	var prev map[string]any
	for _, bound := range bounds {
		ranges = append(ranges, chunkRange{min: prev, max: bound})
		prev = bound
	}
	return append(ranges, chunkRange{min: prev, max: nil})
}

// keyType classifies a primary key column by how interpolateKeyBounds can split it.
type keyType int

const (
	keyTypeNotInterpolable keyType = iota
	keyTypeInteger
	keyTypeNumber
	keyTypeDatetime
)

// getInterpolableKeyType reports whether column is a NUMBER, FLOAT, DATE or TIMESTAMP that
// can be interpolated between its MIN and MAX.
func getInterpolableKeyType(ctx context.Context, db DBQuerier, table UserTable, column string) (keyType, error) {
	var (
		dataType string
		scale    sql.NullInt64
	)
	if err := db.QueryRowContext(ctx,
		`SELECT DATA_TYPE, DATA_SCALE FROM ALL_TAB_COLUMNS WHERE OWNER = :1 AND TABLE_NAME = :2 AND COLUMN_NAME = :3`,
		table.Schema, table.Name, column,
	).Scan(&dataType, &scale); err != nil {
		return keyTypeNotInterpolable, fmt.Errorf("getting type of column %s of table '%s': %w", column, table.FullName(), err)
	}
	switch {
	case dataType == "NUMBER" && scale.Valid && scale.Int64 == 0:
		return keyTypeInteger, nil
	case dataType == "NUMBER", dataType == "FLOAT":
		return keyTypeNumber, nil
	case dataType == "DATE", strings.HasPrefix(dataType, "TIMESTAMP") && !strings.Contains(dataType, "TIME ZONE"):
		return keyTypeDatetime, nil
	default:
		return keyTypeNotInterpolable, nil
	}
}

// interpolateKeyBounds returns the sorted bounds that split column's MIN to MAX as of scn
// into numRanges equal-width ranges, or none if the table has fewer than two keys.
func interpolateKeyBounds(ctx context.Context, db DBQuerier, table UserTable, column string, kt keyType, scn SCN, numRanges int) ([]map[string]any, error) {
	return queryKeyspace(ctx, db, table, interpolatedBoundsQuery(table, column, kt, scn, numRanges)+` ORDER BY 1`)
}

// countOccupiedRanges returns how many interpolated ranges hold a row as of scn, by counting
// the distinct first keys after each bound with cheap index probes.
func countOccupiedRanges(ctx context.Context, db DBQuerier, table UserTable, column string, kt keyType, scn SCN, numRanges, numBounds int) (int, error) {
	if numBounds == 0 {
		return 1, nil
	}
	q := fmt.Sprintf(`SELECT COUNT(DISTINCT "N") FROM (
		SELECT (SELECT MIN("%[1]s") FROM "%[2]s"."%[3]s" AS OF SCN %[4]d WHERE "%[1]s" > "BOUNDS"."%[1]s") "N"
		FROM (%[5]s) "BOUNDS"
	)`, column, table.Schema, table.Name, uint64(scn), interpolatedBoundsQuery(table, column, kt, scn, numRanges))

	nextKeys, err := withFlashbackRetry(ctx, func() (n int, err error) {
		err = db.QueryRowContext(ctx, q).Scan(&n)
		return n, err
	})
	if err != nil {
		return 0, fmt.Errorf("counting occupied key ranges of table '%s': %w", table.FullName(), withFlashbackPrivilegeHint(err, table))
	}
	return 1 + nextKeys, nil
}

// interpolatedBoundsQuery returns the unordered query behind interpolateKeyBounds.
func interpolatedBoundsQuery(table UserTable, column string, kt keyType, scn SCN, numRanges int) string {
	bound := `"LO" + ("HI" - "LO") * (LEVEL / %[1]d)`
	if kt == keyTypeInteger {
		bound = `FLOOR(` + bound + `)`
	}
	q := `SELECT DISTINCT "B" AS "%[2]s" FROM (
		SELECT ` + bound + ` "B", "LO", "HI"
		FROM (SELECT MIN("%[2]s") "LO", MAX("%[2]s") "HI" FROM "%[3]s"."%[4]s" AS OF SCN %[5]d)
		WHERE "LO" < "HI" CONNECT BY LEVEL < %[1]d
	) WHERE "B" > "LO" AND "B" < "HI"`
	return fmt.Sprintf(q, max(numRanges, 1), column, table.Schema, table.Name, uint64(scn))
}

// snapshotTableWithFilterTask returns a task that pages through table's custom filter query
// by primary key in a read-only transaction, without chunking.
func (s *Snapshot) snapshotTableWithFilterTask(table UserTable, maxBatchSize int, customQuery string, tracker *tableCompletionTracker) func(context.Context) error {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.db.withReadOnlyTxn(ctx, func(tx *sql.Tx) error {
			tablePks, _, err := getTablePrimaryKeys(ctx, tx, table)
			if err != nil {
				return err
			}
			l.Debugf("Found primary keys for table '%s': %v", table, tablePks)

			numRowsProcessed, err = s.pageKeyset(ctx, tx, table, tablePks, keysetScan{customQuery: customQuery, scn: InvalidSCN}, maxBatchSize)
			return err
		})
		if err != nil {
			return err
		}
		tracker.done(numRowsProcessed)
		return nil
	}
}

// snapshotTableFullScanTask returns a task that reads an unchunked table in one full scan.
func (s *Snapshot) snapshotTableFullScanTask(table UserTable, maxBatchSize int, tracker *tableCompletionTracker) func(context.Context) error {
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.db.withReadOnlyTxn(ctx, func(tx *sql.Tx) (err error) {
			numRowsProcessed, err = s.snapshotTableFullScan(ctx, tx, table, maxBatchSize)
			return err
		})
		if err != nil {
			return fmt.Errorf("processing snapshot table scan: %w", err)
		}
		tracker.done(numRowsProcessed)
		return nil
	}
}

// scanTableRangeTask returns a task that pages through one (minExclusive, maxInclusive]
// key range of table AS OF the snapshot SCN, so every chunk reads the same point in time.
func (s *Snapshot) scanTableRangeTask(table UserTable, pkCols []string, minExclusive, maxInclusive map[string]any, maxBatchSize int, tracker *tableCompletionTracker) func(context.Context) error {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.db.withConn(ctx, func(db *sql.Conn) (err error) {
			scan := keysetScan{minExclusive: minExclusive, maxInclusive: maxInclusive, scn: s.scn}
			numRowsProcessed, err = s.pageKeyset(ctx, db, table, pkCols, scan, maxBatchSize)
			return err
		})
		if err != nil {
			return err
		}
		l.Debugf("Finished scanning range chunk (%+v %+v], %d rows processed", minExclusive, maxInclusive, numRowsProcessed)
		tracker.done(numRowsProcessed)
		return nil
	}
}

// keysetScan is the part of a keyset scan that differs between tasks.
type keysetScan struct {
	minExclusive map[string]any // nil starts at the first key
	maxInclusive map[string]any // nil reads to the last key
	customQuery  string         // empty reads the base table
	scn          SCN            // InvalidSCN reads without AS OF SCN
}

// pageKeyset publishes every row of scan in pages of up to maxBatchSize rows ordered by pks,
// returning the number of rows published.
func (s *Snapshot) pageKeyset(ctx context.Context, db DBQuerier, table UserTable, pks []string, scan keysetScan, maxBatchSize int) (numRowsProcessed int, err error) {
	lastSeen := make(map[string]any, len(pks))
	for _, pk := range pks {
		lastSeen[pk] = nil
	}

	after := scan.minExclusive
	for {
		batchCount, err := s.processBatch(ctx, db, table, pks, scan, after, lastSeen, maxBatchSize)
		if err != nil {
			return numRowsProcessed, fmt.Errorf("processing snapshot batch: %w", err)
		}
		numRowsProcessed += batchCount
		if batchCount < maxBatchSize {
			return numRowsProcessed, nil
		}
		after = lastSeen
	}
}

// processBatch publishes one page of scan's rows after the keys in after, and stores the last
// row's keys in lastSeen for the next page.
func (s *Snapshot) processBatch(ctx context.Context, db DBQuerier, table UserTable, pks []string, scan keysetScan, after, lastSeen map[string]any, maxBatchSize int) (batchCount int, err error) {
	batchRows, err := querySnapshotTable(ctx, db, table, pks, scan, after, maxBatchSize)
	if err != nil {
		return 0, fmt.Errorf("execute snapshot table query: %w", err)
	}
	defer func() {
		if closeErr := batchRows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing snapshot rows: %w", closeErr)
		}
	}()

	types, err := batchRows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("fetch column types: %w", err)
	}

	values, mappers := prepSnapshotScannerAndMappers(types)

	columns, err := batchRows.Columns()
	if err != nil {
		return 0, fmt.Errorf("fetch columns: %w", err)
	}

	colMeta := buildColumnMeta(types)

	for batchRows.Next() {
		batchCount++

		if err := batchRows.Scan(values...); err != nil {
			return 0, err
		}

		if err := s.publishRow(ctx, table, columns, types, values, mappers, colMeta, lastSeen); err != nil {
			return 0, err
		}
	}

	if err = batchRows.Err(); err != nil {
		return 0, fmt.Errorf("iterating snapshot table row: %w", err)
	}
	s.snapshotRowsTotalMetric.Incr(int64(batchCount), table.FullName())
	return batchCount, nil
}

func (s *Snapshot) publishRow(ctx context.Context, table UserTable, columns []string, types []*sql.ColumnType, values []any, mappers []func(any) (any, error), colMeta []ColumnMeta, lastSeenPksValues map[string]any) error {
	row := map[string]any{}
	for idx, value := range values {
		v, err := mappers[idx](value)
		if err != nil {
			return err
		}
		if !s.lobEnabled && IsLOBTypeName(types[idx].DatabaseTypeName()) {
			v = nil
		}
		row[columns[idx]] = v
		if _, ok := lastSeenPksValues[columns[idx]]; ok {
			lastSeenPksValues[columns[idx]] = value
		}
	}

	m := MessageEvent{
		Table:      table.Name,
		Schema:     table.Schema,
		Data:       row,
		Operation:  MessageOperationRead,
		ColumnMeta: colMeta,
	}
	if s.scn != InvalidSCN {
		m.SCN = s.scn
	}
	if !s.commitTimestamp.IsZero() {
		m.CommitTimestamp = s.commitTimestamp
	}

	if err := s.publisher.Publish(ctx, &m); err != nil {
		return fmt.Errorf("handling snapshot table row: %w", err)
	}
	return nil
}

// snapshotTableFullScan publishes every row of table from one unordered scan, which Oracle
// reads with fast sequential multiblock I/O.
func (s *Snapshot) snapshotTableFullScan(ctx context.Context, tx *sql.Tx, table UserTable, maxBatchSize int) (numRowsProcessed int, err error) {
	tableName := table.FullName()
	q := fmt.Sprintf(`SELECT * FROM "%s"."%s"`, table.Schema, table.Name)
	rows, err := tx.QueryContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("execute snapshot table scan: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing snapshot rows: %w", closeErr)
		}
	}()

	types, err := rows.ColumnTypes()
	if err != nil {
		return 0, fmt.Errorf("fetch column types: %w", err)
	}

	values, mappers := prepSnapshotScannerAndMappers(types)

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("fetch columns: %w", err)
	}

	colMeta := buildColumnMeta(types)

	var sinceCancelCheck int
	for rows.Next() {
		if err = rows.Scan(values...); err != nil {
			return numRowsProcessed, err
		}

		if err = s.publishRow(ctx, table, columns, types, values, mappers, colMeta, nil); err != nil {
			return numRowsProcessed, err
		}

		numRowsProcessed++
		s.snapshotRowsTotalMetric.Incr(1, tableName)

		sinceCancelCheck++
		if sinceCancelCheck >= maxBatchSize {
			sinceCancelCheck = 0
			if err = ctx.Err(); err != nil {
				return numRowsProcessed, err
			}
		}
	}

	if err = rows.Err(); err != nil {
		return numRowsProcessed, fmt.Errorf("iterating snapshot table row: %w", err)
	}

	return numRowsProcessed, nil
}

// getTablePrimaryKeys returns table's primary key columns in order, and whether the key is
// ENABLED and VALIDATED, the only state that guarantees unique, non-null keys.
func getTablePrimaryKeys(ctx context.Context, db DBQuerier, table UserTable) (pks []string, enforced bool, err error) {
	// ALL_CONSTRAINTS/ALL_CONS_COLUMNS work in any container context after ALTER SESSION SET CONTAINER.
	pkSQL := `
	SELECT acc.column_name, ac.status, ac.validated
	FROM all_constraints ac
	JOIN all_cons_columns acc
		ON ac.constraint_name = acc.constraint_name
		AND ac.owner = acc.owner
	WHERE ac.constraint_type = 'P'
		AND UPPER(ac.table_name) = UPPER(:1)
		AND UPPER(ac.owner) = UPPER(:2)
	ORDER BY acc.position`
	pkArgs := []any{table.Name, table.Schema}

	rows, err := db.QueryContext(ctx, pkSQL, pkArgs...)
	if err != nil {
		return nil, false, fmt.Errorf("get primary key: %w", err)
	}
	defer rows.Close()

	// A table has at most one primary key constraint, so status and validated are the
	// same on every row.
	var status, validated string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk, &status, &validated); err != nil {
			return nil, false, err
		}
		pks = append(pks, pk)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("discovering primary keys for table '%s': %w", table.FullName(), err)
	}
	if len(pks) == 0 {
		return nil, false, fmt.Errorf("%w for table '%s'", errNoPrimaryKey, table.FullName())
	}

	return pks, status == "ENABLED" && validated == "VALIDATED", nil
}

// getTableRowCount returns table's optimizer statistics row count, or 0 if unknown.
func getTableRowCount(ctx context.Context, db DBQuerier, table UserTable) (int64, error) {
	var numRows sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`,
		table.Schema, table.Name,
	).Scan(&numRows); err != nil {
		return 0, fmt.Errorf("getting statistics for table '%s': %w", table.FullName(), err)
	}
	return max(numRows.Int64, 0), nil
}

// maxChunkRanges returns the most chunks numRows rows can be split into while each holds at least minRowsPerChunk.
func maxChunkRanges(numRows int64) int {
	return int(min(numRows/minRowsPerChunk, math.MaxInt32))
}

// countRowsUpTo returns table's row count as of scn, reading at most limit rows.
func countRowsUpTo(ctx context.Context, db DBQuerier, table UserTable, scn SCN, limit int) (int64, error) {
	q := fmt.Sprintf(`SELECT COUNT(*) FROM (SELECT 1 FROM "%s"."%s" AS OF SCN %d WHERE ROWNUM <= %d)`, table.Schema, table.Name, uint64(scn), limit)
	n, err := withFlashbackRetry(ctx, func() (n int64, err error) {
		err = db.QueryRowContext(ctx, q).Scan(&n)
		return n, err
	})
	if err != nil {
		return 0, fmt.Errorf("counting rows of table '%s': %w", table.FullName(), withFlashbackPrivilegeHint(err, table))
	}
	return n, nil
}

// samplePercent returns the SAMPLE percentage expected to draw numSamples of total rows,
// capped just below 100, which Oracle rejects (ORA-30562).
func samplePercent(numSamples int, total int64) float64 {
	const maxSamplePercent = 99.9999
	return min(maxSamplePercent, max(0.0001, 100.0*float64(numSamples)/float64(max(total, 1))))
}

// sampleTableKeyspace returns up to maxKeys sorted primary key samples as of scn, thinned
// to every k-th key so they spread evenly across the keyspace.
func sampleTableKeyspace(ctx context.Context, db DBQuerier, table UserTable, pkCols []string, scn SCN, sampleClause string, maxKeys int) ([]map[string]any, error) {
	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = `"` + col + `"`
	}
	cols := strings.Join(quoted, ", ")
	q := `SELECT %[1]s FROM (
		SELECT %[1]s, ROW_NUMBER() OVER (ORDER BY %[1]s) "RPCN__RN", COUNT(*) OVER () "RPCN__CNT"
		FROM "%[2]s"."%[3]s" %[4]s AS OF SCN %[5]d
	) WHERE MOD("RPCN__RN", CEIL("RPCN__CNT" / %[6]d)) = 0 ORDER BY %[1]s`
	return queryKeyspace(ctx, db, table, fmt.Sprintf(q, cols, table.Schema, table.Name, sampleClause, uint64(scn), max(maxKeys, 1)))
}

// queryKeyspace runs a flashback query for table's key values, returning each row as a chunk bound.
func queryKeyspace(ctx context.Context, db DBQuerier, table UserTable, query string) ([]map[string]any, error) {
	rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
		return db.QueryContext(ctx, query)
	})
	if err != nil {
		return nil, fmt.Errorf("querying keyspace of table '%s': %w", table.FullName(), withFlashbackPrivilegeHint(err, table))
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("fetching column types of keyspace of table '%s': %w", table.FullName(), err)
	}
	values, mappers := prepSnapshotScannerAndMappers(types)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("fetching columns of keyspace of table '%s': %w", table.FullName(), err)
	}

	var keys []map[string]any
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("scanning key of table '%s': %w", table.FullName(), err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			v, err := mappers[i](values[i])
			if err != nil {
				return nil, fmt.Errorf("decoding key column %s of table '%s': %w", col, table.FullName(), err)
			}
			row[col] = v
		}
		keys = append(keys, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating keys of table '%s': %w", table.FullName(), err)
	}
	return keys, nil
}

// buildLexicographicBoundExpr expands a composite key comparison such as (a, b) > (x, y)
// into (a > x) OR (a = x AND b > y), since Oracle has no tuple comparison.
func buildLexicographicBoundExpr(pk []string, boundVals map[string]any, op string, paramIdx *int) (expr string, args []any) {
	strictOp := op
	if op == "<=" {
		strictOp = "<"
	}

	var sb strings.Builder
	for i := range pk {
		if i > 0 {
			sb.WriteString(" OR ")
		}
		sb.WriteString("(")
		for j := range i {
			if j > 0 {
				sb.WriteString(" AND ")
			}
			*paramIdx++
			fmt.Fprintf(&sb, `"%s" = :%d`, pk[j], *paramIdx)
			args = append(args, boundVals[pk[j]])
		}
		if i > 0 {
			sb.WriteString(" AND ")
		}
		cmp := strictOp
		if i == len(pk)-1 {
			cmp = op
		}
		*paramIdx++
		fmt.Fprintf(&sb, `"%s" %s :%d`, pk[i], cmp, *paramIdx)
		args = append(args, boundVals[pk[i]])
		sb.WriteString(")")
	}
	return sb.String(), args
}

// querySnapshotTable fetches the next keyset page of scan after the keys in after, reading the
// base table AS OF scan.scn when it's valid and there is no custom query.
func querySnapshotTable(ctx context.Context, db DBQuerier, table UserTable, pk []string, scan keysetScan, after map[string]any, limit int) (*sql.Rows, error) {
	baseTableRef := fmt.Sprintf(`"%s"."%s"`, table.Schema, table.Name)
	flashback := scan.customQuery == "" && scan.scn != InvalidSCN
	if flashback {
		baseTableRef = fmt.Sprintf("%s AS OF SCN %d", baseTableRef, scan.scn)
	}

	q, args := buildKeysetPageQuery(baseTableRef, scan.customQuery, pk, after, scan.maxInclusive, limit)
	rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
		return db.QueryContext(ctx, q, args...)
	})
	if err == nil {
		return rows, nil
	}
	var oraErr *goora.OracleError
	if scan.customQuery != "" && errors.As(err, &oraErr) && oraErr.ErrCode == errCodeInvalidIdentifier {
		return nil, fmt.Errorf("%w\n\nThis usually means the snapshot filter for table '%s' doesn't project all of its primary key columns (%s). "+
			"Cursor-based pagination filters and sorts on the full primary key against the filter's own result set, "+
			"so every primary key column must be included in the filter's SELECT list even if it otherwise selects only a subset of columns",
			err, table.FullName(), strings.Join(pk, ", "))
	}
	if flashback {
		err = withFlashbackPrivilegeHint(err, table)
	}
	return nil, err
}

// buildKeysetPageQuery returns the query and bind args for one keyset page of baseTableRef, or
// of customQuery when set, after lastSeen and up to maxInclusive, where a nil bound is unbounded.
func buildKeysetPageQuery(baseTableRef, customQuery string, pk []string, lastSeen, maxInclusive map[string]any, limit int) (string, []any) {
	var (
		args     []any
		paramIdx int
		clauses  []string
	)
	if lastSeen != nil {
		expr, exprArgs := buildLexicographicBoundExpr(pk, lastSeen, ">", &paramIdx)
		clauses = append(clauses, "("+expr+")")
		args = append(args, exprArgs...)
	}
	if maxInclusive != nil {
		expr, exprArgs := buildLexicographicBoundExpr(pk, maxInclusive, "<=", &paramIdx)
		clauses = append(clauses, "("+expr+")")
		args = append(args, exprArgs...)
	}

	var parts []string
	switch {
	case customQuery == "":
		parts = append(parts, "SELECT * FROM "+baseTableRef)
	case len(clauses) == 0:
		// No bounds, so use the custom query as is rather than wrapping it in a subquery.
		parts = append(parts, customQuery)
	default:
		// Wrap the custom query so the added WHERE doesn't conflict with one it already has.
		parts = append(parts, fmt.Sprintf("SELECT * FROM (%s) t", customQuery))
	}
	if len(clauses) > 0 {
		parts = append(parts, "WHERE "+strings.Join(clauses, " AND "))
	}
	// Oracle uses FETCH FIRST instead of TOP, and it comes at the end.
	parts = append(parts, buildOrderByClause(pk), fmt.Sprintf("FETCH FIRST %d ROWS ONLY", limit))
	return strings.Join(parts, " "), args
}

func prepSnapshotScannerAndMappers(cols []*sql.ColumnType) (values []any, mappers []func(any) (any, error)) {
	for _, col := range cols {
		precision, scale, ok := col.DecimalSize()
		val, mapper := SnapshotScanDest(col.DatabaseTypeName(), col.Name(), precision, scale, ok)
		values = append(values, val)
		mappers = append(mappers, mapper)
	}
	return
}

// SnapshotScanDest returns the scan destination and value mapper for a column of the given
// go-ora type name, and is exported so tests can check it against the schema mapping.
func SnapshotScanDest(dbTypeName, colName string, precision, scale int64, hasDecimalSize bool) (val any, mapper func(any) (any, error)) {
	stringMapping := func(mapper func(s string) (any, error)) func(any) (any, error) {
		return func(v any) (any, error) {
			s, ok := v.(*sql.NullString)
			if !ok {
				return nil, fmt.Errorf("expected %T got %T", "", v)
			}
			if !s.Valid {
				return nil, nil
			}
			return mapper(s.String)
		}
	}

	// Oracle database type names
	switch dbTypeName {
	case "RAW", "LONG RAW", "BLOB", "VarRaw", "LongRaw", "LongVarRaw", "OCIBlobLocator":
		return new(sql.Null[[]byte]), snapshotValueMapper[[]byte]
	case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE",
		"TimeStampTZ", "TimeStampDTY", "TimeStampTZ_DTY", "TimeStampLTZ_DTY", "TimeStampeLTZ", "TIMESTAMPTZ":
		return new(sql.NullTime), func(v any) (any, error) {
			s, ok := v.(*sql.NullTime)
			if !ok {
				return nil, fmt.Errorf("expected %T got %T", time.Time{}, v)
			}
			if !s.Valid {
				return nil, nil
			}
			return s.Time, nil
		}
	case "NUMBER", "INTEGER", "INT", "SMALLINT", "FLOAT":
		// Classify the column with the same NumberToCommon the streaming
		// schema cache uses, so snapshot and streaming agree on whether a
		// NUMBER is an Int64, a bounded Decimal, or a BigDecimal — and so
		// the emitted value type matches the schema in both modes.
		common := NumberToCommon(colName, precision, scale, hasDecimalSize)
		if common.Type == schema.Int64 {
			// Scan integer-width columns natively; go-ora handles the
			// NUMBER → int64 conversion robustly without a string round-trip.
			return new(sql.Null[int64]), snapshotValueMapper[int64]
		}
		// Decimal / BigDecimal: scan as text and canonicalise to a
		// string via the shared coercion (never a bare number), so
		// downstream Avro string-field encoding accepts the value.
		return new(sql.NullString), stringMapping(func(text string) (any, error) {
			out, err := sqlutil.CoerceToCommon(common, text)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", colName, err)
			}
			return out, nil
		})
	case "BINARY_FLOAT", "IBFloat", "BFloat", "BINARY_DOUBLE", "IBDouble", "BDouble":
		return new(sql.Null[float64]), snapshotValueMapper[float64]
	case "CLOB", "NCLOB", "LONG", "LongVarChar", "OCIClobLocator":
		// Character large objects - handle as string
		return new(sql.NullString), stringMapping(func(s string) (any, error) {
			return s, nil
		})
	case "JSON", "TNSType(119)":
		// Oracle 21c+ native JSON type. go-ora v2.9.0's TNSType stringer
		// has no entry for it (119), so DatabaseTypeName() renders the
		// raw "TNSType(119)" form.
		return new(sql.NullString), stringMapping(func(s string) (v any, err error) {
			err = json.Unmarshal([]byte(s), &v)
			return
		})
	default:
		// Default to string for VARCHAR2, CHAR, NVARCHAR2, NCHAR, etc.
		return new(sql.Null[string]), snapshotValueMapper[string]
	}
}

func buildOrderByClause(pk []string) string {
	quoted := make([]string, len(pk))
	for i, col := range pk {
		quoted[i] = `"` + col + `"`
	}
	return "ORDER BY " + strings.Join(quoted, ", ")
}

// buildColumnMeta returns the column type metadata that MessageEvent carries to the schema cache.
func buildColumnMeta(types []*sql.ColumnType) []ColumnMeta {
	meta := make([]ColumnMeta, len(types))
	for i, ct := range types {
		meta[i] = ColumnMeta{
			Name:     ct.Name(),
			TypeName: ct.DatabaseTypeName(),
		}
		if precision, scale, ok := ct.DecimalSize(); ok {
			meta[i].Precision = precision
			meta[i].Scale = scale
			meta[i].HasDecimalSize = true
		}
	}
	return meta
}

// IsLOBTypeName reports whether a go-ora type name is a large object, whose value is nulled
// in snapshot rows when lob_enabled is false.
func IsLOBTypeName(dbType string) bool {
	switch dbType {
	case "CLOB", "NCLOB", "BLOB", "LONG", "LONG RAW",
		"LongVarChar", "LongRaw", "LongVarRaw", // go-ora driver-level names for CLOB/NCLOB/LONG and BLOB/LONG RAW (inline LOB mode)
		"OCIClobLocator", "OCIBlobLocator": // go-ora driver-level LOB locator names (non-inline mode)
		return true
	}
	return false
}

func snapshotValueMapper[T any](v any) (any, error) {
	s, ok := v.(*sql.Null[T])
	if !ok {
		var e T
		return nil, fmt.Errorf("expected %T got %T", e, v)
	}
	if !s.Valid {
		return nil, nil
	}
	return s.V, nil
}

// isMissingFlashbackPrivilege reports whether err from an AS OF SCN read means the user lacks
// FLASHBACK on the table, returning the Oracle error.
func isMissingFlashbackPrivilege(err error) (*goora.OracleError, bool) {
	var oraErr *goora.OracleError
	if !errors.As(err, &oraErr) {
		return nil, false
	}
	return oraErr, oraErr.ErrCode == errCodeInsufficientPrivileges || oraErr.ErrCode == errCodeMissingFlashbackPrivilege
}

// withFlashbackPrivilegeHint adds the missing FLASHBACK grant to an ORA-01031 or ORA-41900 from an AS OF SCN read.
func withFlashbackPrivilegeHint(err error, table UserTable) error {
	if _, ok := isMissingFlashbackPrivilege(err); !ok {
		return err
	}
	return fmt.Errorf("%w\n\nParallel snapshot chunks read table '%s' with flashback queries (AS OF SCN), which require the FLASHBACK privilege in addition to SELECT: "+
		"GRANT FLASHBACK ON %s TO <user>, or GRANT FLASHBACK ANY TABLE TO <user>",
		err, table.FullName(), table.FullName())
}

// withFlashbackRetry retries fn on the transient ORA-01466 that AS OF SCN reads can hit just after Prepare.
func withFlashbackRetry[T any](ctx context.Context, fn func() (T, error)) (result T, err error) {
	// The race window closes within a few hundred milliseconds, so this isn't configurable.
	const (
		flashbackRetryAttempts = 5
		flashbackRetryDelay    = 100 * time.Millisecond
	)
	for range flashbackRetryAttempts {
		result, err = fn()
		if err == nil {
			return result, nil
		}
		var oraErr *goora.OracleError
		if !errors.As(err, &oraErr) || oraErr.ErrCode != errCodeFlashbackTableChanged {
			return result, err
		}
		select {
		case <-time.After(flashbackRetryDelay):
		case <-ctx.Done():
			return result, ctx.Err()
		}
	}
	return result, err
}
