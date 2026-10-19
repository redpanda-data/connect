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
	"strings"
	"sync/atomic"
	"time"

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
	// FLASHBACK grant; see withFlashbackPrivilegeHint.
	errCodeInsufficientPrivileges = 1031
)

// withFlashbackPrivilegeHint annotates an ORA-01031 raised by an AS OF SCN read with the
// grant needed to fix it, since the error alone doesn't say which privilege is missing and
// a user with SELECT on the table would reasonably expect the read to succeed.
func withFlashbackPrivilegeHint(err error, table UserTable) error {
	var oraErr *goora.OracleError
	if !errors.As(err, &oraErr) || oraErr.ErrCode != errCodeInsufficientPrivileges {
		return err
	}
	return fmt.Errorf("%w\n\nParallel snapshot chunks read table '%s' with flashback queries (AS OF SCN), which require the FLASHBACK privilege in addition to SELECT: "+
		"GRANT FLASHBACK ON %s TO <user>, or GRANT FLASHBACK ANY TABLE TO <user>. Alternatively, set max_parallel_snapshot_tables to 1 to snapshot without flashback queries",
		err, table.FullName(), table.FullName())
}

// withFlashbackRetry retries fn on ORA-01466, which AS OF SCN reads can hit transiently
// just after Prepare captures the SCN. Other errors are returned immediately.
func withFlashbackRetry[T any](ctx context.Context, fn func() (T, error)) (result T, err error) {
	// flashbackRetryAttempts and flashbackRetryDelay bound the retry in withFlashbackRetry.
	// These aren't exposed as config because they guard a narrow, internal-only race window
	// (freshly-committed DML settling before it's safely flashback-readable) rather than
	// anything a user would tune - by design the retry window closes within a few hundred
	// milliseconds of the snapshot's SCN being captured.
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

// errNoPrimaryKey indicates a table has no discoverable primary key. Snapshot planning
// treats this as expected (not fatal) for unfiltered tables, falling back to a full
// unordered scan - see planTableTasks.
var errNoPrimaryKey = errors.New("no primary key found")

// Snapshot is responsible for creating snapshots of existing tables based on the Tables
// configuration value.
type Snapshot struct {
	dbPool                  *sql.DB
	tables                  []UserTable
	filters                 map[string]string
	publisher               ChangePublisher
	log                     *service.Logger
	snapshotStatusMetric    *service.MetricGauge
	snapshotRowsTotalMetric *service.MetricCounter
	lobEnabled              bool
	pdbName                 string
	scn                     SCN       // published as part of snapshot read metadata
	commitTimestamp         time.Time // Oracle server time at snapshot prepare
}

// NewSnapshot creates a new instance of Snapshot capable of snapshotting provided tables.
// It does this by creating a transaction with snapshot level isolation before paging
// through rows, sending them to be batched.
func NewSnapshot(ctx context.Context,
	connectionString string,
	tables []UserTable,
	filters map[string]string,
	publisher ChangePublisher,
	lobEnabled bool,
	pdbName string,
	logger *service.Logger,
	metrics *service.Metrics,
) (*Snapshot, error) {
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		return nil, fmt.Errorf("connecting to oracle database for snapshotting: %w", err)
	}

	if err := ApplyNLSSettings(ctx, db); err != nil {
		db.Close()
		return nil, fmt.Errorf("configuring nls for snapshot session: %w", err)
	}

	s := &Snapshot{
		dbPool:                  db,
		tables:                  tables,
		filters:                 filters,
		publisher:               publisher,
		lobEnabled:              lobEnabled,
		pdbName:                 pdbName,
		snapshotStatusMetric:    metrics.NewGauge("oracledb_cdc_snapshot_status", "table"),
		snapshotRowsTotalMetric: metrics.NewCounter("oracledb_cdc_snapshot_rows_total", "table"),
		log:                     logger,
		scn:                     InvalidSCN,
	}
	return s, nil
}

// Prepare prepares the snapshot by starting a transaction with appropriate isolation level.
// Returns the current SCN for the snapshot.
func (s *Snapshot) Prepare(ctx context.Context) (SCN, error) {
	if len(s.tables) == 0 {
		return InvalidSCN, errors.New("no tables provided")
	}

	var currentSCN SCN
	if err := s.dbPool.QueryRowContext(ctx, `SELECT CURRENT_SCN, SYSTIMESTAMP FROM V$DATABASE`).Scan(&currentSCN, &s.commitTimestamp); err != nil {
		return InvalidSCN, fmt.Errorf("getting current SCN for snapshot: %w", err)
	}

	// capture to include on snapshot metadata
	s.scn = currentSCN

	s.log.Infof("Captured SCN before snapshot at SCN: %s", s.scn)
	return s.scn, nil
}

// Read launches N go routines (based on maxWorkers) and starts the process of
// iterating through each table, reading rows based on maxBatchSize, sending the row as a
// replication.MessageEvent to the configured publisher. Large, unfiltered, PK'd tables are
// split into independent range chunks (see planTableTasks) so a single big table can't
// starve every other table of workers; every table's task(s) are flattened into one shared
// pool bounded by maxWorkers, mirroring pglogicalstream's processSnapshot.
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

	var tasks []func(context.Context) error
	for _, table := range s.tables {
		tableTasks, err := s.planTableTasks(ctx, table, maxWorkers, maxBatchSize)
		if err != nil {
			return fmt.Errorf("planning snapshot for table '%s': %w", table.FullName(), err)
		}
		tasks = append(tasks, tableTasks...)
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(maxWorkers)
	for _, task := range tasks {
		wg.Go(func() error { return task(ctx) })
	}

	if err := wg.Wait(); err != nil {
		return fmt.Errorf("processing snapshots: %w", err)
	}

	return nil
}

// tableCompletionTracker counts down the outstanding tasks for a single table so the
// "done" gauge and rows-processed log line fire exactly once, right after that table's own
// last chunk finishes - even though every table's tasks now share one flattened, global
// worker pool rather than each table getting its own goroutine.
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

// done records a finished task's row count, marking the table as fully snapshotted once
// every one of its tasks has reported in.
func (t *tableCompletionTracker) done(rowsProcessed int) {
	t.rows.Add(int64(rowsProcessed))
	if t.remaining.Add(-1) == 0 {
		t.snapshot.snapshotStatusMetric.Set(1, t.table.FullName())
		t.snapshot.log.With("src_table", t.table.FullName()).Infof("Table snapshot completed, %d rows processed", t.rows.Load())
	}
}

// planTableTasks decides how a single table will be scanned during the snapshot and
// returns the independent task(s) that accomplish it:
//   - a custom filter configured -> a single sequential PK-keyset pagination task against
//     the filter's own result set, unchanged from before (SAMPLE can't target a subquery,
//     so chunking is out of scope for these).
//   - no filter, and either a single worker, no discoverable primary key, a primary key
//     that isn't enabled and validated, no table statistics, or a keyspace too small to
//     split -> a single full unordered-scan task.
//     A single sequential cursor is cheaper than ordered keyset pages when there's nothing
//     to parallelise, and it preserves the ability to snapshot PK-less tables.
//   - no filter, has a primary key -> the keyspace is sampled and split into up to
//     maxWorkers range-bounded chunk tasks, each independently consistent via AS OF SCN.
func (s *Snapshot) planTableTasks(ctx context.Context, table UserTable, maxWorkers, maxBatchSize int) ([]func(context.Context) error, error) {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	customQuery, hasFilter := s.filters[tableName]

	if hasFilter {
		l.Infof("Launching snapshot of table '%s' with snapshot filter", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableWithFilterTask(table, maxBatchSize, customQuery, tracker)}, nil
	}

	fullScan := func() []func(context.Context) error {
		l.Infof("Launching snapshot of table '%s'", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableFullScanTask(table, maxBatchSize, tracker)}
	}

	// With a single worker there's no parallelism to gain from chunking, so skip the PK
	// lookup and keyspace sampling entirely.
	if maxWorkers <= 1 {
		return fullScan(), nil
	}

	var (
		pks      []string
		enforced bool
	)
	if err := s.withSnapshotConn(ctx, func(db *sql.Conn) (err error) {
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

	ranges, err := s.planChunkRanges(ctx, table, pks, maxWorkers)
	if err != nil {
		return nil, fmt.Errorf("planning chunk ranges: %w", err)
	}
	if len(ranges) <= 1 {
		return fullScan(), nil
	}

	l.Infof("Launching snapshot of table '%s' split into %d parallel chunks", tableName, len(ranges))

	tracker := newTableCompletionTracker(s, table, len(ranges))
	tasks := make([]func(context.Context) error, 0, len(ranges))
	for _, r := range ranges {
		tasks = append(tasks, s.scanTableRangeTask(table, pks, r.min, r.max, maxBatchSize, tracker))
	}
	return tasks, nil
}

// chunkRange is a half-open (minExclusive, maxInclusive] primary-key bound used to split a
// single table's snapshot scan into independently-runnable ranges. A nil bound means
// unbounded in that direction; the first range's min and the last range's max are always
// nil.
type chunkRange struct {
	min map[string]any
	max map[string]any
}

// planChunkRanges samples table's primary key keyspace and splits it into up to maxWorkers
// roughly even ranges. Tables that are too small or lack optimizer statistics (needed to
// size the sample) get a single unbounded range.
//
// SAMPLE BLOCK is tried first as it only reads the sampled blocks, but it samples whole
// multiblock reads, so on smaller tables it often returns no rows. If it returns too few
// keys, row SAMPLE is used instead, which reads the whole primary key index but is cheap
// on a table small enough for block sampling to fail.
func (s *Snapshot) planChunkRanges(ctx context.Context, table UserTable, pkCols []string, maxWorkers int) ([]chunkRange, error) {
	const (
		// overSampleFactor is the number of primary key samples drawn per worker when planning
		// chunks: enough to split the keyspace evenly while keeping the sample query cheap.
		overSampleFactor = 32
	)
	numSamples := min(maxWorkers, 256) * overSampleFactor

	var samples []map[string]any
	if err := s.withSnapshotConn(ctx, func(db *sql.Conn) error {
		stats, err := getTableStats(ctx, db, table)
		if err != nil {
			return err
		}
		if !stats.valid() {
			s.log.With("src_table", table.FullName()).Infof("Table '%s' has no optimizer statistics, so it will be snapshotted without parallel chunking; gather statistics with DBMS_STATS.GATHER_TABLE_STATS to enable it", table.FullName())
			return nil
		}
		blockClause := fmt.Sprintf("SAMPLE BLOCK (%.4f)", samplePercent(numSamples, stats.blocks))
		if samples, err = sampleTableKeyspace(ctx, db, table, pkCols, s.scn, blockClause); err != nil || len(samples) > maxWorkers {
			return err
		}
		rowClause := fmt.Sprintf("SAMPLE (%.4f)", samplePercent(numSamples, stats.numRows))
		samples, err = sampleTableKeyspace(ctx, db, table, pkCols, s.scn, rowClause)
		return err
	}); err != nil {
		return nil, fmt.Errorf("sampling keyspace for table '%s': %w", table.FullName(), err)
	}

	// Use max(1, ...) to avoid chunkSize=0 when samples < maxWorkers (e.g. small tables
	// that fit on a single block produce only a handful of samples), which would otherwise
	// cause an infinite loop below.
	chunkSize := max(1, len(samples)/maxWorkers)
	var (
		prev   map[string]any
		ranges []chunkRange
	)
	for i := chunkSize; i < len(samples); i += chunkSize {
		bound := samples[i]
		ranges = append(ranges, chunkRange{min: prev, max: bound})
		prev = bound
	}
	ranges = append(ranges, chunkRange{min: prev, max: nil})
	return ranges, nil
}

// withSnapshotConn runs fn against a dedicated *sql.Conn suitable for snapshot queries,
// switched into the configured PDB (and back to CDB$ROOT afterward) in CDB mode. NLS and
// TIME_ZONE settings are applied to every connection handed out here, in both modes: an
// ALTER SESSION issued against the *sql.DB pool only reaches whichever pooled connection
// happened to run it, and keyset pagination over DATE/TIMESTAMP or fractional NUMBER
// primary keys depends on those settings to compare bound values correctly - running a
// page on a connection without them can silently skip or re-read rows.
func (s *Snapshot) withSnapshotConn(ctx context.Context, fn func(conn *sql.Conn) error) (err error) {
	var conn *sql.Conn
	if conn, err = s.dbPool.Conn(ctx); err != nil {
		return fmt.Errorf("acquiring snapshot connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			// snapshot has completed at this point so logging the error is sufficient.
			s.log.Errorf("Closing snapshot connection: %v", closeErr)
		}
	}()

	if s.pdbName != "" {
		if _, err = conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+s.pdbName); err != nil {
			return fmt.Errorf("switching session to PDB '%s' for snapshot: %w", s.pdbName, err)
		}
		defer func() {
			if _, cerr := conn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); cerr != nil {
				// logging the error is sufficient here, connection will be closed in defer call above.
				s.log.Errorf("Switching session back to root container: %v", cerr)
			}
		}()
	}

	if err = ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("configuring nls for snapshot connection: %w", err)
	}

	return fn(conn)
}

// withReadOnlyTxn runs fn against a *sql.Tx pinned to Oracle's automatic serializable
// isolation for READ ONLY transactions, opened on a connection from withSnapshotConn. Used
// only by the snapshot paths that still need transaction-scoped consistency -
// custom-filtered tables and unchunked full-table scans - where AS OF SCN chunking (see
// scanTableRangeTask) isn't used.
func (s *Snapshot) withReadOnlyTxn(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return s.withSnapshotConn(ctx, func(conn *sql.Conn) (err error) {
		// Use context.Background() to prevent database/sql from spawning an
		// awaitDone goroutine that races with our explicit Rollback below.
		// The go-ora v2 driver has an unsynchronized field in Session that
		// causes a data race between BreakConnection (from awaitDone) and
		// IsBreak (from our Rollback). Transaction lifetime is managed
		// manually via the defer and explicit Rollback at the end.
		tx, err := conn.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("beginning snapshot transaction: %w", err)
		}

		// In Oracle, READ ONLY transactions automatically provide serializable isolation
		if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("setting transaction read-only: %w", err)
		}
		defer func() {
			if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
					s.log.Errorf("Failed to rollback snapshot transaction: %v", rbErr)
				}
			}
		}()

		if err = fn(tx); err != nil {
			return err
		}

		if rbErr := tx.Rollback(); rbErr != nil {
			s.log.Errorf("Failed rollback snapshot transaction: %v", rbErr)
		}
		return nil
	})
}

// snapshotTableWithFilterTask returns a task that pages through a custom snapshot filter
// query using PK-keyset pagination against the filter's own result set. Custom filters may
// join or aggregate, so a physical-order scan can't safely assume every row maps to a
// stable ROWID/heap position, and SAMPLE can't target a subquery - so this path is never
// chunked.
func (s *Snapshot) snapshotTableWithFilterTask(table UserTable, maxBatchSize int, customQuery string, tracker *tableCompletionTracker) func(context.Context) error {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.withReadOnlyTxn(ctx, func(tx *sql.Tx) error {
			tablePks, _, err := getTablePrimaryKeys(ctx, tx, table)
			if err != nil {
				return err
			}
			l.Debugf("Found primary keys for table '%s': %v", table, tablePks)

			lastSeenPksValues := map[string]any{}
			for _, pk := range tablePks {
				lastSeenPksValues[pk] = nil
			}

			var pksForQuery map[string]any
			for {
				batchCount, err := s.processBatch(ctx, tx, table, tablePks, pksForQuery, nil, lastSeenPksValues, maxBatchSize, tableName, customQuery, InvalidSCN)
				if err != nil {
					return fmt.Errorf("processing snapshot batch: %w", err)
				}

				numRowsProcessed += batchCount
				pksForQuery = lastSeenPksValues
				if batchCount < maxBatchSize {
					break
				}
			}
			return nil
		})
		tracker.done(numRowsProcessed)
		return err
	}
}

// snapshotTableFullScanTask returns a task that runs snapshotTableFullScan for tables that
// aren't chunked: those with no discoverable primary key, a primary key that isn't
// enabled and validated, no optimizer statistics, a keyspace too small to split, or any
// table when only a single worker is configured.
func (s *Snapshot) snapshotTableFullScanTask(table UserTable, maxBatchSize int, tracker *tableCompletionTracker) func(context.Context) error {
	tableName := table.FullName()
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.withReadOnlyTxn(ctx, func(tx *sql.Tx) (err error) {
			numRowsProcessed, err = s.snapshotTableFullScan(ctx, tx, table, maxBatchSize, tableName)
			return err
		})
		tracker.done(numRowsProcessed)
		if err != nil {
			return fmt.Errorf("processing snapshot table scan: %w", err)
		}
		return nil
	}
}

// scanTableRangeTask returns a task that pages through a single (minExclusive,
// maxInclusive] primary-key range of table, analogous to pglogicalstream's
// scanTableRange. Every page is pinned to the SCN captured at Prepare via AS OF SCN, so
// independently-scheduled chunks - even across different tables - agree on a single
// consistent snapshot without needing a shared, long-lived transaction.
func (s *Snapshot) scanTableRangeTask(table UserTable, pkCols []string, minExclusive, maxInclusive map[string]any, maxBatchSize int, tracker *tableCompletionTracker) func(context.Context) error {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	return func(ctx context.Context) error {
		var numRowsProcessed int
		err := s.withSnapshotConn(ctx, func(db *sql.Conn) error {
			lastSeenPksValues := map[string]any{}
			for _, pk := range pkCols {
				lastSeenPksValues[pk] = nil
			}

			pksForQuery := minExclusive
			for {
				batchCount, err := s.processBatch(ctx, db, table, pkCols, pksForQuery, maxInclusive, lastSeenPksValues, maxBatchSize, tableName, "", s.scn)
				if err != nil {
					return fmt.Errorf("processing snapshot range batch: %w", err)
				}

				numRowsProcessed += batchCount
				pksForQuery = lastSeenPksValues
				if batchCount < maxBatchSize {
					break
				}
			}
			return nil
		})
		tracker.done(numRowsProcessed)
		if err != nil {
			return err
		}
		l.Debugf("Finished scanning range chunk (%+v %+v], %d rows processed", minExclusive, maxInclusive, numRowsProcessed)
		return nil
	}
}

// processBatch queries and processes a single page of rows from a snapshot table.
// pksForQuery is passed to querySnapshotTable as the exclusive lower bound (nil for the
// first page of an unbounded scan, or the range's minExclusive for the first page of a
// chunk). maxInclusivePkVal is the chunk's upper bound (nil for the custom-filter path and
// for unbounded/last-chunk scans). lastSeenPksValues is mutated in place with the PK values
// from the last row of the batch, so the caller can pass it as pksForQuery on the next
// iteration.
func (s *Snapshot) processBatch(ctx context.Context, db DBQuerier, table UserTable, tablePks []string, pksForQuery, maxInclusivePkVal map[string]any, lastSeenPksValues map[string]any, maxBatchSize int, tableName, customQuery string, scn SCN) (batchCount int, err error) {
	batchRows, err := querySnapshotTable(ctx, db, table, tablePks, pksForQuery, maxInclusivePkVal, maxBatchSize, customQuery, scn)
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

		if err := s.publishRow(ctx, table, columns, types, values, mappers, colMeta, lastSeenPksValues); err != nil {
			return 0, err
		}
	}

	if err = batchRows.Err(); err != nil {
		return 0, fmt.Errorf("iterating snapshot table row: %w", err)
	}
	s.snapshotRowsTotalMetric.Incr(int64(batchCount), tableName)
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

// snapshotTableFullScan performs a single, full unordered scan so Oracle's optimizer
// picks a full table scan (sequential multiblock reads) over random disk I/O when ordered.
func (s *Snapshot) snapshotTableFullScan(ctx context.Context, tx *sql.Tx, table UserTable, maxBatchSize int, tableName string) (numRowsProcessed int, err error) {
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

// getTablePrimaryKeys returns the columns of table's primary key in key order. enforced
// reports whether the constraint is ENABLED and VALIDATED, the only state in which Oracle
// guarantees every existing row has a unique, non-null key.
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

// tableStats is a table's optimizer-statistics size estimate, used to scale the sample
// percentages in planChunkRanges. A zero field means the statistic is unknown.
type tableStats struct {
	blocks  int64
	numRows int64
}

// valid reports whether both estimates are known. BLOCKS and NUM_ROWS are null if
// DBMS_STATS was never run against the table (and zero if it was empty when it last ran),
// in which case the table's real size is unknown.
func (t tableStats) valid() bool {
	return t.blocks > 0 && t.numRows > 0
}

// getTableStats reads table's block and row count estimates from ALL_TABLES rather than
// the more precise DBA_SEGMENTS, since the latter needs elevated catalog privileges a
// normal CDC user may not have.
func getTableStats(ctx context.Context, db DBQuerier, table UserTable) (tableStats, error) {
	var blocks, numRows sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT BLOCKS, NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`,
		table.Schema, table.Name,
	).Scan(&blocks, &numRows); err != nil {
		return tableStats{}, fmt.Errorf("getting statistics for table '%s': %w", table.FullName(), err)
	}
	return tableStats{blocks: max(blocks.Int64, 0), numRows: max(numRows.Int64, 0)}, nil
}

// samplePercent returns the SAMPLE percentage expected to draw numSamples of total units
// (blocks or rows). For small tables this deliberately pushes the percentage toward 100%,
// accepting a read of nearly everything rather than trying to be clever.
// Unlike Postgres's TABLESAMPLE SYSTEM (which accepts up to and including 100),
// Oracle's SAMPLE clause rejects a percentage of exactly 100 (ORA-30562: invalid SAMPLE percentage),
// so the upper bound is capped just short of it.
func samplePercent(numSamples int, total int64) float64 {
	const maxSamplePercent = 99.9999
	return min(maxSamplePercent, max(0.0001, 100.0*float64(numSamples)/float64(max(total, 1))))
}

// sampleTableKeyspace returns sorted primary-key samples as of scn, used by planChunkRanges
// as candidate chunk boundaries. SAMPLE must precede AS OF SCN (ORA-03049 otherwise) and
// can't target a view or subquery, so chunking only applies to unfiltered base tables.
//
// ROWNUM applies before ORDER BY, so maxKeyspaceSampleRows bounds the scan when stale
// statistics inflate the percentage. Too few samples just means a single range.
func sampleTableKeyspace(ctx context.Context, db DBQuerier, table UserTable, pkCols []string, scn SCN, sampleClause string) ([]map[string]any, error) {
	const (
		// maxKeyspaceSampleRows caps the keys sampleTableKeyspace holds in memory when stale
		// statistics inflate the sample percentage. Truncating only skews chunk sizes, never
		// coverage.
		maxKeyspaceSampleRows = 100_000
	)
	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = `"` + col + `"`
	}
	cols := strings.Join(quoted, ", ")
	q := `SELECT %s FROM "%s"."%s" %s AS OF SCN %d WHERE ROWNUM <= %d ORDER BY %s`
	query := fmt.Sprintf(q, cols, table.Schema, table.Name, sampleClause, uint64(scn), maxKeyspaceSampleRows, cols)

	rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
		return db.QueryContext(ctx, query)
	})
	if err != nil {
		return nil, fmt.Errorf("sampling keyspace for table '%s': %w", table.FullName(), withFlashbackPrivilegeHint(err, table))
	}
	defer rows.Close()

	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("fetch column types for keyspace sample: %w", err)
	}
	values, mappers := prepSnapshotScannerAndMappers(types)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("fetch columns for keyspace sample: %w", err)
	}

	var samples []map[string]any
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("scanning sampled key for table '%s': %w", table.FullName(), err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			v, err := mappers[i](values[i])
			if err != nil {
				return nil, fmt.Errorf("decoding sampled column %s for table '%s': %w", col, table.FullName(), err)
			}
			row[col] = v
		}
		samples = append(samples, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating sampled keys for table '%s': %w", table.FullName(), err)
	}
	return samples, nil
}

// buildLexicographicBoundExpr expands a composite key bound into the lexicographic form
// Oracle needs in place of tuple comparison: (col1, col2) > (v1, v2) becomes
// (col1 > v1) OR (col1 = v1 AND col2 > v2).
//
// op applies only to the last column; earlier columns use its strict form, so "<=" yields
// (col1 < v1) OR (col1 = v1 AND col2 <= v2). paramIdx is shared across calls so bind
// parameters in one query stay contiguous.
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

// querySnapshotTable fetches the next page of table, or of customQuery if set, using
// PK-keyset pagination. lastSeenPkVal is the exclusive lower bound (nil on the first page)
// and maxInclusivePkVal the optional inclusive upper bound of a chunk.
//
// For the base table, a valid scn adds AS OF SCN so chunks read a consistent point in time
// without a shared transaction. customQuery relies on its READ ONLY transaction instead.
func querySnapshotTable(ctx context.Context, db DBQuerier, table UserTable, pk []string, lastSeenPkVal, maxInclusivePkVal map[string]any, limit int, customQuery string, scn SCN) (*sql.Rows, error) {
	baseTableRef := fmt.Sprintf(`"%s"."%s"`, table.Schema, table.Name)
	flashback := customQuery == "" && scn != InvalidSCN
	if flashback {
		baseTableRef = fmt.Sprintf("%s AS OF SCN %d", baseTableRef, scn)
	}

	// Oracle uses FETCH FIRST instead of TOP, and it comes at the end
	if lastSeenPkVal == nil && maxInclusivePkVal == nil {
		// No cursor: use custom query directly to avoid a redundant wrapping subquery.
		var base string
		if customQuery != "" {
			base = customQuery
		} else {
			base = "SELECT * FROM " + baseTableRef
		}
		q := strings.Join([]string{base, buildOrderByClause(pk), fmt.Sprintf("FETCH FIRST %d ROWS ONLY", limit)}, " ")
		rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		})
		if err != nil && flashback {
			err = withFlashbackPrivilegeHint(err, table)
		}
		return rows, err
	}

	// Bounded pagination requires a WHERE clause; wrap the custom query in a subquery so
	// the added WHERE does not conflict with any WHERE already present in customQuery.
	var tableSource string
	if customQuery != "" {
		tableSource = fmt.Sprintf("(%s) t", customQuery)
	} else {
		tableSource = baseTableRef
	}

	var (
		args     []any
		paramIdx int
		clauses  []string
	)
	if lastSeenPkVal != nil {
		expr, exprArgs := buildLexicographicBoundExpr(pk, lastSeenPkVal, ">", &paramIdx)
		clauses = append(clauses, "("+expr+")")
		args = append(args, exprArgs...)
	}
	if maxInclusivePkVal != nil {
		expr, exprArgs := buildLexicographicBoundExpr(pk, maxInclusivePkVal, "<=", &paramIdx)
		clauses = append(clauses, "("+expr+")")
		args = append(args, exprArgs...)
	}

	snapshotQueryParts := []string{
		"SELECT * FROM " + tableSource,
		"WHERE " + strings.Join(clauses, " AND "),
		buildOrderByClause(pk),
		fmt.Sprintf("FETCH FIRST %d ROWS ONLY", limit),
	}
	q := strings.Join(snapshotQueryParts, " ")
	rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
		return db.QueryContext(ctx, q, args...)
	})
	if err != nil {
		var oraErr *goora.OracleError
		if customQuery != "" && errors.As(err, &oraErr) && oraErr.ErrCode == errCodeInvalidIdentifier {
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
	return rows, nil
}

// Close safely closes all open connections opened for the snapshotting process.
// It should be called after a non-recoverale error or once the snapshot process has completed.
func (s *Snapshot) Close() error {
	if s.dbPool != nil {
		if err := s.dbPool.Close(); err != nil {
			return fmt.Errorf("closing database connection: %w", err)
		}
	}
	return nil
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

// SnapshotScanDest returns the scan destination and value mapper for a column
// with the given go-ora driver type name (sql.ColumnType.DatabaseTypeName())
// and decimal metadata. It is exported so tests can pin its classification of
// every driver type-name spelling against the schema mapping's — the two are
// separate case-sensitive enumerations of the same fact and have drifted
// before (see TestSnapshotScannerSchemaParity).
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

// buildColumnMeta extracts lightweight type metadata from sql.ColumnType values
// for carrying through MessageEvent to the schema cache.
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

// IsLOBTypeName reports whether the given go-ora driver type name denotes a
// large-object column, whose value is nulled in snapshot rows when
// lob_enabled is false. Exported so tests can pin its classification against
// the schema mapping and scan-destination enumerations of the same spellings
// (see TestSnapshotScannerSchemaParity).
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
