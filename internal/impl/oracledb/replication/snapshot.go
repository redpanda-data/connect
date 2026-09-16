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

// errCodeInvalidIdentifier is Oracle's ORA-00904, raised when a query references a
// column that doesn't exist in its source. Used to detect a custom snapshot filter
// that doesn't project all of a table's primary key columns - see querySnapshotTable.
const errCodeInvalidIdentifier = 904

// errCodeFlashbackTableChanged is Oracle's ORA-01466, which AS OF SCN (flashback) reads
// can raise transiently when issued very soon after the DML that the SCN was captured
// against - Oracle's delayed block cleanout for that DML may not have caught up yet. It's
// self-resolving; see withFlashbackRetry.
const errCodeFlashbackTableChanged = 1466

// flashbackRetryAttempts and flashbackRetryDelay bound the retry in withFlashbackRetry.
// These aren't exposed as config because they guard a narrow, internal-only race window
// (freshly-committed DML settling before it's safely flashback-readable) rather than
// anything a user would tune - by design the retry window closes within a few hundred
// milliseconds of the snapshot's SCN being captured.
const (
	flashbackRetryAttempts = 5
	flashbackRetryDelay    = 100 * time.Millisecond
)

// withFlashbackRetry retries fn a bounded number of times on ORA-01466 (see
// errCodeFlashbackTableChanged), which every AS OF SCN read in this file's chunked
// snapshot path can hit transiently right after Prepare captures its SCN. Any other error
// is returned immediately.
func withFlashbackRetry[T any](ctx context.Context, fn func() (T, error)) (result T, err error) {
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

// overSampleFactor controls how many primary-key samples are drawn per worker when
// planning parallel snapshot chunks, matching pglogicalstream's processSnapshot: enough
// samples to bucket the keyspace fairly, without the sampling query itself becoming
// expensive.
const overSampleFactor = 32

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
//   - no filter, no discoverable primary key -> a single full unordered-scan task,
//     unchanged from before (preserves the ability to snapshot PK-less tables).
//   - no filter, has a primary key -> the keyspace is sampled and split into up to
//     maxWorkers range-bounded chunk tasks, each independently consistent via AS OF SCN.
//     Small tables naturally collapse to a single chunk (see planChunkRanges).
func (s *Snapshot) planTableTasks(ctx context.Context, table UserTable, maxWorkers, maxBatchSize int) ([]func(context.Context) error, error) {
	tableName := table.FullName()
	l := s.log.With("src_table", tableName)
	customQuery, hasFilter := s.filters[tableName]

	if hasFilter {
		l.Infof("Launching snapshot of table '%s' with snapshot filter", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableWithFilterTask(table, maxBatchSize, customQuery, tracker)}, nil
	}

	var pks []string
	if err := s.withPDBConn(ctx, func(db DBQuerier) (err error) {
		pks, err = getTablePrimaryKeys(ctx, db, table)
		return err
	}); err != nil {
		if !errors.Is(err, errNoPrimaryKey) {
			return nil, err
		}
		l.Infof("Launching snapshot of table '%s'", tableName)
		tracker := newTableCompletionTracker(s, table, 1)
		return []func(context.Context) error{s.snapshotTableFullScanTask(table, maxBatchSize, tracker)}, nil
	}

	ranges, err := s.planChunkRanges(ctx, table, pks, maxWorkers)
	if err != nil {
		return nil, fmt.Errorf("planning chunk ranges: %w", err)
	}

	if len(ranges) > 1 {
		l.Infof("Launching snapshot of table '%s' split into %d parallel chunks", tableName, len(ranges))
	} else {
		l.Infof("Launching snapshot of table '%s'", tableName)
	}

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

// planChunkRanges samples table's primary-key keyspace and buckets it into up to
// maxWorkers roughly-even ranges, mirroring pglogicalstream.processSnapshot's own
// sampled-keyspace chunking. Small tables (where sampling naturally returns few or no
// rows - an Oracle-documented behavior of SAMPLE on small segments) collapse to a single,
// unbounded range rather than failing or over-splitting.
func (s *Snapshot) planChunkRanges(ctx context.Context, table UserTable, pkCols []string, maxWorkers int) ([]chunkRange, error) {
	numSamples := min(maxWorkers, 256) * overSampleFactor

	var samples []map[string]any
	if err := s.withPDBConn(ctx, func(db DBQuerier) error {
		blockCount, err := getTableBlockCount(ctx, db, table)
		if err != nil {
			return err
		}
		samples, err = sampleTableKeyspace(ctx, db, table, pkCols, s.scn, blockCount, numSamples)
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

// withPDBConn runs fn against a database handle suitable for read-only catalog and
// snapshot queries: the shared pool directly in non-CDB mode, or a dedicated *sql.Conn
// switched into the configured PDB (and back to CDB$ROOT afterward) in CDB mode. Unlike
// withReadOnlyTxn, this does not open a transaction - callers here (planning queries and
// AS OF SCN chunk scans) get their consistency from Oracle's flashback query, not from a
// long-lived transaction.
func (s *Snapshot) withPDBConn(ctx context.Context, fn func(db DBQuerier) error) (err error) {
	if s.pdbName == "" {
		return fn(s.dbPool)
	}

	var conn *sql.Conn
	if conn, err = s.dbPool.Conn(ctx); err != nil {
		return fmt.Errorf("acquiring snapshot connection: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			s.log.Errorf("Closing snapshot connection: %v", closeErr)
		}
	}()

	if _, err = conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+s.pdbName); err != nil {
		return fmt.Errorf("switching session to PDB '%s' for snapshot: %w", s.pdbName, err)
	}
	defer func() {
		if _, cerr := conn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); cerr != nil {
			s.log.Errorf("Switching session back to root container: %v", cerr)
		}
	}()

	// Connections obtained after NewSnapshot's construction-time db-wide ApplyNLSSettings
	// call don't inherit that setting automatically, so it must be re-applied here.
	if err = ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("configuring nls for snapshot connection: %w", err)
	}

	return fn(conn)
}

// withReadOnlyTxn runs fn against a *sql.Tx pinned to Oracle's automatic serializable
// isolation for READ ONLY transactions, switching into the configured PDB container first
// when running in CDB mode (and back to CDB$ROOT afterward). Used only by the two
// snapshot paths that still need transaction-scoped consistency - custom-filtered and
// PK-less full-table-scan tables - where AS OF SCN chunking (see scanTableRangeTask)
// isn't applicable.
func (s *Snapshot) withReadOnlyTxn(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	var tx *sql.Tx
	switch {
	case s.pdbName != "":
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

		if _, err = conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+s.pdbName); err != nil {
			return fmt.Errorf("switching session to PDB '%s' for snapshot: %w", s.pdbName, err)
		}
		defer func() {
			if _, cerr := conn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); cerr != nil {
				// logging the error is sufficient here, connection will be closed in defer call above.
				s.log.Errorf("Switching session back to root container: %v", cerr)
			}
		}()
		// Use context.Background() to prevent database/sql from spawning an
		// awaitDone goroutine that races with our explicit Rollback below.
		// The go-ora v2 driver has an unsynchronized field in Session that
		// causes a data race between BreakConnection (from awaitDone) and
		// IsBreak (from our Rollback). Transaction lifetime is managed
		// manually via the defer and explicit Rollback at the end.
		if tx, err = conn.BeginTx(context.Background(), nil); err != nil {
			return fmt.Errorf("beginning snapshot transaction: %w", err)
		}
	default:
		// Non-CDB mode: use db.BeginTx directly — no *Conn needed.
		// See CDB path comment above for why context.Background() is used.
		if tx, err = s.dbPool.BeginTx(context.Background(), nil); err != nil {
			return fmt.Errorf("beginning snapshot transaction: %w", err)
		}
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
			tablePks, err := getTablePrimaryKeys(ctx, tx, table)
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

// snapshotTableFullScanTask returns a task that runs snapshotTableFullScan for tables with
// no discoverable primary key, unchanged from before chunking was introduced.
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
		err := s.withPDBConn(ctx, func(db DBQuerier) error {
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

func getTablePrimaryKeys(ctx context.Context, db DBQuerier, table UserTable) ([]string, error) {
	// ALL_CONSTRAINTS/ALL_CONS_COLUMNS work in any container context after ALTER SESSION SET CONTAINER.
	pkSQL := `
	SELECT acc.column_name
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
		return nil, fmt.Errorf("get primary key: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		pks = append(pks, pk)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("discovering primary keys for table '%s': %w", table.FullName(), err)
	}
	if len(pks) == 0 {
		return nil, fmt.Errorf("%w for table '%s'", errNoPrimaryKey, table.FullName())
	}

	return pks, nil
}

// getTableBlockCount estimates the number of physical blocks used by table, for scaling
// the SAMPLE BLOCK percentage in sampleTableKeyspace. It reads ALL_TABLES rather than the
// more precise DBA_SEGMENTS, since the latter needs elevated catalog privileges a normal
// CDC user may not have. BLOCKS/NUM_ROWS can be null or stale if DBMS_STATS was never run
// against the table; null or zero is treated as 1 block, which pushes the sampling
// percentage formula toward ~100% - a safe, if unoptimized, degrade.
func getTableBlockCount(ctx context.Context, db DBQuerier, table UserTable) (int64, error) {
	var blocks, numRows sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT BLOCKS, NUM_ROWS FROM ALL_TABLES WHERE OWNER = :1 AND TABLE_NAME = :2`,
		table.Schema, table.Name,
	).Scan(&blocks, &numRows); err != nil {
		return 0, fmt.Errorf("getting block count for table '%s': %w", table.FullName(), err)
	}
	if !blocks.Valid || blocks.Int64 <= 0 {
		return 1, nil
	}
	return blocks.Int64, nil
}

// sampleTableKeyspace draws a cheap, roughly-uniform sample of a table's primary-key
// values by combining Oracle's block-level SAMPLE clause with a flashback query pinned to
// scn, then sorting the result. The returned, sorted PK tuples are used as candidate chunk
// boundaries by planChunkRanges. SAMPLE must precede AS OF SCN in the table reference -
// the reverse order is a syntax error (ORA-03049) - and SAMPLE cannot target a subquery or
// view, which is why this (and chunking generally) is restricted to the base-table,
// no-custom-filter path.
//
// Mirrors pglogicalstream's randomlySampleKeyspace percentage formula (using block count
// in place of page count): for small tables this deliberately pushes the requested
// percentage toward 100%, i.e. it accepts scanning nearly everything on small tables
// rather than trying to be clever. Oracle's own documented behavior for SAMPLE on small
// segments means this can still return anywhere from zero rows to the whole table; that's
// fine, since sampling only picks split points, and planChunkRanges collapses to a single
// unbounded range when too few samples come back.
func sampleTableKeyspace(ctx context.Context, db DBQuerier, table UserTable, pkCols []string, scn SCN, blockCount int64, numSamples int) ([]map[string]any, error) {
	// Unlike Postgres's TABLESAMPLE SYSTEM (which accepts up to and including 100), Oracle's
	// SAMPLE clause rejects a percentage of exactly 100 (ORA-30562: invalid SAMPLE
	// percentage), so the upper bound is capped just short of it.
	const maxSamplePercent = 99.9999
	pct := min(maxSamplePercent, max(0.0001, 100.0*float64(numSamples)/float64(max(blockCount, 1))))

	quoted := make([]string, len(pkCols))
	for i, col := range pkCols {
		quoted[i] = `"` + col + `"`
	}
	cols := strings.Join(quoted, ", ")

	q := fmt.Sprintf(
		`SELECT %s FROM "%s"."%s" SAMPLE BLOCK (%.4f) AS OF SCN %d ORDER BY %s`,
		cols, table.Schema, table.Name, pct, uint64(scn), cols,
	)

	rows, err := withFlashbackRetry(ctx, func() (*sql.Rows, error) {
		return db.QueryContext(ctx, q)
	})
	if err != nil {
		return nil, fmt.Errorf("sampling keyspace for table '%s': %w", table.FullName(), err)
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

// buildLexicographicBoundExpr builds a WHERE expression comparing the composite key
// (pk[0], pk[1], ...) against boundVals using a lexicographic tuple comparison - Oracle
// has no native row-value tuple comparison, so a composite bound like (col1, col2) > (v1,
// v2) must be expanded to: (col1 > v1) OR (col1 = v1 AND col2 > v2).
//
// op is the comparison used at the deepest (final) column of each clause; every shallower
// column position uses the strict form of op, so that (for example) an inclusive upper
// bound op of "<=" only becomes non-strict on the very last clause: (col1 < v1) OR (col1 =
// v1 AND col2 <= v2). paramIdx is shared across successive calls (e.g. a lower and an upper
// bound in the same query) so parameter numbering stays contiguous.
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

// querySnapshotTable pages through table (or, for the custom-filter path, customQuery's
// own result set) using PK-keyset pagination. lastSeenPkVal is the exclusive lower bound
// (nil for the first page); maxInclusivePkVal is an optional inclusive upper bound used
// only for the base-table, no-custom-filter chunked scan path (see scanTableRangeTask) -
// SAMPLE, and therefore chunking, can't target the custom-filter form. scn, if not
// InvalidSCN, pins the base-table form to that flashback SCN so every independently
// scheduled chunk (and every table) reads a single consistent point in time without a
// shared, long-lived transaction; it is never applied to the custom-filter form, which
// keeps relying on its enclosing READ ONLY transaction for consistency instead.
func querySnapshotTable(ctx context.Context, db DBQuerier, table UserTable, pk []string, lastSeenPkVal, maxInclusivePkVal map[string]any, limit int, customQuery string, scn SCN) (*sql.Rows, error) {
	baseTableRef := fmt.Sprintf(`"%s"."%s"`, table.Schema, table.Name)
	if customQuery == "" && scn != InvalidSCN {
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
		return withFlashbackRetry(ctx, func() (*sql.Rows, error) {
			return db.QueryContext(ctx, q)
		})
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
