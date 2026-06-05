// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package replication implements DB2 SQL Replication CDC streaming and the
// open-window deduplication algorithm for incremental snapshotting.
//
// # Open-Window Deduplication Algorithm
//
// Adapted from Netflix DBLog / Debezium DB2 connector. Guarantees that each
// source row is emitted exactly once (op="r") or superseded by the CDC event
// that modified it during the snapshot window.
//
//	┌──────────────────────────────────────────────────────────────────────┐
//	│                 INCREMENTAL SNAPSHOT ENGINE                          │
//	│                                                                      │
//	│  CDC stream ──────────────────────────────────────────────────────►  │
//	│  (always running)                                                    │
//	│                                                                      │
//	│  For each chunk:                                                     │
//	│                                                                      │
//	│  1. lowCSN  = MAX(SYNCHPOINT)   ←── watermark before SELECT         │
//	│  2. SELECT * FROM table WHERE pk > lastKey ORDER BY pk               │
//	│     FETCH FIRST chunkSize ROWS ONLY                                  │
//	│  3. highCSN = MAX(SYNCHPOINT)   ←── watermark after SELECT          │
//	│                                                                      │
//	│  4. Open DeduplicationWindow(lowCSN, highCSN)                        │
//	│       ┌────────────────────────────────────────────────┐             │
//	│       │  CDC event arrives with CSN in (low, high]?   │             │
//	│       │  YES → Evict(pk) from window                  │             │
//	│       │  NO  → Pass through to output                 │             │
//	│       └────────────────────────────────────────────────┘             │
//	│                                                                      │
//	│  5. Streamer advances past highCSN → calls window.Flush()            │
//	│  6. Surviving rows emitted as op="r" (snapshot read)                 │
//	│  7. Checkpoint: save LastEmittedPK so crash can resume here          │
//	│  8. Repeat from step 1 with lastKey = LastPKValues()                 │
//	│     until chunk is empty (table fully snapshotted)                   │
//	└──────────────────────────────────────────────────────────────────────┘
//
// Correctness invariant: any row modified during (lowCSN, highCSN] appears in
// the CDC stream and is evicted. Snapshot emits only rows whose state was
// stable across the entire chunk window.
package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ---- IncrementalTable -------------------------------------------------------

// IncrementalTable describes one entry in the pending incremental snapshot queue.
// Tables are processed in queue order; each table is fully snapshotted before
// the next one begins. The queue is persisted in the checkpoint store so that a
// restart resumes the in-progress snapshot rather than discarding completed work.
type IncrementalTable struct {
	Schema   string `json:"schema"`    // DB2 source schema (TABSCHEMA), upper-cased
	Name     string `json:"name"`      // DB2 table name (TABNAME), upper-cased
	SignalID string `json:"signal_id"` // ID of the execute-snapshot signal that triggered this entry
}

// ---- IncrementalSnapshotContext ---------------------------------------------

// IncrementalSnapshotContext holds all persisted state for an in-progress
// incremental snapshot. Serialized into the checkpoint store so restarts
// resume at the exact last-emitted row.
type IncrementalSnapshotContext struct {
	// Tables is the ordered queue of tables still to be snapshotted.
	Tables []IncrementalTable `json:"tables"`

	// LastEmittedPK is the last primary-key value successfully emitted to the
	// event channel for each table (keyed "schema.table"). The next chunk
	// query starts WHERE pk > LastEmittedPK.
	LastEmittedPK map[string][]any `json:"last_emitted_pk"`

	// MaxPK is the upper-bound primary key captured at chunk-0 time (SELECT MAX(pk)).
	// Stored for observability; the chunk loop ends when the chunk query returns empty.
	MaxPK map[string][]any `json:"max_pk"`

	// SignalIDs holds the IDs of the execute-snapshot signal rows that triggered
	// this snapshot. Persisted so that if signal DELETE fails after all tables
	// are snapshotted, the next session can retry deletion using this set rather
	// than re-running the full snapshot.
	SignalIDs []string `json:"signal_ids,omitempty"`
}

// Running reports whether the incremental snapshot has tables remaining in the
// queue. Returns false when all tables have been fully snapshotted.
func (c *IncrementalSnapshotContext) Running() bool {
	return len(c.Tables) > 0
}

// CurrentTable returns a pointer to the head of the snapshot queue, or nil when
// the queue is empty (Running() == false). The caller must not modify the
// returned value directly; use AdvanceTable to dequeue the current table.
func (c *IncrementalSnapshotContext) CurrentTable() *IncrementalTable {
	if !c.Running() {
		return nil
	}
	return &c.Tables[0]
}

// AdvanceTable dequeues the current table from the snapshot queue.
// After AdvanceTable, CurrentTable returns the next table, or nil if
// the queue is now empty. Callers should persist the context after advancing.
func (c *IncrementalSnapshotContext) AdvanceTable() {
	if len(c.Tables) > 0 {
		c.Tables = c.Tables[1:]
	}
}

// ---- DeduplicationWindow ----------------------------------------------------

// DeduplicationWindow holds in-memory snapshot rows for one chunk.
// Filled by ChunkReader; CDC events arriving within (LowCSN, HighCSN] evict
// rows whose PK was modified. Surviving rows are flushed as OpTypeRead events.
//
// Thread-safety: mu protects all mutable map/slice fields. AddRow is called
// only from the engine goroutine before SetWindow; Evict/Flush are called from
// the Streamer goroutine after SetWindow. The mu guards against future callers
// that might violate this implicit sequencing, and against data-race detection.
type DeduplicationWindow struct {
	Schema  string
	Table   string
	LowCSN  CSN // exclusive lower bound (stream was here when chunk started)
	HighCSN CSN // inclusive upper bound (MAX(SYNCHPOINT) after chunk SELECT)

	mu           sync.Mutex
	pks          []string
	rows         map[string]map[string]any
	pkOrder      []string // preserves insertion order for deterministic Flush output
	lastUniquePK []any    // PK values of the last row added via a new (unseen) key

	flushedOnce sync.Once
	flushed     chan struct{} // closed when Flush() is called
}

// NewDeduplicationWindow creates a DeduplicationWindow for the given table and
// CSN bracket. pks is the ordered list of primary-key column names used to build
// per-row identity keys. The LowCSN and HighCSN bounds define the half-open
// interval (LowCSN, HighCSN] within which CDC events will evict snapshot rows.
func NewDeduplicationWindow(schema, table string, lowCSN, highCSN CSN, pks []string) *DeduplicationWindow {
	return &DeduplicationWindow{
		Schema:  schema,
		Table:   table,
		LowCSN:  lowCSN,
		HighCSN: highCSN,
		pks:     pks,
		rows:    make(map[string]map[string]any),
		flushed: make(chan struct{}),
	}
}

// normalizePKVal converts []byte values to string so that snapshot rows
// (where the DB2 ODBC driver returns CHAR/VARCHAR columns as []byte) and
// CDC rows (where convertDB2Value has already converted CHAR/VARCHAR to string)
// produce identical pkKey lookup keys. Without this normalisation, Evict always
// misses for varchar/char PK tables, causing every modified row to be emitted
// twice (once as op="r" from the snapshot and once from the CDC stream).
func normalizePKVal(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// pkKey builds a deterministic lookup key from the PK column values in rowData.
// Uses strings.Builder to avoid the JSON encoder overhead (alloc + reflection)
// that the previous json.Marshal approach paid on every AddRow and Evict call.
// The format is "\x00"-delimited scalar representations — sufficient because:
//   - PK column values are scalar types (int64, string, json.Number, nil)
//   - The key is only used for map lookups, never serialised or parsed
//   - "\x00" cannot appear in a valid DB2 identifier or scalar column value
//   - normalizePKVal ensures []byte and string produce the same key
func (w *DeduplicationWindow) pkKey(rowData map[string]any) string {
	var sb strings.Builder
	sb.Grow(len(w.pks) * 16)
	for i, pk := range w.pks {
		if i > 0 {
			sb.WriteByte(0) // \x00 separator
		}
		// Type-switch on the expected scalar PK types to avoid fmt.Fprintf's
		// reflection overhead (called per-row in the hot snapshot path).
		switch v := normalizePKVal(rowData[pk]).(type) {
		case string:
			sb.WriteString(v)
		case int64:
			var buf [20]byte
			sb.Write(strconv.AppendInt(buf[:0], v, 10))
		case int:
			var buf [20]byte
			sb.Write(strconv.AppendInt(buf[:0], int64(v), 10))
		case json.Number:
			sb.WriteString(v.String())
		case float64:
			var buf [32]byte
			sb.Write(strconv.AppendFloat(buf[:0], v, 'f', -1, 64))
		case nil:
			sb.WriteString("\x00<nil>") // \x00 prefix distinguishes SQL NULL from the literal string "<nil>"
		default:
			fmt.Fprintf(&sb, "%v", v)
		}
	}
	return sb.String()
}

// AddRow adds a snapshot row to the window. If a row with the same PK was
// already added, it is replaced with the new value (last-write wins for the
// chunk SELECT). Duplicate PKs within a chunk SELECT should not occur in
// practice (primary keys are unique by definition), but the replacement
// behaviour is safe.
func (w *DeduplicationWindow) AddRow(row map[string]any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := w.pkKey(row)
	if _, exists := w.rows[key]; !exists {
		w.pkOrder = append(w.pkOrder, key)
		// Capture PK values for the keyset cursor. Done only on first-seen keys
		// so lastUniquePK always reflects pkOrder[len-1], not a later duplicate.
		pkVals := make([]any, len(w.pks))
		for i, pk := range w.pks {
			pkVals[i] = normalizePKVal(row[pk])
		}
		w.lastUniquePK = pkVals
	}
	w.rows[key] = row
}

// Evict removes the row matching pk if the CDC event's CSN falls within
// (LowCSN, HighCSN] and the schema/table match. Returns true if evicted.
// The CDC copy is authoritative; the snapshot copy is discarded.
func (w *DeduplicationWindow) Evict(schema, table string, rowData map[string]any, csn CSN) bool {
	if schema != w.Schema || table != w.Table {
		return false
	}
	// Half-open interval: csn must be strictly after LowCSN and at-or-before HighCSN.
	// When HighCSN is null (newly-registered table with no committed txns yet),
	// there is no upper bound — evict any event after LowCSN so that snapshot
	// rows are not duplicated by the CDC stream.
	if !csn.Greater(w.LowCSN) || (!w.HighCSN.IsNull() && csn.Greater(w.HighCSN)) {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	key := w.pkKey(rowData)
	if _, ok := w.rows[key]; ok {
		delete(w.rows, key)
		return true
	}
	return false
}

// Len returns the number of rows surviving in the window (added minus evicted).
// After Flush() the rows map is cleared and Len returns 0, but LastPKValues()
// still returns the last PK for keyset pagination.
func (w *DeduplicationWindow) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.rows)
}

// Empty reports whether no rows were added to the window, meaning the chunk
// SELECT returned zero rows and the table has been fully snapshotted.
func (w *DeduplicationWindow) Empty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pkOrder) == 0
}

// LastPKValues returns the primary-key values of the last (highest-PK) row
// added to the window, in PK column order. Returns nil if the window is empty.
// Used as the keyset cursor for the next chunk's WHERE pk > lastKey predicate.
// The values are valid even after Flush() because lastUniquePK is not cleared.
func (w *DeduplicationWindow) LastPKValues() []any {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastUniquePK
}

// Flush returns surviving rows as OpTypeRead ChangeEvents and signals any Wait callers.
// Calling Flush more than once returns an empty slice after the first call.
func (w *DeduplicationWindow) Flush() []ChangeEvent {
	w.mu.Lock()
	events := make([]ChangeEvent, 0, len(w.rows))
	for _, key := range w.pkOrder {
		row, ok := w.rows[key]
		if !ok {
			continue
		}
		events = append(events, ChangeEvent{
			Schema:    w.Schema,
			Table:     w.Table,
			Operation: OpTypeRead,
			CSN:       NullCSN(),
			Data:      row,
			PKColumns: w.pks,
		})
	}
	clear(w.rows)
	// Preserve pkOrder so LastPKValues() can still return the last-row PK after
	// Flush() — RunTable uses this as a keyset-pagination cursor for the next chunk.
	// Signal under the lock so no Evict() can interleave between "state committed"
	// and "flushed channel closed", which would corrupt pkOrder seen by Wait() callers.
	w.flushedOnce.Do(func() { close(w.flushed) })
	w.mu.Unlock()
	return events
}

// Wait blocks until Flush has been called or ctx is cancelled.
// flushed is given priority over ctx: if both are ready simultaneously, a
// closed flushed channel means the rows were already emitted and the caller
// should advance the checkpoint rather than treating the flush as a loss.
func (w *DeduplicationWindow) Wait(ctx context.Context) error {
	select {
	case <-w.flushed:
		return nil
	case <-ctx.Done():
		// Re-check flushed: if both fired simultaneously prefer the flush result
		// so the engine advances LastEmittedPK and avoids duplicate rows on resume.
		select {
		case <-w.flushed:
			return nil
		default:
		}
		return ctx.Err()
	}
}

// ---- ChunkReader ------------------------------------------------------------

// ChunkReader reads one chunk of rows from a source table, bracketed by CSN watermarks.
type ChunkReader struct {
	db        *sql.DB
	cdcSchema string
	schema    string
	table     string
	pks       []string
	batchSize int
	// Pre-computed SQL strings avoid per-chunk fmt.Sprintf / strings.Builder overhead.
	synchpointQuery string
	queryNoBounds   string // SELECT … ORDER BY … FETCH FIRST N
	queryLowerOnly  string // SELECT … WHERE pk > (?) ORDER BY … FETCH FIRST N
	queryUpperOnly  string // SELECT … WHERE pk <= (?) ORDER BY … FETCH FIRST N
	queryBothBounds string // SELECT … WHERE pk > (?) AND pk <= (?) ORDER BY … FETCH FIRST N
	// cachedCols caches the column names after the first chunk to avoid calling
	// rows.Columns() (which invokes SQLDescribeCol via CGo) on every chunk.
	cachedCols []string
}

// NewChunkReader creates a ChunkReader for one table.
// All identifier arguments (cdcSchema, schema, table, pks) are embedded in SQL
// format strings; callers are responsible for validating them with
// isValidDB2Identifier before constructing a ChunkReader.
func NewChunkReader(db *sql.DB, cdcSchema, schema, table string, pks []string, batchSize int) *ChunkReader {
	quotedSchema := QuoteDB2Identifier(schema)
	quotedTable := QuoteDB2Identifier(table)
	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = QuoteDB2Identifier(pk)
	}

	base := fmt.Sprintf("SELECT * FROM %s.%s", quotedSchema, quotedTable)
	orderSuffix := fmt.Sprintf(" ORDER BY %s FETCH FIRST %d ROWS ONLY",
		strings.Join(quotedPKs, ", "), batchSize)

	pkBound := func(op string) string {
		if len(quotedPKs) == 1 {
			return quotedPKs[0] + " " + op + " ?"
		}
		pkList := strings.Join(quotedPKs, ", ")
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(quotedPKs)), ", ")
		return fmt.Sprintf("(%s) %s (%s)", pkList, op, placeholders)
	}

	lowerClause := pkBound(">")
	upperClause := pkBound("<=")

	return &ChunkReader{
		db:        db,
		cdcSchema: cdcSchema,
		schema:    schema,
		table:     table,
		pks:       pks,
		batchSize: batchSize,
		// Prefer SYNCHPOINT (capture-daemon high-water mark) and fall back to
		// CD_NEW_SYNCHPOINT (registration watermark) only when SYNCHPOINT is NULL
		// for newly-added tables. Using MAX(CD_NEW_SYNCHPOINT, SYNCHPOINT) can produce
		// a watermark ahead of where the streamer has processed, causing Wait() to
		// block indefinitely on tables with no recent traffic.
		// Filter by both SOURCE_OWNER and SOURCE_TABLE so that a high-watermark from
		// a different table in the same schema does not inflate lowCSN for this table,
		// which would cause the deduplication window to evict snapshot rows that the
		// CDC stream has not yet delivered.
		// Use ? parameters for SOURCE_OWNER/SOURCE_TABLE (string-column predicates);
		// cdcSchema is a validated identifier and is safe to embed directly.
		synchpointQuery: fmt.Sprintf(`
			SELECT COALESCE(MAX(SYNCHPOINT), MAX(CD_NEW_SYNCHPOINT))
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER = ?
			   AND SOURCE_TABLE = ?`,
			cdcSchema,
		),
		queryNoBounds:   base + orderSuffix,
		queryLowerOnly:  base + " WHERE " + lowerClause + orderSuffix,
		queryUpperOnly:  base + " WHERE " + upperClause + orderSuffix,
		queryBothBounds: base + " WHERE " + lowerClause + " AND " + upperClause + orderSuffix,
	}
}

// ReadChunk reads one chunk starting after lastKey (nil = first chunk) and
// bounded above by maxKey (nil = no upper bound). maxKey must be the value
// returned by selectMaxPK at snapshot start so the chunk loop terminates even
// when rows are inserted into the table during snapshotting.
// Returns a DeduplicationWindow ready for Streamer-driven eviction.
func (r *ChunkReader) ReadChunk(ctx context.Context, streamCSN CSN, lastKey, maxKey []any) (*DeduplicationWindow, error) {
	// Capture lowCSN = MAX(SYNCHPOINT) just before the chunk SELECT.
	lowCSN, err := r.captureMaxSynchpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("capturing low watermark: %w", err)
	}
	// Use the higher of the stream's current CSN and the DB watermark as the
	// effective lower bound so any CDC events since the last stream poll are
	// included in the window.
	if streamCSN.Greater(lowCSN) {
		lowCSN = streamCSN
	}

	rowData, err := r.selectChunk(ctx, lastKey, maxKey)
	if err != nil {
		return nil, fmt.Errorf("reading chunk: %w", err)
	}

	// Capture highCSN = MAX(SYNCHPOINT) just after the chunk SELECT.
	highCSN, err := r.captureMaxSynchpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("capturing high watermark: %w", err)
	}

	// Guard against an inverted window bracket: if the CDC stream has already
	// advanced past the post-SELECT DB watermark (e.g. asncap wrote the commit
	// record to IBMSNAP_REGISTER AFTER our highCSN read), clamp highCSN to
	// lowCSN.  The resulting bracket (lowCSN, lowCSN] is empty, so no CDC
	// events are evicted — but the streamer has already passed highCSN and will
	// flush the window immediately, emitting all snapshot rows correctly.
	if highCSN.Less(lowCSN) {
		highCSN = lowCSN
	}

	win := NewDeduplicationWindow(r.schema, r.table, lowCSN, highCSN, r.pks)
	win.pkOrder = make([]string, 0, len(rowData)) // pre-size to exact chunk length
	for _, rowMap := range rowData {
		win.AddRow(rowMap)
	}
	return win, nil
}

// captureMaxSynchpoint queries the highest committed CDC watermark from
// IBMSNAP_REGISTER for the source schema.  It unions both SYNCHPOINT
// (updated after each captured transaction) and CD_NEW_SYNCHPOINT (set at
// ADDTABLE time) so that newly-registered tables with a null SYNCHPOINT still
// return a non-null watermark, matching the logic in Streamer.getUpperBound.
func (r *ChunkReader) captureMaxSynchpoint(ctx context.Context) (CSN, error) {
	var synchBytes []byte
	if err := r.db.QueryRowContext(ctx, r.synchpointQuery, r.schema, r.table).Scan(&synchBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Table not yet registered in IBMSNAP_REGISTER — return NullCSN so
			// the caller treats this as "no watermark" rather than a fatal error.
			return NullCSN(), nil
		}
		return CSN{}, fmt.Errorf("querying SYNCHPOINT watermark: %w", err)
	}
	if len(synchBytes) == 0 {
		return NullCSN(), nil
	}
	return NewCSNFromDBValue(synchBytes), nil
}

// selectChunk issues a keyset-paginated SELECT for the chunk.
// lastKey is the exclusive lower bound (nil = start from beginning).
// maxKey is the inclusive upper bound captured at snapshot start (nil = unbounded).
// Returns rows as pre-built column→value maps, eliminating the []any→map
// conversion that ReadChunk would otherwise do on every row.
func (r *ChunkReader) selectChunk(ctx context.Context, lastKey, maxKey []any) ([]map[string]any, error) {
	// Select the pre-built query string based on which bounds are present,
	// avoiding per-chunk string allocation.
	var query string
	var args []any
	switch {
	case len(lastKey) > 0 && len(maxKey) > 0:
		query = r.queryBothBounds
		args = make([]any, 0, len(lastKey)+len(maxKey))
		args = append(args, lastKey...)
		args = append(args, maxKey...)
	case len(lastKey) > 0:
		query = r.queryLowerOnly
		args = lastKey
	case len(maxKey) > 0:
		// Upper bound from snapshot-start maxPK prevents chasing rows inserted
		// during snapshotting (would cause an infinite chunk loop on active tables).
		query = r.queryUpperOnly
		args = maxKey
	default:
		query = r.queryNoBounds
	}

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Cache column names after the first chunk to avoid per-chunk SQLDescribeCol
	// round-trips. Invalidate if the table schema changed (e.g. ADDCOLUMN).
	freshCols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if r.cachedCols == nil || len(r.cachedCols) != len(freshCols) {
		r.cachedCols = freshCols
	}
	cols := r.cachedCols

	// scanDest is a shared buffer reused across rows: Scan fills it, then we
	// copy values into a new map before the next rows.Next() call overwrites it.
	// This halves per-row allocations vs allocating a fresh []any per row.
	scanDest := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range cols {
		ptrs[i] = &scanDest[i]
	}

	result := make([]map[string]any, 0, r.batchSize)
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		rowMap := make(map[string]any, len(cols))
		for i, col := range cols {
			rowMap[col] = scanDest[i]
		}
		result = append(result, rowMap)
	}
	return result, rows.Err()
}

// ---- Logger -----------------------------------------------------------------

// Logger is the minimal logging interface used by IncrementalSnapshotEngine to
// emit progress messages and warnings. It is satisfied by *service.Logger
// (Redpanda Connect's logging type) and by the noopLogger stub used in tests.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Infof(string, ...any) {}
func (noopLogger) Warnf(string, ...any) {}

// ---- IncrementalSnapshotEngine ----------------------------------------------

// IncrementalSnapshotEngine drives the open-window chunk loop for one table at
// a time. It coordinates with a running Streamer via DeduplicationWindow: for
// each chunk it captures a low/high CSN watermark, hands the window to the
// Streamer for in-flight CDC event eviction, and waits for the Streamer to pass
// the highCSN before flushing surviving snapshot rows as OpTypeRead events.
// Concurrency: RunTable blocks the calling goroutine until one table is complete.
// The Streamer runs concurrently on a separate goroutine.
type IncrementalSnapshotEngine struct {
	db           *sql.DB
	cdcSchema    string
	chunkSize    int
	streamer     *Streamer
	checkpointFn func(ctx context.Context, ictx *IncrementalSnapshotContext) error
	logger       Logger
}

// NewIncrementalSnapshotEngine creates an IncrementalSnapshotEngine that uses db
// to read source table rows, cdcSchema to query IBMSNAP_REGISTER for watermarks,
// chunkSize as the FETCH FIRST N ROWS ONLY limit per chunk, streamer to install
// DeduplicationWindows, checkpointFn to persist progress after each chunk, and
// logger to emit per-chunk progress (pass nil for no-op logging).
func NewIncrementalSnapshotEngine(
	db *sql.DB,
	cdcSchema string,
	chunkSize int,
	streamer *Streamer,
	checkpointFn func(ctx context.Context, ictx *IncrementalSnapshotContext) error,
	logger Logger,
) *IncrementalSnapshotEngine {
	if logger == nil {
		logger = noopLogger{}
	}
	return &IncrementalSnapshotEngine{
		db:           db,
		cdcSchema:    cdcSchema,
		chunkSize:    chunkSize,
		streamer:     streamer,
		checkpointFn: checkpointFn,
		logger:       logger,
	}
}

// RunTable snapshots one table using the open-window algorithm.
// It calls checkpointFn after each successful chunk so a crash resumes from
// the last fully-emitted primary key.
// Returns nil when the table is fully snapshotted.
func (e *IncrementalSnapshotEngine) RunTable(
	ctx context.Context,
	ictx *IncrementalSnapshotContext,
	table IncrementalTable,
	streamCSN CSN,
) error {
	// Defense-in-depth: validate identifiers at the engine boundary so no
	// SQL injection is possible even if the caller skips upstream validation.
	if !isValidDB2IdentifierInternal(table.Schema) || !isValidDB2IdentifierInternal(table.Name) {
		return fmt.Errorf("unsafe table identifier %q.%q: must be non-empty uppercase alphanumeric+underscore", table.Schema, table.Name)
	}

	tableKey := table.Schema + "." + table.Name

	pks, err := e.discoverPrimaryKeys(ctx, table.Schema, table.Name)
	if err != nil {
		return fmt.Errorf("discovering primary keys for %s.%s: %w", table.Schema, table.Name, err)
	}
	for _, pk := range pks {
		if !isValidDB2IdentifierInternal(pk) {
			return fmt.Errorf("catalog returned unsafe PK column name %q for %s.%s", pk, table.Schema, table.Name)
		}
	}

	if err := e.checkPKNotNull(ctx, table.Schema, table.Name, pks); err != nil {
		return fmt.Errorf("primary key nullability check for %s.%s: %w", table.Schema, table.Name, err)
	}

	// Discover and persist max PK only on the first chunk (not on resume).
	if _, ok := ictx.MaxPK[tableKey]; !ok {
		maxPK, err := e.selectMaxPK(ctx, table.Schema, table.Name, pks)
		if err != nil {
			return fmt.Errorf("selecting max PK for %s.%s: %w", table.Schema, table.Name, err)
		}
		if maxPK == nil {
			// Table is empty at snapshot start — nothing to snapshot.
			// Store an empty (non-nil) slice as a sentinel so a checkpoint resume
			// does not re-run selectMaxPK and re-enter the loop.
			ictx.MaxPK[tableKey] = []any{}
			e.logger.Infof("incremental snapshot: table=%q.%q is empty, skipping", table.Schema, table.Name)
			return nil
		}
		ictx.MaxPK[tableKey] = maxPK
	}

	// An empty-sentinel MaxPK means the table was empty at snapshot start.
	if len(ictx.MaxPK[tableKey]) == 0 {
		return nil
	}

	cr := NewChunkReader(e.db, e.cdcSchema, table.Schema, table.Name, pks, e.chunkSize)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastKey := ictx.LastEmittedPK[tableKey]
		maxKey := ictx.MaxPK[tableKey]
		win, err := cr.ReadChunk(ctx, streamCSN, lastKey, maxKey)
		if err != nil {
			return fmt.Errorf("reading chunk for %s.%s: %w", table.Schema, table.Name, err)
		}

		e.logger.Infof("incremental snapshot chunk: table=%q.%q lastKeyLen=%d csnWindow=[%s,%s]",
			table.Schema, table.Name, len(ictx.LastEmittedPK[tableKey]), win.LowCSN, win.HighCSN)

		if win.Empty() {
			// No more rows — table fully snapshotted.
			return nil
		}

		rowsInChunk := win.Len()

		// Hand the window to the Streamer; CDC events arriving in-window will
		// evict rows before they reach the handler. The Streamer flushes the
		// window once it has processed all events up to HighCSN.
		e.streamer.SetWindow(win)

		// Block until the Streamer has flushed the window (passed HighCSN).
		if err := win.Wait(ctx); err != nil {
			// Remove the stale window reference so CDC events on the next
			// snapshot round do not incorrectly evict rows from a new window.
			e.streamer.ClearWindow(win)
			return err
		}

		e.logger.Infof("incremental snapshot chunk: table=%q.%q chunkRows=%d csnLow=%s csnHigh=%s",
			table.Schema, table.Name, rowsInChunk, win.LowCSN, win.HighCSN)

		// Advance the resume position to the last row emitted by this chunk.
		if lastPK := win.LastPKValues(); lastPK != nil {
			ictx.LastEmittedPK[tableKey] = lastPK
		}
		// Only advance streamCSN if the window produced a real upper bound;
		// a null HighCSN (newly-registered table with no committed transactions)
		// must not overwrite an already-established floor — doing so would cause
		// the next chunk's LowCSN to regress, widening the eviction window and
		// risking duplicate/out-of-order delivery.
		if !win.HighCSN.IsNull() && win.HighCSN.Greater(streamCSN) {
			streamCSN = win.HighCSN
		}

		if err := e.checkpointFn(ctx, ictx); err != nil {
			return fmt.Errorf("checkpointing after chunk: %w", err)
		}
	}
}

// discoverPrimaryKeys queries SYSCAT.KEYCOLUSE for the PK column list.
func (e *IncrementalSnapshotEngine) discoverPrimaryKeys(ctx context.Context, schema, table string) ([]string, error) {
	query := `
		SELECT k.COLNAME
		FROM SYSCAT.KEYCOLUSE k
		JOIN SYSCAT.TABCONST c
		  ON c.CONSTNAME = k.CONSTNAME
		 AND c.TABSCHEMA = k.TABSCHEMA
		 AND c.TABNAME   = k.TABNAME
		WHERE c.TYPE     = 'P'
		  AND k.TABSCHEMA = ?
		  AND k.TABNAME   = ?
		ORDER BY k.COLSEQ`

	rows, err := e.db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("querying primary keys: %w", err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pks = append(pks, strings.TrimSpace(col))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(pks) == 0 {
		return nil, fmt.Errorf("table %q.%q has no primary key", schema, table)
	}
	return pks, nil
}

// checkPKNotNull verifies that all pk columns are NOT NULL in SYSCAT.COLUMNS.
// A nullable PK makes WHERE pk > lastKey undefined (NULL comparisons are UNKNOWN in DB2),
// which would silently skip rows and break exactly-once delivery guarantees.
func (e *IncrementalSnapshotEngine) checkPKNotNull(ctx context.Context, schema, table string, pks []string) error {
	if len(pks) == 0 {
		return nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(pks)), ",")
	query := fmt.Sprintf(`
		SELECT COLNAME, NULLS
		FROM SYSCAT.COLUMNS
		WHERE TABSCHEMA = ?
		  AND TABNAME   = ?
		  AND COLNAME IN (%s)`, placeholders)

	args := make([]any, 0, 2+len(pks))
	args = append(args, schema, table)
	for _, pk := range pks {
		args = append(args, pk)
	}

	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("querying column nullability: %w", err)
	}
	defer rows.Close()

	var nullable []string
	for rows.Next() {
		var colName, nulls string
		if err := rows.Scan(&colName, &nulls); err != nil {
			return err
		}
		if strings.TrimSpace(nulls) == "Y" {
			nullable = append(nullable, strings.TrimSpace(colName))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(nullable) > 0 {
		return fmt.Errorf("table %q.%q has nullable primary key column(s) %v: keyset pagination requires NOT NULL PKs",
			schema, table, nullable)
	}
	return nil
}

// selectMaxPK queries the maximum primary key tuple for the snapshot upper bound.
//
// For compound PKs, per-column MAX() produces a synthetic tuple (MAX(a), MAX(b))
// that may not correspond to any real row and can be lexicographically greater than
// any real row. For example, rows (1,100),(2,50) yield MAX(a)=2, MAX(b)=100 → (2,100),
// which exceeds the real max (2,50) and allows rows inserted AFTER highCSN to slip
// through the upper-bound filter without being evicted. To avoid this, we select the
// lexicographically largest row directly using an ORDER BY … DESC FETCH FIRST 1.
func (e *IncrementalSnapshotEngine) selectMaxPK(ctx context.Context, schema, table string, pks []string) ([]any, error) {
	quotedSchema := QuoteDB2Identifier(schema)
	quotedTable := QuoteDB2Identifier(table)
	quotedPKs := make([]string, len(pks))
	for i, pk := range pks {
		quotedPKs[i] = QuoteDB2Identifier(pk)
	}
	// Select only the PK columns in descending order to get the lexicographic maximum.
	// Each quotedPK already carries its own DESC direction; the format string must
	// not add a trailing DESC (that would double-up on single-PK tables).
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s FETCH FIRST 1 ROW ONLY",
		strings.Join(quotedPKs, ", "),
		quotedSchema, quotedTable,
		strings.Join(quotedPKs, " DESC, ")+" DESC",
	)
	row := e.db.QueryRowContext(ctx, query)
	vals := make([]any, len(pks))
	ptrs := make([]any, len(pks))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := row.Scan(ptrs...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("selecting max PK for %s.%s: %w", schema, table, err)
	}
	// Normalize []byte → string so that VARCHAR/CHAR max-PK values survive
	// JSON round-trip in the checkpoint store (json.Marshal encodes []byte as
	// base64, which would not compare correctly as a bound parameter).
	for i, v := range vals {
		vals[i] = normalizePKVal(v)
	}
	return vals, nil
}
