// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// db2IdentifierRE matches valid DB2 identifiers that are safe to embed in SQL
// without quoting: non-empty, first char letter or underscore, rest alphanumeric or underscore.
var db2IdentifierRE = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// StreamConfig holds configuration for the CDC streaming phase. The Streamer
// polls ASNCDC change tables for rows with IBMSNAP_COMMITSEQ > the last seen
// CSN, then sorts, pairs D+I update rows, and delivers them to the event handler.
type StreamConfig struct {
	// Schemas is the list of DB2 source schemas (TABSCHEMA) whose change tables are polled.
	// Must have at least one element; each entry must be a valid DB2 identifier
	// (uppercase alphanumeric+underscore). For single-schema operation pass a one-element slice.
	Schemas []string

	// Tables is the explicit list of table names (without schema prefix) to stream.
	// When empty, all CDC-registered tables for Schema are discovered dynamically
	// from ASNCDC.IBMSNAP_REGISTER during Initialize.
	Tables []string

	// AsnCDCSchema is the schema that owns the SQL Replication control tables
	// (IBMSNAP_REGISTER, and the generated change tables). Defaults to "ASNCDC".
	//
	// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=tables-ibmsnap-register
	AsnCDCSchema string

	// BackoffInterval is the sleep duration between change-table polls when no
	// new rows are found. Should be at least the DB2 capture daemon's commit
	// interval to avoid wasted round-trips.
	BackoffInterval time.Duration

	// PollBatchSize is the FETCH FIRST N ROWS ONLY limit per change-table poll
	// query. A full batch (rawCount == PollBatchSize) triggers safe-CSN logic
	// to avoid advancing the watermark past a truncated transaction.
	PollBatchSize int

	// StartingCSN is the exclusive lower-bound CSN for the first poll. Rows
	// with IBMSNAP_COMMITSEQ <= StartingCSN are skipped. A null CSN means start
	// from the very beginning of the change tables (all existing rows).
	StartingCSN CSN

	// CommitSeqByteLen is the byte length of IBMSNAP_COMMITSEQ in the change
	// tables. DB2 ≤ 11.x uses CHAR(10) FOR BIT DATA (10 bytes); DB2 12.1+ uses
	// CHAR(16) FOR BIT DATA (16 bytes). Zero triggers auto-detection during
	// Initialize via SYSCAT.COLUMNS (the recommended setting).
	//
	// IBM reference: https://www.ibm.com/docs/en/db2/11.5?topic=tables-ibmsnap-uow
	CommitSeqByteLen int

	// TableFilter narrows which tables in Tables (or all discovered tables when
	// Tables is empty) are actually polled. When nil all candidate tables are
	// included.
	TableFilter func(string) bool
}

// asncdcSchema returns the CDC control schema, defaulting to "ASNCDC".
func (c *StreamConfig) asncdcSchema() string {
	if c.AsnCDCSchema != "" {
		return c.AsnCDCSchema
	}
	return "ASNCDC"
}

// schemasINClause returns a SQL IN clause fragment like "'S1', 'S2'".
// Each schema name is validated before embedding so that callers that bypass
// Initialize() cannot inject SQL via an unvalidated Schemas value.
func (c *StreamConfig) schemasINClause() string {
	quoted := make([]string, 0, len(c.Schemas))
	for _, s := range c.Schemas {
		if !isValidDB2IdentifierInternal(s) {
			continue
		}
		quoted = append(quoted, "'"+s+"'")
	}
	return strings.Join(quoted, ", ")
}

// Streamer polls DB2 change tables and emits ChangeEvents in CSN order.
//
// On each poll iteration the Streamer:
//  1. Queries MAX(SYNCHPOINT) from ASNCDC.IBMSNAP_REGISTER as a safe upper
//     bound (avoids reading rows inserted by in-progress transactions).
//  2. Issues one SELECT per change table for rows in the window
//     (currentCSN, upperCSN].
//  3. Merges and sorts all events by (CSN, IBMSNAP_INTENTSEQ) before delivery.
//  4. Advances its watermark to the highest CSN seen.
//
// When no new rows are found the loop backs off for StreamConfig.BackoffInterval.
// changeTableEntry is a pre-parsed element of changeTables built at Initialize()
// time. Storing schema and table separately avoids strings.SplitN on every poll.
type changeTableEntry struct {
	qualKey         string // "SCHEMA.TABLE" — key used in intentSeqByTable
	schema          string
	table           string
	changeTableName string // qualified CDC change table, e.g. "ASNCDC.CD_ORDERS"
	lastRawCount    int    // raw row count from the last poll; used to pre-size rawEvents next poll
}

// changeTableMeta caches per-table column metadata derived from rows.Columns()
// and rows.ColumnTypes(). Computed once on the first poll and reused on
// subsequent polls — the change table schema is fixed for the connector lifetime.
type changeTableMeta struct {
	columns      []string
	opIdx        int
	csnIdx       int
	intentSeqIdx int
	tsIdx        int
	opcodeIdx    int
	dataColIdxs  []int
	dataColNames []string
	dataColKinds []db2ColumnKind
}

// Streamer polls DB2 CDC change tables and delivers ordered ChangeEvents to a handler.
type Streamer struct {
	db           *sql.DB
	config       StreamConfig
	version      Version
	changeTables map[string]string // monitored table name -> change table qualified name
	// changeTableList is the ordered slice form of changeTables, built once in
	// Initialize() so pollChanges can iterate without SplitN on every poll cycle.
	changeTableList []changeTableEntry

	// pendingBeforeByTable holds the trailing opTypeUpdateBefore event (opcode 3)
	// when a PollBatchSize boundary falls between the D and I rows of an update
	// pair. The matching opTypeUpdateAfter row arrives next poll and is inserted
	// before it by pollChangeTable before pairOpcodeEvents merges the pair.
	// Protected by pendingMu. Reset in Initialize() so stale D-rows from a prior
	// connection cannot be matched against I-rows from the new connection.
	pendingMu            sync.Mutex
	pendingBeforeByTable map[string]*ChangeEvent

	// activeWindow is the current open-window deduplication window set by the
	// incremental snapshot engine. Protected by windowMu.
	windowMu     sync.Mutex
	activeWindow *DeduplicationWindow

	// currentCSN tracks the latest CSN the Streamer has processed.
	// Read by IncrementalSnapshotEngine.CurrentCSN() (snapshot goroutine) and
	// written by Stream() (CDC goroutine). Using atomic.Pointer eliminates the
	// RWMutex lock/unlock on the hot poll path; each write heap-allocates a CSN
	// but that is one allocation per successful poll cycle (~10/s at default config).
	currentCSN atomic.Pointer[CSN]

	// columnMeta caches per-change-table column metadata after the first poll.
	// Keys are changeTableName (qualified, e.g. "ASNCDC.CD_ORDERS"). Written
	// once per table on first poll; read-only after that. Protected by metaMu.
	metaMu     sync.RWMutex
	columnMeta map[string]*changeTableMeta

	// commitSeqByteLen is the detected IBMSNAP_COMMITSEQ byte length (10 or 16).
	// Written by Initialize() and read by CommitSeqByteLen() on separate goroutines;
	// atomic to avoid a data race between the streaming goroutine (writer) and the
	// schema-change poller goroutine (reader).
	commitSeqByteLen atomic.Int32

	// schemasIN is the pre-computed SQL IN clause fragment for config.Schemas,
	// e.g. "'S1', 'S2'". It is computed once in NewStreamer and reused on every
	// getUpperBound call to avoid repeated slice allocation + strings.Join.
	schemasIN string

	// upperBoundQuery is the pre-computed SQL for getUpperBound, built once in
	// Initialize() to avoid fmt.Sprintf on every poll cycle.
	upperBoundQuery string

	// pollQueryParts maps each changeTableName to a pre-split query template.
	// During Initialize the query body is split at its two %s placeholders into
	// three static parts; buildPollQuery concatenates them with afterPredicate and
	// upperHex without reflection (no fmt.Sprintf). Nil until Initialize() completes
	// (tests and direct-use paths fall back to the ad-hoc construction in buildPollQuery).
	pollQueryParts map[string][3]string

	// log is used to emit diagnostic warnings (e.g. unmatched D+I pairs).
	// A noopLogger is used when nil to keep callers unconditional.
	log Logger

	// lastPollEventCount is the observed total ChangeEvent count from the most recent
	// successful poll. Used to right-size the allEvents slice on the next poll,
	// avoiding repeated slice doublings on hot workloads while keeping idle polls cheap.
	// Accessed concurrently by Stream() and any concurrent pollChanges callers; use
	// atomic operations to avoid a data race under the -race detector.
	lastPollEventCount atomic.Int64
}

// NewStreamer creates a new Streamer.
// Pass functional options (e.g. WithStreamerLogger) to configure optional behaviour.
func NewStreamer(db *sql.DB, config StreamConfig, version Version, opts ...func(*Streamer)) *Streamer {
	s := &Streamer{
		db:                   db,
		config:               config,
		version:              version,
		changeTables:         make(map[string]string),
		pendingBeforeByTable: make(map[string]*ChangeEvent),
		columnMeta:           make(map[string]*changeTableMeta),
		schemasIN:            config.schemasINClause(),
		log:                  noopLogger{},
	}
	// currentCSN is an atomic.Pointer; must be initialized via Store after struct
	// creation because the struct literal cannot directly initialize atomic types.
	startCSN := config.StartingCSN
	s.currentCSN.Store(&startCSN)
	for _, o := range opts {
		o(s)
	}
	return s
}

// WithStreamerLogger sets the logger used by the Streamer for diagnostic warnings.
func WithStreamerLogger(l Logger) func(*Streamer) {
	return func(s *Streamer) { s.log = l }
}

// SetWindow installs a DeduplicationWindow on the Streamer.
// CDC events within the window's CSN bracket will evict matching snapshot rows.
// When the stream passes window.HighCSN, the surviving rows are prepended to
// the batch as OpTypeRead events and the window is cleared.
func (s *Streamer) SetWindow(w *DeduplicationWindow) {
	// Do NOT reset pendingBeforeByTable here. A D+I UPDATE pair can be split
	// across the poll boundary that installs the window; discarding the pending
	// D-row would cause the matching I-row to be re-classified as an INSERT,
	// losing the before-image. The pending map is only cleared at Initialize()
	// time (reconnect / startup), which is safe because those D-rows belong to
	// a prior connection whose matching I-rows will never arrive on this stream.
	s.windowMu.Lock()
	s.activeWindow = w
	s.windowMu.Unlock()
}

// ClearWindow removes w from the Streamer's activeWindow if it is still
// current.  Call this when the incremental snapshot engine cancels a window
// before it reaches HighCSN (e.g. context deadline) to prevent the stale
// window from evicting rows in a future snapshot round.
func (s *Streamer) ClearWindow(w *DeduplicationWindow) {
	s.windowMu.Lock()
	if s.activeWindow == w {
		s.activeWindow = nil
	}
	s.windowMu.Unlock()
}

// hasPending reports whether there is a pending before-image for tableName.
// pendingBeforeByTable is only written by pollChangeTable on the single Poll
// goroutine, so the lock is not strictly required for this read. It is kept
// for clarity and to guard against future callers from other goroutines.
func (s *Streamer) hasPending(tableName string) bool {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return s.pendingBeforeByTable[tableName] != nil
}

// CurrentCSN returns the latest committed CSN the Streamer has processed.
func (s *Streamer) CurrentCSN() CSN {
	if ptr := s.currentCSN.Load(); ptr != nil {
		return *ptr
	}
	return NullCSN()
}

// CommitSeqByteLen returns the detected byte length of IBMSNAP_COMMITSEQ (10 or 16).
// The value is set by Initialize(); callers outside the Streamer should read it here
// rather than from the StreamConfig copy they passed at construction time.
// Uses an atomic load to be safe for concurrent reads from the schema-change poller goroutine.
func (s *Streamer) CommitSeqByteLen() int {
	if n := s.commitSeqByteLen.Load(); n > 0 {
		return int(n)
	}
	// Initialize() has not yet run (pre-connect or test path); return the default.
	return 10
}

// InvalidateColumnMetaForSource drops the cached changeTableMeta for the change table
// corresponding to the given source owner and table. The next poll re-derives the meta
// from the live column descriptor, picking up any schema changes (e.g., ADDCOLUMN).
// Safe to call concurrently with Stream().
func (s *Streamer) InvalidateColumnMetaForSource(srcOwner, srcTable string) {
	key := strings.ToUpper(srcOwner) + "." + strings.ToUpper(srcTable)
	// changeTables is written only during Initialize() and never modified after.
	// The Streamer is published to callers only after Initialize() returns (via
	// runStreaming's d.streamer assignment under d.mu), so any concurrent caller
	// of InvalidateColumnMetaForSource already observes the fully-initialized map.
	changeTable, ok := s.changeTables[key]
	if !ok {
		return
	}
	s.metaMu.Lock()
	delete(s.columnMeta, changeTable)
	s.metaMu.Unlock()
}

// isValidDB2IdentifierInternal reports whether s is safe to embed in SQL strings:
// non-empty, first char letter or underscore, remaining chars alphanumeric or underscore.
func isValidDB2IdentifierInternal(s string) bool {
	return db2IdentifierRE.MatchString(s)
}

// Initialize discovers the change tables for all monitored tables from IBMSNAP_REGISTER.
// When config.Tables is empty, all registered tables for the schema are discovered dynamically.
// config.TableFilter (if set) narrows the discovered or configured set.
func (s *Streamer) Initialize(ctx context.Context) error {
	for _, schema := range s.config.Schemas {
		if !isValidDB2IdentifierInternal(schema) {
			return fmt.Errorf("invalid schema %q: must be non-empty uppercase alphanumeric+underscore", schema)
		}
	}

	// Reset on reconnect: if Initialize is called again (e.g. after a transient
	// error), stale entries from deregistered tables must not persist. A stale
	// pending D-row would produce a spurious warning on the next poll when its
	// expected I-row never arrives (different connection = different CSN space).
	s.changeTables = make(map[string]string)
	s.changeTableList = nil
	s.pendingMu.Lock()
	s.pendingBeforeByTable = make(map[string]*ChangeEvent)
	s.pendingMu.Unlock()
	s.metaMu.Lock()
	s.columnMeta = make(map[string]*changeTableMeta)
	s.metaMu.Unlock()

	cdcSchema := s.config.asncdcSchema()

	query := fmt.Sprintf(`
		SELECT SOURCE_OWNER, SOURCE_TABLE, CD_OWNER, CD_TABLE
		FROM %s.IBMSNAP_REGISTER
		WHERE SOURCE_OWNER IN (%s)
	`, cdcSchema, s.schemasIN)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying CDC registration: %w", err)
	}
	defer rows.Close()

	dynamicDiscovery := len(s.config.Tables) == 0

	registered := make(map[string]bool)

	for rows.Next() {
		var sourceOwner, sourceTable, cdOwner, cdTable string
		if err := rows.Scan(&sourceOwner, &sourceTable, &cdOwner, &cdTable); err != nil {
			return fmt.Errorf("scanning registration row: %w", err)
		}

		sourceOwner = strings.TrimSpace(sourceOwner)
		sourceTable = strings.TrimSpace(sourceTable)
		cdOwner = strings.TrimSpace(cdOwner)
		cdTable = strings.TrimSpace(cdTable)
		// Validate catalog-sourced identifiers before embedding in SQL as table names.
		// Non-conformant values (spaces, quotes, semicolons) would allow SQL injection
		// via a rogue IBMSNAP_REGISTER row; skip them rather than produce broken queries.
		if !isValidDB2IdentifierInternal(cdOwner) || !isValidDB2IdentifierInternal(cdTable) {
			continue
		}
		changeKey := strings.ToUpper(sourceOwner) + "." + strings.ToUpper(sourceTable)
		changeTableName := cdOwner + "." + cdTable

		if dynamicDiscovery {
			// Accept all registered tables, apply filter below.
			if s.config.TableFilter == nil || s.config.TableFilter(sourceTable) {
				s.changeTables[changeKey] = changeTableName
				// Force a new backing array to avoid aliasing the caller's slice
				// (on reconnect, the same StreamConfig is reused with spare capacity).
				s.config.Tables = append(s.config.Tables[:len(s.config.Tables):len(s.config.Tables)], sourceTable)
				registered[sourceTable] = true
			}
		} else {
			for _, monitoredTable := range s.config.Tables {
				if strings.EqualFold(sourceTable, monitoredTable) {
					if s.config.TableFilter == nil || s.config.TableFilter(monitoredTable) {
						s.changeTables[changeKey] = changeTableName
					}
					registered[monitoredTable] = true
					break
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating registration rows: %w", err)
	}

	if !dynamicDiscovery {
		for _, table := range s.config.Tables {
			if !registered[table] {
				return fmt.Errorf("table %s is not registered for CDC (run ASNCDC.ADDTABLE)", table)
			}
		}
	}

	if len(s.changeTables) == 0 {
		return fmt.Errorf("no CDC-registered tables found for schemas %s (check ASNCDC.IBMSNAP_REGISTER)", s.schemasIN)
	}

	// Pre-parse changeTables into changeTableList so pollChanges can iterate
	// without calling strings.SplitN on every poll cycle, and so that
	// detectCommitSeqByteLen (below) can iterate it in deterministic order.
	s.changeTableList = make([]changeTableEntry, 0, len(s.changeTables))
	for qualKey, cdTableName := range s.changeTables {
		parts := strings.SplitN(qualKey, ".", 2)
		if len(parts) != 2 {
			continue
		}
		s.changeTableList = append(s.changeTableList, changeTableEntry{
			qualKey:         qualKey,
			schema:          parts[0],
			table:           parts[1],
			changeTableName: cdTableName,
		})
	}

	// Auto-detect IBMSNAP_COMMITSEQ byte length from the registered CD tables.
	// Must run after changeTableList is built (detectCommitSeqByteLen iterates it).
	if s.config.CommitSeqByteLen <= 0 {
		if byteLen, err := s.detectCommitSeqByteLen(ctx); err == nil {
			s.config.CommitSeqByteLen = byteLen
		} else {
			s.config.CommitSeqByteLen = 10 // safe default for older DB2
		}
	}
	// Mirror into the atomic so CommitSeqByteLen() and buildPollQuery are
	// race-free when read by the schema-change poller goroutine concurrently.
	s.commitSeqByteLen.Store(int32(s.config.CommitSeqByteLen))

	// Pre-compute the getUpperBound query string. It is structurally invariant —
	// cdcSchema and schemasIN never change after Initialize — so rebuilding it with
	// fmt.Sprintf on every poll cycle is unnecessary allocation.
	s.upperBoundQuery = fmt.Sprintf(`
		SELECT MAX(t.SYNCHPOINT) FROM (
			SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER IN (%s)
			UNION ALL
			SELECT SYNCHPOINT
			  FROM %s.IBMSNAP_REGISTER
			 WHERE SOURCE_OWNER IN (%s)
		) t`,
		cdcSchema, s.schemasIN,
		cdcSchema, s.schemasIN,
	)

	// Pre-compute per-table poll query templates. changeTableName and PollBatchSize
	// are baked in once here; only afterPredicate (%[1]s) and upperHex (%[2]s) are
	// substituted at query time, eliminating the large fmt.Sprintf on every poll.
	const pollQueryBody = `
		SELECT CASE
		  WHEN cdc.IBMSNAP_OPERATION = 'D'
		       AND LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'I'
		       THEN 3
		  WHEN cdc.IBMSNAP_OPERATION = 'I'
		       AND LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'D'
		       THEN 4
		  WHEN cdc.IBMSNAP_OPERATION = 'D' THEN 1
		  WHEN cdc.IBMSNAP_OPERATION = 'I' THEN 2
		  ELSE 0
		END AS IBMSNAP_OPCODE,
		cdc.*
		FROM %s cdc
		WHERE %%s
		  AND cdc.IBMSNAP_COMMITSEQ <= X'%%s'
		ORDER BY cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ
		FETCH FIRST %d ROWS ONLY
	`
	s.pollQueryParts = make(map[string][3]string, len(s.changeTableList))
	for _, entry := range s.changeTableList {
		tmpl := fmt.Sprintf(pollQueryBody, entry.changeTableName, s.config.PollBatchSize)
		// Split at the two %s placeholders so buildPollQuery can substitute
		// afterPredicate and upperHex via plain concatenation (no fmt.Sprintf).
		parts := strings.SplitN(tmpl, "%s", 3)
		if len(parts) == 3 {
			s.pollQueryParts[entry.changeTableName] = [3]string{parts[0], parts[1], parts[2]}
		}
	}

	return nil
}

// detectCommitSeqByteLen queries SYSCAT.COLUMNS to determine the actual byte
// length of IBMSNAP_COMMITSEQ in the registered CD tables.
// DB2 ≤ 11.x uses CHAR(10) FOR BIT DATA; DB2 12.1+ uses CHAR(16) FOR BIT DATA.
func (s *Streamer) detectCommitSeqByteLen(ctx context.Context) (int, error) {
	// Iterate changeTableList (deterministic order) rather than the changeTables
	// map to avoid non-deterministic query ordering in tests and production.
	for _, entry := range s.changeTableList {
		parts := strings.SplitN(entry.changeTableName, ".", 2)
		if len(parts) != 2 {
			continue
		}
		var length int
		err := s.db.QueryRowContext(ctx,
			"SELECT LENGTH FROM SYSCAT.COLUMNS WHERE TABSCHEMA=? AND TABNAME=? AND COLNAME='IBMSNAP_COMMITSEQ'",
			strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]),
		).Scan(&length)
		// DB2 only defines CHAR(10) for ≤11.x and CHAR(16) for 12.1+.
		// Accept only these two values to reject malformed or malicious catalog rows
		// that could cause oversized SQL hex literals or excessive buffer allocations.
		if err == nil && (length == 10 || length == 16) {
			return length, nil
		}
	}
	return 0, errors.New("could not detect IBMSNAP_COMMITSEQ byte length from registered CD tables")
}

// Stream continuously polls change tables and sends events to handler until ctx is cancelled.
func (s *Streamer) Stream(ctx context.Context, handler func(event ChangeEvent) error) error {
	currentCSN := s.config.StartingCSN
	// intentSeqByTable tracks the last IBMSNAP_INTENTSEQ seen per table at currentCSN,
	// enabling composite (CSN, IntentSeq) pagination when a batch boundary lands
	// mid-transaction at the same CSN across multiple tables.
	intentSeqByTable := make(map[string]int64)

	// Reuse a single backoff timer rather than allocating a new time.Timer per idle cycle.
	backoffTimer := time.NewTimer(s.config.BackoffInterval)
	defer backoffTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, maxCSN, newIntentSeqs, err := s.pollChanges(ctx, currentCSN, intentSeqByTable)
		if err != nil {
			return fmt.Errorf("polling changes: %w", err)
		}

		for _, event := range events {
			if err := handler(event); err != nil {
				return fmt.Errorf("handler error: %w", err)
			}
		}

		if len(events) > 0 {
			currentCSN = maxCSN
			csnCopy := maxCSN
			s.currentCSN.Store(&csnCopy)
			intentSeqByTable = newIntentSeqs
		} else {
			// Drain the channel before Reset: if the timer fired between the last
			// select exit and now, Reset would not reset the fire-state and the next
			// select would return immediately, producing a zero-sleep spin cycle.
			if !backoffTimer.Stop() {
				select {
				case <-backoffTimer.C:
				default:
				}
			}
			backoffTimer.Reset(s.config.BackoffInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-backoffTimer.C:
			}
		}
	}
}

// tableResult holds one table's poll output.
type tableResult struct {
	tableName string
	events    []ChangeEvent
	// full is true when the raw SQL row count equalled PollBatchSize (before
	// pairOpcodeEvents merging) OR when the table has a pending before-image
	// that must be matched next poll. Either condition means the watermark must
	// not advance past this table's last event CSN.
	full bool
	// heldCSN is non-nil when pollChangeTable stored a new pending D-row
	// (opTypeUpdateBefore) at the end of this poll cycle. The matching I-row
	// arrives on the next poll; anyFullAtReturnCSN composite pagination in
	// pollChanges ensures the I-row remains reachable on the next poll via
	// "(COMMITSEQ = heldCSN AND INTENTSEQ > maxSeenIntentSeq)".
	heldCSN *CSN
}

// computeSafeCSN returns the highest CSN that is safe to advance to across all
// tables in a single poll round.
//
// For each full table (raw row count == PollBatchSize, or pending before-image
// held), the batch may have been truncated mid-transaction. The safe per-table
// ceiling is the penultimate distinct CSN within that table's own batch; events
// at the max CSN must be re-fetched. Edge case: all rows in the batch share one
// CSN — advance to tableMax anyway (at-least-once; caller deduplicates on
// csn+intentSeq).
//
// Non-full tables impose no constraint: all rows up to upperCSN were returned.
//
// The global safe ceiling is the minimum of all per-table ceilings, starting
// from upperCSN (no full table → no narrowing).
//
// afterCSN is the watermark of the last committed poll; it is used as a
// no-advance floor when a table holds a pending D-row but returned no other
// events — the matching I-row has not arrived yet and must not be skipped.
func computeSafeCSN(results []tableResult, upperCSN, afterCSN CSN) CSN {
	safeCSN := upperCSN
	for _, r := range results {
		if !r.full {
			continue // not full — no constraint from this table
		}
		if len(r.events) == 0 {
			if r.heldCSN != nil {
				// A pending D-row was created this poll with no other events in
				// this batch. The matching I-row arrives next poll; must not
				// advance past afterCSN so the I-row's CSN (== heldCSN) remains
				// within the next poll window's [afterCSN+1, upperCSN] range.
				if afterCSN.Less(safeCSN) {
					safeCSN = afterCSN
				}
			}
			continue
		}
		tableMax := r.events[len(r.events)-1].CSN
		// Default: edge case where all rows share one CSN — advance to tableMax.
		tableSafe := tableMax
		for i := len(r.events) - 2; i >= 0; i-- {
			if !r.events[i].CSN.Equal(tableMax) {
				tableSafe = r.events[i].CSN // found penultimate distinct CSN
				break
			}
		}
		if tableSafe.Less(safeCSN) {
			safeCSN = tableSafe
		}
		// NOTE: no extra cap for r.heldCSN here.
		//
		// When all events in a full batch share one CSN (tableSafe == heldCSN ==
		// tableMax), the old cap "safeCSN = afterCSN" trimmed every event and caused
		// an infinite loop. The I-row reachability concern (next poll "COMMITSEQ >
		// tableSafe" would skip the I-row that shares heldCSN) is already addressed by
		// anyFullAtReturnCSN composite pagination: pollChanges sets newIntentSeqs[table]
		// = max(INTENTSEQ in this batch) so the next poll uses:
		//   "(COMMITSEQ > safeCSN) OR (COMMITSEQ = safeCSN AND INTENTSEQ > maxSeen)"
		// which reaches the I-row at the same CSN with a higher INTENTSEQ.
		//
		// When events have mixed CSNs (tableSafe < heldCSN = tableMax), the condition
		// "!tableSafe.Less(heldCSN)" was already false, so the old block never fired
		// in that case — removing it has no effect on mixed-CSN batches.
	}
	return safeCSN
}

// computeReturnCSN caps safeCSN at the actual max event CSN so that sparse
// event windows do not advance the watermark past the last observed event.
// When no table is full, safeCSN equals upperCSN which may be much higher than
// any event in the batch; returning upperCSN would silently skip future events
// that land before the next poll window opens.
func computeReturnCSN(safeCSN CSN, sorted []ChangeEvent) CSN {
	if len(sorted) == 0 {
		return safeCSN
	}
	maxEventCSN := sorted[len(sorted)-1].CSN
	if maxEventCSN.Less(safeCSN) {
		return maxEventCSN
	}
	return safeCSN
}

// trimToSafeCSN removes events with CSN strictly above safeCSN (right-trims the
// sorted slice). If every event exceeds safeCSN — the heldCSN edge case where
// safeCSN equals afterCSN and no safe event exists — the slice is empty so the
// watermark does not advance and the next poll re-fetches the same window.
func trimToSafeCSN(sorted []ChangeEvent, safeCSN CSN) []ChangeEvent {
	if len(sorted) == 0 || !sorted[len(sorted)-1].CSN.Greater(safeCSN) {
		return sorted // fast path: all events within safe window (common case)
	}
	// Binary search for the first event at or after safeCSN: O(log n) vs O(n).
	// Use a proper 3-way comparator so BinarySearchFunc lands at the first event
	// with CSN == safeCSN; then advance past equal events to include them.
	cut, _ := slices.BinarySearchFunc(sorted, safeCSN, func(e ChangeEvent, s CSN) int {
		return e.CSN.Compare(s)
	})
	for cut < len(sorted) && sorted[cut].CSN.Equal(safeCSN) {
		cut++
	}
	return sorted[:cut]
}

// pollChanges polls all change tables and merges events ordered by CSN.
//
// Watermark safety: IBMSNAP_COMMITSEQ identifies a DB2 transaction; all rows
// from that transaction share the same CSN.  If any change table returned
// exactly PollBatchSize rows the batch may have been truncated in the middle
// of a transaction, so we must NOT advance currentCSN past that last CSN — the
// remaining rows would be permanently skipped by the `> afterCSN` predicate.
//
// We compute the safe ceiling per-table (not globally) and take the minimum.
// Using the global penultimate would cause events from a full table at a lower
// CSN to be permanently skipped when another table has events at a higher CSN.
//
// intentSeqByTable holds the per-table last IBMSNAP_INTENTSEQ seen at afterCSN.
// When any table has a non-zero entry, composite (CSN, IntentSeq) pagination is
// used for that table so the batch does not re-deliver already-seen rows.
// A global afterIntentSeq would incorrectly skip table B's events when table A
// defines the high-water mark for a shared CSN.
func (s *Streamer) pollChanges(ctx context.Context, afterCSN CSN, intentSeqByTable map[string]int64) ([]ChangeEvent, CSN, map[string]int64, error) {
	upperCSN, err := s.getUpperBound(ctx)
	if err != nil {
		return nil, CSN{}, nil, fmt.Errorf("getting upper bound: %w", err)
	}
	// Skip the poll when the capture daemon hasn't advanced past our watermark,
	// but only when no table has mid-CSN pending rows (intentSeq > 0).
	// Any table with intentSeq > 0 means we are mid-transaction and must continue.
	anyPending := false
	for _, seq := range intentSeqByTable {
		if seq > 0 {
			anyPending = true
			break
		}
	}
	if !upperCSN.Greater(afterCSN) && !anyPending {
		// If there is an active snapshot window whose HighCSN is null (the table
		// was just registered and asncap hasn't captured any transactions yet),
		// flush the window immediately so the snapshot rows are returned rather
		// than blocking forever waiting for a CDC event to advance past HighCSN.
		s.windowMu.Lock()
		w := s.activeWindow
		s.windowMu.Unlock()
		if w != nil && (w.HighCSN.IsNull() || !upperCSN.Less(w.HighCSN)) {
			flushed := w.Flush()
			// Only clear activeWindow if it is still the same window we just flushed.
			// SetWindow may have installed a new window between our Flush call and
			// this lock acquisition; clearing it unconditionally would lose that window.
			s.windowMu.Lock()
			if s.activeWindow == w {
				s.activeWindow = nil
			}
			s.windowMu.Unlock()
			if len(flushed) > 0 {
				return flushed, afterCSN, intentSeqByTable, nil
			}
		}
		return nil, afterCSN, intentSeqByTable, nil
	}

	// changeTableList is normally pre-built by Initialize(). Fall back to
	// rebuilding it inline for callers (e.g. unit tests) that populate
	// changeTables directly without calling Initialize().
	if len(s.changeTableList) == 0 && len(s.changeTables) > 0 {
		s.changeTableList = make([]changeTableEntry, 0, len(s.changeTables))
		for qualKey, cdTableName := range s.changeTables {
			parts := strings.SplitN(qualKey, ".", 2)
			if len(parts) != 2 {
				continue
			}
			cdParts := strings.SplitN(cdTableName, ".", 2)
			if len(cdParts) != 2 || !isValidDB2IdentifierInternal(cdParts[0]) || !isValidDB2IdentifierInternal(cdParts[1]) {
				s.log.Warnf("changeTableList fallback: skipping invalid CD table name %q", cdTableName)
				continue
			}
			s.changeTableList = append(s.changeTableList, changeTableEntry{
				qualKey: qualKey, schema: parts[0], table: parts[1], changeTableName: cdTableName,
			})
		}
	}
	n := len(s.changeTableList)
	results := make([]tableResult, 0, n)
	// Size allEvents based on last observed total to avoid repeated doublings on
	// hot workloads while keeping idle polls cheap (default n*16 cap).
	allocCap := int(s.lastPollEventCount.Load())
	if minCap := n * 16; allocCap < minCap {
		allocCap = minCap
	}
	allEvents := make([]ChangeEvent, 0, allocCap)

	for i := range s.changeTableList {
		entry := &s.changeTableList[i]
		tableIntentSeq := intentSeqByTable[entry.qualKey]
		// Capture hasPending BEFORE pollChangeTable; the call may consume the old
		// pending entry and set a NEW pending D-row at the batch tail.
		wasPending := s.hasPending(entry.qualKey)
		events, rawCount, err := s.pollChangeTable(ctx, entry.qualKey, entry.schema, entry.table, entry.changeTableName, afterCSN, tableIntentSeq, upperCSN, entry.lastRawCount)
		entry.lastRawCount = rawCount
		if err != nil {
			return nil, CSN{}, nil, fmt.Errorf("polling change table %s: %w", entry.changeTableName, err)
		}
		// Snapshot any NEW pending D-row written by pollChangeTable this cycle.
		// Its CSN is the lower bound that safeCSN must not advance to or past.
		var heldCSN *CSN
		s.pendingMu.Lock()
		if ev := s.pendingBeforeByTable[entry.qualKey]; ev != nil {
			c := ev.CSN
			heldCSN = &c
		}
		s.pendingMu.Unlock()
		// full=true when: raw row count hit the batch limit (batch was truncated),
		// OR a pending before-image was held before this poll (matching I row must
		// arrive next poll), OR a new pending D-row was just stored this poll
		// (matching I row must arrive next poll — watermark must not advance to heldCSN).
		full := rawCount == s.config.PollBatchSize || wasPending || heldCSN != nil
		results = append(results, tableResult{tableName: entry.qualKey, events: events, full: full, heldCSN: heldCSN})
		allEvents = append(allEvents, events...)
	}

	sorted := s.sortEventsByCSN(allEvents)
	if len(sorted) == 0 {
		// Even with no CDC events this poll, the capture daemon may have advanced
		// past the active window's HighCSN without producing events for the tracked
		// tables (e.g. other-table transactions advanced the watermark). Flush the
		// window so the snapshot rows are not blocked indefinitely.
		s.windowMu.Lock()
		w := s.activeWindow
		s.windowMu.Unlock()
		if w != nil && (w.HighCSN.IsNull() || upperCSN.Greater(w.HighCSN) || upperCSN.Equal(w.HighCSN)) {
			flushed := w.Flush()
			// Only clear activeWindow if it is still the same window we flushed.
			s.windowMu.Lock()
			if s.activeWindow == w {
				s.activeWindow = nil
			}
			s.windowMu.Unlock()
			if len(flushed) > 0 {
				return flushed, afterCSN, intentSeqByTable, nil
			}
		}
		return nil, afterCSN, intentSeqByTable, nil
	}

	safeCSN := computeSafeCSN(results, upperCSN, afterCSN)
	sorted = trimToSafeCSN(sorted, safeCSN)

	if len(sorted) == 0 {
		return nil, afterCSN, intentSeqByTable, nil
	}

	returnCSN := computeReturnCSN(safeCSN, sorted)

	// Deduplication window: evict rows whose PK was modified by in-window CDC events,
	// then flush surviving snapshot rows when the batch passes the window's HighCSN.
	s.windowMu.Lock()
	w := s.activeWindow
	s.windowMu.Unlock()
	if w != nil {
		for i := range sorted {
			if !sorted[i].CSN.IsNull() {
				w.Evict(sorted[i].Schema, sorted[i].Table, sorted[i].Data, sorted[i].CSN)
				// For UPDATE events also evict using the before-image so that a
				// PK-modifying UPDATE evicts the old PK from the snapshot window.
				if sorted[i].Operation == OpTypeUpdate && len(sorted[i].BeforeData) > 0 {
					w.Evict(sorted[i].Schema, sorted[i].Table, sorted[i].BeforeData, sorted[i].CSN)
				}
			}
		}
		// If the last event in the batch has reached or passed HighCSN, flush the
		// surviving snapshot rows and prepend them before the CDC events.
		// Use GreaterOrEqual so a batch whose last event lands exactly on HighCSN
		// triggers the flush immediately rather than waiting for the next poll.
		if sorted[len(sorted)-1].CSN.Greater(w.HighCSN) || sorted[len(sorted)-1].CSN.Equal(w.HighCSN) {
			flushed := w.Flush()
			if len(flushed) > 0 {
				sorted = append(flushed, sorted...)
			}
			// Only clear activeWindow if it is still the same window we flushed.
			s.windowMu.Lock()
			if s.activeWindow == w {
				s.activeWindow = nil
			}
			s.windowMu.Unlock()
		}
	}

	// Build per-table max intentSeq at returnCSN for composite pagination.
	//
	// Normally: if returnCSN advanced past afterCSN, intent seqs reset to nil so
	// the next poll uses the simple "COMMITSEQ > returnCSN" predicate.
	//
	// Exception — large transaction: when a full table's entire batch shares a
	// single CSN (e.g. 5000 UPDATE rows all at CSN=N, PollBatchSize=1000), the
	// loop in computeSafeCSN finds no penultimate CSN so tableSafe = tableMax = N.
	// returnCSN = N > afterCSN, which would reset newIntentSeqs to nil. The next
	// poll would then use "COMMITSEQ > N", permanently skipping the remaining 4000
	// rows at N. To prevent this, we detect when any full table's last event lands
	// at returnCSN and populate newIntentSeqs so the next poll uses composite
	// pagination: "(COMMITSEQ > N OR (COMMITSEQ = N AND INTENTSEQ > maxSeen))".
	anyFullAtReturnCSN := false
	for _, r := range results {
		if r.full && len(r.events) > 0 && r.events[len(r.events)-1].CSN.Equal(returnCSN) {
			anyFullAtReturnCSN = true
			break
		}
	}

	var newIntentSeqs map[string]int64
	if returnCSN.Greater(afterCSN) && !anyFullAtReturnCSN {
		// nil signals "all seqs reset"; avoids one map allocation per poll cycle.
	} else {
		newIntentSeqs = make(map[string]int64, len(intentSeqByTable))
		for i := len(sorted) - 1; i >= 0; i-- {
			ev := sorted[i]
			if !ev.CSN.Equal(returnCSN) {
				break
			}
			if ev.IntentSeq > newIntentSeqs[ev.QualKey] {
				newIntentSeqs[ev.QualKey] = ev.IntentSeq
			}
		}
	}
	// Update adaptive capacity hint for the next poll's allEvents allocation.
	s.lastPollEventCount.Store(int64(len(sorted)))
	return sorted, returnCSN, newIntentSeqs, nil
}

// getUpperBound returns the highest log position the DB2 capture daemon has
// durably written, using MAX over both CD_NEW_SYNCHPOINT (set at ADDTABLE time,
// always non-null) and SYNCHPOINT (updated after each captured transaction).
// This prevents reading change rows that belong to an in-progress transaction.
func (s *Streamer) getUpperBound(ctx context.Context) (CSN, error) {
	// Use the pre-computed query when Initialize has been called; otherwise build
	// it on the fly (test or direct-use path where Initialize was skipped).
	query := s.upperBoundQuery
	if query == "" {
		cdcSchema := s.config.asncdcSchema()
		query = fmt.Sprintf(`
			SELECT MAX(t.SYNCHPOINT) FROM (
				SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT
				  FROM %s.IBMSNAP_REGISTER
				 WHERE SOURCE_OWNER IN (%s)
				UNION ALL
				SELECT SYNCHPOINT
				  FROM %s.IBMSNAP_REGISTER
				 WHERE SOURCE_OWNER IN (%s)
			) t`,
			cdcSchema, s.schemasIN,
			cdcSchema, s.schemasIN,
		)
	}

	var synchpointRaw any
	err := s.db.QueryRowContext(ctx, query).Scan(&synchpointRaw)
	if err != nil {
		return CSN{}, fmt.Errorf("getting SYNCHPOINT: %w", err)
	}

	if synchpointRaw == nil {
		return NullCSN(), nil
	}

	// SYNCHPOINT is CHAR(n) FOR BIT DATA; the DB2 CLI driver may return
	// it as a hex-encoded string — getBitDataBytes hex-decodes it if needed.
	synchpointBytes := getBitDataBytes(&synchpointRaw)
	if len(synchpointBytes) == 0 {
		return NullCSN(), nil
	}

	return NewCSNFromDBValue(synchpointBytes), nil
}

// buildPollQuery returns the SQL used to fetch a batch of change rows.
//
// IBMSNAP_COMMITSEQ is typed CHAR(n) FOR BIT DATA — a raw binary column.
// Reliable parameter binding for binary columns via SQL_C_BINARY would require
// exact n-byte buffers and careful length handling. To avoid this complexity,
// CSN bounds are embedded directly as hex literals sized to match the column
// (X'<2n chars>'). This is safe because CSN values come from our own state,
// never from user input.
//
// When afterIntentSeq > 0 the query uses composite (CSN, IntentSeq) pagination
// so that a PollBatchSize cutoff mid-CSN resumes from where it left off rather
// than re-reading or skipping the tail of the same transaction.
func (s *Streamer) buildPollQuery(changeTableName string, afterCSN CSN, afterIntentSeq int64, upperCSN CSN) string {
	byteLen := int(s.commitSeqByteLen.Load())
	if byteLen <= 0 {
		byteLen = 10 // Initialize() has not yet run; fall back to DB2 ≤11.x default.
	}
	afterHex := afterCSN.SQLHex(byteLen)
	upperHex := upperCSN.SQLHex(byteLen)

	var afterPredicate string
	if afterIntentSeq > 0 {
		// Composite pagination: resume within the same CSN using IntentSeq.
		// IBMSNAP_INTENTSEQ is CHAR(10) FOR BIT DATA — must compare using a
		// binary hex literal X'...' not a decimal integer.
		//
		// Encoding: pack the int64 into bytes [0:8] (big-endian), matching how
		// getInt64 decodes it (reads bytes [0:8] of the 10-byte column value).
		// Pad the trailing bytes [8:9] to 0xFF so that the 10-byte binary
		// comparison correctly skips ALL rows whose leading 8-byte value equals
		// afterIntentSeq (regardless of their trailing 2 bytes). Without 0xFF
		// padding, rows at the same int64 but with trailing bytes > 0x00 would
		// satisfy the predicate, re-delivering already-seen rows.
		var intentBytes [10]byte
		binary.BigEndian.PutUint64(intentBytes[:8], uint64(afterIntentSeq))
		intentBytes[8] = 0xFF
		intentBytes[9] = 0xFF
		// Encode directly to uppercase hex, avoiding the two-alloc
		// strings.ToUpper(hex.EncodeToString(...)) pattern.
		const upperHex = "0123456789ABCDEF"
		var intentHexBuf [20]byte
		for i, b := range intentBytes {
			intentHexBuf[2*i] = upperHex[b>>4]
			intentHexBuf[2*i+1] = upperHex[b&0x0F]
		}
		intentHex := string(intentHexBuf[:])
		// Assemble without fmt to avoid reflection overhead; all parts are hex-safe strings.
		afterPredicate = "(cdc.IBMSNAP_COMMITSEQ > X'" + afterHex +
			"' OR (cdc.IBMSNAP_COMMITSEQ = X'" + afterHex +
			"' AND cdc.IBMSNAP_INTENTSEQ > X'" + intentHex + "'))"
	} else {
		afterPredicate = "cdc.IBMSNAP_COMMITSEQ > X'" + afterHex + "'"
	}

	// Use the pre-split per-table parts when available (normal production path after
	// Initialize()). Concatenate three static parts with afterPredicate and upperHex
	// directly — no fmt.Sprintf reflection on the hot poll path.
	if parts, ok := s.pollQueryParts[changeTableName]; ok {
		return parts[0] + afterPredicate + parts[1] + upperHex + parts[2]
	}

	// Fallback: build the query ad-hoc for tests and direct-use paths where
	// Initialize() was not called. LEAD/LAG window functions classify each D/I row:
	//   OPCODE 1 = standalone DELETE   OPCODE 2 = standalone INSERT
	//   OPCODE 3 = before-image of UPDATE (D row immediately before I)
	//   OPCODE 4 = after-image of UPDATE  (I row immediately after D)
	// Ported from Debezium LuwPlatform.java CHANGE_TABLE_DATA_COLUMNS_QUERY.
	return fmt.Sprintf(`
		SELECT CASE
		  WHEN cdc.IBMSNAP_OPERATION = 'D'
		       AND LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'I'
		       THEN 3
		  WHEN cdc.IBMSNAP_OPERATION = 'I'
		       AND LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ) = 'D'
		       THEN 4
		  WHEN cdc.IBMSNAP_OPERATION = 'D' THEN 1
		  WHEN cdc.IBMSNAP_OPERATION = 'I' THEN 2
		  ELSE 0
		END AS IBMSNAP_OPCODE,
		cdc.*
		FROM %s cdc
		WHERE %s
		  AND cdc.IBMSNAP_COMMITSEQ <= X'%s'
		ORDER BY cdc.IBMSNAP_COMMITSEQ, cdc.IBMSNAP_INTENTSEQ
		FETCH FIRST %d ROWS ONLY
	`, changeTableName, afterPredicate, upperHex, s.config.PollBatchSize)
}

// pollChangeTable queries a single change table for events in the CSN window (afterCSN, upperCSN].
//
// Returns the merged events, the raw SQL row count (before pairOpcodeEvents merging), and any error.
// The raw row count is used by the caller to determine whether the batch was full (truncated).
//
// Pending before-image handling: when the LEAD/LAG query places a D+I pair at the PollBatchSize
// boundary, the opTypeUpdateBefore (D) row is saved in s.pendingBeforeByTable[tableName] instead
// of being emitted as a phantom delete. On the next poll, it is prepended to the raw events before
// pairOpcodeEvents processes the complete pair.
func (s *Streamer) pollChangeTable(ctx context.Context, tableKey, schema, tableName, changeTableName string, afterCSN CSN, afterIntentSeq int64, upperCSN CSN, rawCountHint int) ([]ChangeEvent, int, error) {
	query := s.buildPollQuery(changeTableName, afterCSN, afterIntentSeq, upperCSN)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("querying change table %s: %w", changeTableName, err)
	}
	defer rows.Close()

	// Look up cached column metadata; derive and cache on first poll or after
	// an explicit InvalidateColumnMetaForSource call (e.g., on OpTypeSchemaChange).
	s.metaMu.RLock()
	meta := s.columnMeta[changeTableName]
	s.metaMu.RUnlock()

	if meta == nil {
		columns, err := rows.Columns()
		if err != nil {
			return nil, 0, fmt.Errorf("getting columns for %s: %w", changeTableName, err)
		}

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			return nil, 0, fmt.Errorf("getting column types for %s: %w", changeTableName, err)
		}

		m := &changeTableMeta{
			columns:      columns,
			opIdx:        -1,
			csnIdx:       -1,
			intentSeqIdx: -1,
			tsIdx:        -1,
			opcodeIdx:    -1,
		}
		for i, col := range columns {
			switch col {
			case "IBMSNAP_OPERATION":
				m.opIdx = i
			case "IBMSNAP_COMMITSEQ":
				m.csnIdx = i
			case "IBMSNAP_INTENTSEQ":
				m.intentSeqIdx = i
			case "IBMSNAP_LOGMARKER":
				m.tsIdx = i
			case "IBMSNAP_OPCODE":
				m.opcodeIdx = i
			default:
				if !strings.HasPrefix(col, "IBMSNAP_") {
					m.dataColIdxs = append(m.dataColIdxs, i)
					m.dataColNames = append(m.dataColNames, col)
				}
			}
		}
		if m.opIdx < 0 || m.csnIdx < 0 || m.intentSeqIdx < 0 {
			// Sanitize column names before embedding in error string: DB2 catalog
			// names are normally pure ASCII identifiers, but quoted names can contain
			// arbitrary bytes. Replace any non-printable-ASCII byte with '?'.
			safe := make([]string, len(columns))
			for i, c := range columns {
				safe[i] = strings.Map(func(r rune) rune {
					if r >= 0x20 && r < 0x7F {
						return r
					}
					return '?'
				}, c)
			}
			return nil, 0, fmt.Errorf("change table %s is missing required IBMSNAP_ columns (found: %v)", changeTableName, safe)
		}
		m.dataColKinds = make([]db2ColumnKind, len(m.dataColIdxs))
		for i, idx := range m.dataColIdxs {
			m.dataColKinds[i] = columnKind(columnTypes[idx].DatabaseTypeName())
		}
		s.metaMu.Lock()
		s.columnMeta[changeTableName] = m
		s.metaMu.Unlock()
		meta = m
	}

	opIdx, csnIdx, intentSeqIdx, tsIdx, opcodeIdx := meta.opIdx, meta.csnIdx, meta.intentSeqIdx, meta.tsIdx, meta.opcodeIdx
	dataColIdxs, dataColNames, dataColKinds := meta.dataColIdxs, meta.dataColNames, meta.dataColKinds

	// Use the last observed row count as a capacity hint to avoid repeated slice
	// doublings on active tables. Fall back to 16 for the first poll or idle tables.
	rawEvents := make([]ChangeEvent, 0, max(rawCountHint, 16))
	var rawCount int

	// Reuse scanDest and scanPtrs across rows to avoid one allocation per column per row.
	scanDest := make([]any, len(meta.columns))
	scanPtrs := make([]any, len(meta.columns))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		// Clear previous row values before scanning (avoids stale data on nil columns).
		clear(scanDest)

		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, 0, fmt.Errorf("scanning row from %s: %w", changeTableName, err)
		}

		// IBMSNAP_COMMITSEQ is CHAR(n) FOR BIT DATA; the DB2 CLI driver may
		// return it as a hex-encoded string — getBitDataBytes handles both.
		csnBytes := getBitDataBytes(&scanDest[csnIdx])
		intentSeq := getInt64(&scanDest[intentSeqIdx])

		var timestamp time.Time
		if tsIdx >= 0 {
			timestamp = getTime(&scanDest[tsIdx])
		}

		csn := NewCSNFromDBValue(csnBytes)

		var opType OpType
		var opErr error
		if opcodeIdx >= 0 {
			code := getInt64(&scanDest[opcodeIdx])
			opType, opErr = fromOpcodeInt(code)
		} else {
			operation := getString(&scanDest[opIdx])
			opType, opErr = FromDB2Op(operation)
		}
		// Count every row the DB returned so the fullness signal accurately
		// reflects whether the batch was truncated by FETCH FIRST.  Skipping
		// unknown opcodes after this point is safe; they produce no events.
		rawCount++
		if opErr != nil {
			// Skip unknown operation types rather than failing the whole batch.
			continue
		}

		data := make(map[string]any, len(dataColNames))
		for i, idx := range dataColIdxs {
			data[dataColNames[i]] = convertDB2Value(scanDest[idx], dataColKinds[i])
		}

		rawEvents = append(rawEvents, ChangeEvent{
			Schema:    schema,
			Table:     tableName,
			QualKey:   tableKey,
			Operation: opType,
			CSN:       csn,
			IntentSeq: intentSeq,
			Timestamp: timestamp,
			Data:      data,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, rawCount, err
	}

	// Cross-poll D+I pairing: read, update, and clear the pending before-image
	// for this table in a single lock acquisition (H4: was 3 separate acquires).
	// pollChangeTable runs on the Streamer goroutine; the only concurrent writer
	// is SetWindow (signals goroutine), which replaces the entire map, so all
	// reads and writes here must be under pendingMu.
	s.pendingMu.Lock()
	pending := s.pendingBeforeByTable[tableKey]
	if pending != nil {
		// Scan all events for the matching I-row (not just index 0) — composite
		// pagination can produce a batch like [evt@7, I@8] where the match is not
		// at position 0.
		// Two-pass scan: prefer opTypeUpdateAfter over OpTypeInsert at the same
		// CSN. If composite pagination splits a D+I pair across batches, LEAD/LAG
		// classifies the I-row as OPCODE=2 (OpTypeInsert) because its D-row was in
		// the previous batch. We promote it — but only if no opTypeUpdateAfter at
		// the same CSN also matches, to avoid misidentifying a concurrent standalone
		// INSERT (same COMMITSEQ, higher IntentSeq) as the UPDATE after-image.
		matchIdx := -1
		for i, ev := range rawEvents {
			if ev.Operation == opTypeUpdateAfter &&
				ev.CSN.Equal(pending.CSN) && ev.IntentSeq > pending.IntentSeq {
				matchIdx = i
				break // opTypeUpdateAfter is the authoritative match; no need to scan further
			}
		}
		if matchIdx < 0 {
			// No opTypeUpdateAfter found; accept OpTypeInsert as fallback (cross-poll
			// D+I split where LEAD/LAG downgraded the I-row to standalone INSERT).
			for i, ev := range rawEvents {
				if ev.Operation == OpTypeInsert &&
					ev.CSN.Equal(pending.CSN) && ev.IntentSeq > pending.IntentSeq {
					matchIdx = i
					rawEvents[i].Operation = opTypeUpdateAfter // ensure pairOpcodeEvents merges it
					break
				}
			}
		}
		if matchIdx >= 0 {
			// Insert the before-image immediately before its matching after-image.
			rawEvents = append(rawEvents, ChangeEvent{}) // grow by 1
			copy(rawEvents[matchIdx+1:], rawEvents[matchIdx:])
			rawEvents[matchIdx] = *pending
		} else {
			// The expected opTypeUpdateAfter row never arrived. This "shouldn't happen"
			// with DB2 SQL Replication but would silently drop one UPDATE event if it
			// does. Log a warning so operators can correlate in IBMSNAP_APPLYTRACE.
			s.log.Warnf("db2 streamer: unmatched UPDATE before-image for table %q at CSN %s — expected opTypeUpdateAfter did not arrive; one UPDATE event dropped",
				tableKey, pending.CSN)
		}
		delete(s.pendingBeforeByTable, tableKey) // clear whether matched or not
	}
	// If the last raw event is an unmatched opTypeUpdateBefore, hold it for
	// the next poll. pairOpcodeEvents will see a complete pair next time.
	if len(rawEvents) > 0 && rawEvents[len(rawEvents)-1].Operation == opTypeUpdateBefore {
		ev := rawEvents[len(rawEvents)-1]
		s.pendingBeforeByTable[tableKey] = &ev
		rawEvents = rawEvents[:len(rawEvents)-1]
	}
	s.pendingMu.Unlock()

	return pairOpcodeEvents(rawEvents), rawCount, nil
}

// pairOpcodeEvents merges consecutive opTypeUpdateBefore + opTypeUpdateAfter pairs
// (produced by the LEAD/LAG query) into a single OpTypeUpdate event with BeforeData
// populated. Pairs must be consecutive and share the same CSN (guaranteed by the
// LEAD/LAG window function and computeSafeCSN pagination).
//
// Cross-batch D+I pairs are handled upstream: pollChangeTable injects the
// pending before-image at the front and strips the trailing before-image before
// calling this function, so pairOpcodeEvents only sees complete pairs or
// non-update events.
func pairOpcodeEvents(events []ChangeEvent) []ChangeEvent {
	if len(events) == 0 {
		return events
	}
	// Fast path: skip the allocation when no UPDATE before-images are present.
	// opTypeUpdateBefore is rare relative to INSERT/DELETE events; avoid the
	// make([]ChangeEvent, ...) cost for the common no-pairs case.
	hasPairs := false
	for i := range events {
		if events[i].Operation == opTypeUpdateBefore {
			hasPairs = true
			break
		}
	}
	if !hasPairs {
		return events
	}

	out := make([]ChangeEvent, 0, len(events))
	for i := 0; i < len(events); i++ {
		ev := events[i]
		if ev.Operation == opTypeUpdateBefore {
			if i+1 < len(events) {
				next := events[i+1]
				if next.Operation == opTypeUpdateAfter && next.CSN.Equal(ev.CSN) {
					out = append(out, ChangeEvent{
						Schema:     next.Schema,
						Table:      next.Table,
						QualKey:    next.QualKey,
						Operation:  OpTypeUpdate,
						CSN:        next.CSN,
						IntentSeq:  next.IntentSeq,
						Timestamp:  next.Timestamp,
						Data:       next.Data,
						BeforeData: ev.Data,
					})
					i++ // skip the after-image row
					continue
				}
			}
			// Orphaned before-image with no matching after-image.
			// pollChangeTable strips the trailing case into pendingBeforeByTable;
			// this guard discards a mid-stream orphan (DB2 anomaly) rather than
			// leaking the internal opTypeUpdateBefore opcode downstream.
			continue
		}
		// Orphaned after-image (DB2 anomaly: CSN mismatch with preceding before-image).
		// We have the new row state but not the before state; emit as INSERT so
		// consumers see the current row value rather than an unrecognised opcode.
		if ev.Operation == opTypeUpdateAfter {
			ev.Operation = OpTypeInsert
		}
		out = append(out, ev)
	}
	return out
}

// sortEventsByCSN sorts events in-place by (CSN, IntentSeq) and returns the slice.
// (CSN, IntentSeq) is a total order — DB2 guarantees IntentSeq is unique within
// a CommitSeq — so SortFunc (unstable, O(n log n)) is safe and faster than SortStableFunc.
func (*Streamer) sortEventsByCSN(events []ChangeEvent) []ChangeEvent {
	slices.SortFunc(events, func(a, b ChangeEvent) int {
		if c := a.CSN.Compare(b.CSN); c != 0 {
			return c
		}
		return cmp.Compare(a.IntentSeq, b.IntentSeq)
	})
	return events
}

// getString extracts a string value from a scanned any pointer.
func getString(dest any) string {
	value := *(dest.(*any))
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// getBytes extracts raw bytes from a scanned any pointer.
func getBytes(dest any) []byte {
	value := *(dest.(*any))
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return nil
	}
}

// getBitDataBytes extracts raw bytes from a scanned any pointer for a DB2
// FOR BIT DATA column.  The DB2 CLI driver returns VARCHAR(n) FOR BIT DATA
// columns as hex-encoded strings (e.g. "00000021B8...") rather than as binary
// []byte.  This function hex-decodes strings so callers always receive the
// actual binary content.
func getBitDataBytes(dest any) []byte {
	value := *(dest.(*any))
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return v
	case string:
		b, err := hex.DecodeString(v)
		if err != nil {
			// Not a valid hex string — return raw bytes as fallback.
			return []byte(v)
		}
		return b
	default:
		return nil
	}
}

// getInt64 extracts an int64 from a scanned any pointer.
// IBMSNAP_INTENTSEQ is typed CHAR(n) FOR BIT DATA on DB2 LUW; the CLI driver
// returns it as []byte. We interpret the bytes as a big-endian binary offset
// (up to the first 8 bytes) so that composite (CSN, IntentSeq) pagination
// correctly resumes mid-transaction without skipping or duplicating rows.
func getInt64(dest any) int64 {
	value := *(dest.(*any))
	if value == nil {
		return 0
	}
	switch v := value.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		// Some drivers return numeric columns as strings.
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
		// IBMSNAP_INTENTSEQ may arrive as a raw-byte string from some driver
		// versions. Decode as big-endian binary, same as the []byte case.
		// encoding/binary.BigEndian.Uint64 is the idiomatic Go approach.
		// Note: the bytes package (bytes.NewReader, etc.) does not provide
		// integer-from-slice helpers; encoding/binary is the correct stdlib path.
		b := []byte(v)
		if len(b) >= 8 {
			return int64(binary.BigEndian.Uint64(b[:8]))
		}
		var n int64
		for _, bv := range b {
			n = (n << 8) | int64(bv)
		}
		return n
	case []byte:
		// IBMSNAP_INTENTSEQ arrives as []byte (CHAR(10) FOR BIT DATA). Decode
		// the leading 8 bytes as a big-endian uint64 for comparison purposes.
		if len(v) >= 8 {
			return int64(binary.BigEndian.Uint64(v[:8]))
		}
		// Shorter than 8 bytes (should not happen for IBMSNAP_INTENTSEQ, which is
		// always CHAR(10)): accumulate as big-endian integer.
		var n int64
		for _, b := range v {
			n = (n << 8) | int64(b)
		}
		return n
	default:
		return 0
	}
}

// getTime extracts a time.Time from a scanned any pointer.
// The DB2 CLI driver returns TIMESTAMP columns as strings via SQL_C_CHAR
// binding, so we fall back to string parsing when the native time.Time
// assertion fails.
func getTime(dest any) time.Time {
	value := *(dest.(*any))
	if value == nil {
		return time.Time{}
	}
	if t, ok := value.(time.Time); ok {
		return t
	}
	if s, ok := value.(string); ok {
		// DB2 native format: "2006-01-02 15:04:05.999999"
		if t, err := time.Parse("2006-01-02 15:04:05.999999", s); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
