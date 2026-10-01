// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	goora "github.com/sijms/go-ora/v2/network"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

var (
	// captures the time between DB commit and publish
	publishLatencyMetric = "oracledb_cdc_publish_lag_ns"
	// captures the time from query execution to zero or more rows being returned by LogMiner
	timeToFirstRowMetric = "oracledb_cdc_logminer_time_to_first_row_ns"
	// https://docs.oracle.com/en/error-help/db/ora-01291/
	errCodeMissingLogFile = 1291
	// https://docs.oracle.com/en/error-help/db/ora-01368/
	errCodeRedoLogHeaderMismatch = 1368
)

// LogMiner tracks and streams all change events from the configured change
// tables tracked in tables.
type LogMiner struct {
	cfg          *Config
	tables       []replication.UserTable
	logCollector *LogFileCollector
	currentSCN   uint64
	windowSize   int
	redoVolume   *redoVolumeStrategy
	sessionMgr   *SessionManager
	db           *sql.DB

	// Pre-built query string for LogMiner contents
	logMinerQuery string

	// proc buffers the mined redo rows and publishes committed transactions.
	proc *redoProcessor

	contentStmt    *sql.Stmt
	currentSCNStmt *sql.Stmt

	// suppresses repeated "caught up" log lines within a single idle stretch
	caughtUpLogged bool

	timeToFirstRowMetric *service.MetricTimer
	log                  *service.Logger
}

// NewMiner creates a new instance of LogMiner responsible for paging through change events based on the tables param.
// txnCache sets the transaction buffer implementation; pass nil to use the default in-memory cache.
func NewMiner(db *sql.DB, userTables []replication.UserTable, publisher replication.ChangePublisher, cfg *Config, txnCache TransactionCache, metrics *service.Metrics, logger *service.Logger) *LogMiner {
	// Build table filter condition once
	// Transaction control operations (6=START, 7=COMMIT, 36=ROLLBACK) don't have table info
	// and must pass unfiltered. DML (1=INSERT, 2=DELETE, 3=UPDATE) and LOB operations
	// (9=SELECT_LOB_LOCATOR, 10=LOB_WRITE, 11=LOB_TRIM) all carry SEG_OWNER/TABLE_NAME
	// and must be restricted to configured tables to avoid capturing Oracle internal tables.
	var buf strings.Builder
	if len(userTables) > 0 {
		buf.WriteString(" AND (OPERATION_CODE IN (6, 7, 36)")
		// DML and LOB operations carry the real table name — filter by configured tables.
		dmlCodes := "1, 2, 3"
		if cfg.LOBEnabled {
			dmlCodes += ", 9, 10, 11"
		}
		buf.WriteString(" OR (OPERATION_CODE IN (" + dmlCodes + ") AND (") // Filter DML/LOB by table
		for i, t := range userTables {
			if i > 0 {
				buf.WriteString(" OR ")
			}
			fmt.Fprintf(&buf, "(SEG_OWNER = '%s' AND TABLE_NAME = '%s')", strings.ReplaceAll(t.Schema, "'", "''"), strings.ReplaceAll(t.Name, "'", "''"))
		}
		buf.WriteString(")))")
	}
	if cfg.PDBName != "" {
		fmt.Fprintf(&buf, " AND SRC_CON_NAME = '%s'", strings.ReplaceAll(cfg.PDBName, "'", "''"))
	}

	logMinerQuery := "SELECT SCN, SQL_REDO, OPERATION_CODE, TABLE_NAME, SEG_OWNER, TIMESTAMP, XID, COMMIT_SCN, CSF, USERNAME FROM V$LOGMNR_CONTENTS WHERE SCN > :1 AND SCN <= :2" + buf.String()

	lm := &LogMiner{
		cfg:                  cfg,
		db:                   db,
		tables:               userTables,
		timeToFirstRowMetric: metrics.NewTimer(timeToFirstRowMetric),
		log:                  logger,

		// logminer specific
		logMinerQuery: logMinerQuery,
		logCollector:  NewLogFileCollector(),
		sessionMgr:    NewSessionManager(cfg, logger),
		proc: &redoProcessor{
			lobEnabled:       cfg.LOBEnabled,
			txnCache:         txnCache,
			lobStates:        make(map[sqlredo.TransactionID]*sqlredo.TxnLOBState),
			pendingLOBWrites: make(map[sqlredo.TransactionID][]*sqlredo.RedoEvent),
			dmlParser:        sqlredo.NewParser(),
			publisher:        publisher,
			publishLagMetric: metrics.NewTimer(publishLatencyMetric),
			log:              logger,
		},
		windowSize: cfg.SCNWindowSize,
		redoVolume: newRedoVolumeStrategy(cfg.RedoVolumeMin, cfg.RedoVolumeGrowthMax),
	}
	if lm.proc.txnCache == nil {
		lm.proc.txnCache = NewInMemoryCache(cfg.MaxTransactionEvents, metrics, logger)
	}
	return lm
}

// ReadChanges streams the change events from LogMiner via a mining cycle.
func (lm *LogMiner) ReadChanges(ctx context.Context, startPos replication.SCN) (resErr error) {
	// Acquire a dedicated connection so that all LogMiner session operations
	// (NLS settings, ADD_LOGFILE, START_LOGMNR, V$LOGMNR_CONTENTS queries) execute
	// on the same underlying Oracle session. Using lm.db directly risks different
	// calls being routed to different pool connections, breaking session-scoped state.
	conn, err := lm.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring dedicated logminer connection: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil && resErr == nil {
			resErr = fmt.Errorf("closing connection: %w", err)
		}
	}()

	defer func() {
		if err := lm.Close(); err != nil {
			lm.log.Errorf("closing prepared logminer statements: %v", err)
		}
	}()

	if err := replication.ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("applying NLS settings for logminer: %w", err)
	}

	// always find all lob columns on start up as redo logs don't include column data types.
	// this also prevents inline lob rows being emitted as events.
	if err := lm.loadLOBColumnTypes(ctx); err != nil {
		return fmt.Errorf("discovering LOB column types: %w", err)
	}

	lm.currentSCN = uint64(startPos)
	lm.log.Infof("Starting streaming change events for %d table(s) beginning from SCN: %d", len(lm.tables), lm.currentSCN)

	defer func() {
		if lm.sessionMgr.IsActive() {
			if err := lm.sessionMgr.EndSession(context.Background(), conn); err != nil {
				lm.log.Errorf("ending logminer session on exit: %v", err)
			}
		}
	}()

	timer := time.NewTimer(0) // reused timer, reduces memory allocations
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		caughtUp, err := lm.miningCycle(ctx, conn)
		if err != nil {
			return fmt.Errorf("mining logs: %w", err)
		}

		wait := lm.cfg.MiningInterval
		if caughtUp {
			wait = lm.cfg.MiningBackoffInterval
			if !lm.caughtUpLogged {
				lm.log.Debugf("Caught up with redo logs, backing off for %s...", wait)
			}
		}
		lm.caughtUpLogged = caughtUp
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// Close releases all statements prepared over the lifetime of a ReadChanges
// call, along with those owned by the session manager and log file
// collector. It must only be called once the dedicated connection those
// statements were prepared on is no longer needed for LogMiner operations.
func (lm *LogMiner) Close() error {
	var errs []error

	if lm.contentStmt != nil {
		if err := lm.contentStmt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing logminer contents statement: %w", err))
		}
		lm.contentStmt = nil
	}

	if lm.currentSCNStmt != nil {
		if err := lm.currentSCNStmt.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing current SCN statement: %w", err))
		}
		lm.currentSCNStmt = nil
	}

	if err := lm.logCollector.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing log file collector statements: %w", err))
	}

	if err := lm.redoVolume.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing redo_volume statements: %w", err))
	}

	if err := lm.sessionMgr.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing session manager statements: %w", err))
	}

	return errors.Join(errs...)
}

// FindStartPos returns the database's current SCN so that streaming begins from
// the present moment rather than replaying historical redo logs.
func (lm *LogMiner) FindStartPos(ctx context.Context) (replication.SCN, error) {
	var currentPos uint64
	if err := lm.db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentPos); err != nil {
		return 0, fmt.Errorf("querying current SCN from database: %w", err)
	}
	if currentPos == 0 {
		return 0, errors.New("database returned an invalid CURRENT_SCN value (0)")
	}
	return replication.SCN(currentPos), nil
}

func (lm *LogMiner) endExpiredIdleSession(ctx context.Context, conn *sql.Conn) {
	if !lm.sessionMgr.IsExpired(lm.cfg.MaxSessionAge) {
		return
	}
	lm.log.Debugf("LogMiner session has been open for %s, exceeding max_session_age of %s — ending idle session to release accumulated session memory",
		lm.sessionMgr.Age(), lm.cfg.MaxSessionAge)
	if err := lm.sessionMgr.EndSession(ctx, conn); err != nil {
		lm.log.Errorf("Failed to end idle LogMiner session: %v", err)
	}
}

func (lm *LogMiner) miningCycle(ctx context.Context, conn *sql.Conn) (caughtUp bool, err error) {
	// Get database's current SCN to know our target
	if lm.currentSCNStmt == nil {
		stmt, err := conn.PrepareContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE")
		if err != nil {
			return false, fmt.Errorf("preparing current SCN query: %w", err)
		}
		lm.currentSCNStmt = stmt
	}
	var dbCurrentSCN uint64
	if err := lm.currentSCNStmt.QueryRowContext(ctx).Scan(&dbCurrentSCN); err != nil {
		return false, fmt.Errorf("fetching current SCN: %w", err)
	}

	if lm.currentSCN >= dbCurrentSCN {
		lm.endExpiredIdleSession(ctx, conn)
		return true, nil
	}

	if deferMiningCycle(lm.currentSCN, dbCurrentSCN, lm.cfg.MinSCNWindowSize) {
		lm.endExpiredIdleSession(ctx, conn)
		return true, nil
	}

	var (
		endSCN   uint64
		logFiles []*LogFile

		hitCap bool // scn_window specific
	)

	switch lm.cfg.WindowStrategy {
	case WindowStrategyRedoVolume:
		var consecutiveStalls int
		if logFiles, endSCN, consecutiveStalls, err = lm.redoVolume.selectSession(ctx, conn, lm.logCollector, lm.currentSCN, dbCurrentSCN); err != nil {
			return false, err
		}
		// Proven unreachable for the known failure modes (see
		// logFileSelector.consecutiveStalls) - this should never fire, so
		// treat it as a bug report rather than routine backoff.
		if consecutiveStalls >= redoVolumeStallWarnThreshold {
			lm.log.Warnf("redo_volume selector has made no forward progress for %d consecutive cycles at SCN %d - this should not happen and likely indicates a bug; please report it", consecutiveStalls, lm.currentSCN)
		}
	default:
		endSCN = dbCurrentSCN
		if maxRange := uint64(lm.windowSize); lm.currentSCN+maxRange < dbCurrentSCN {
			endSCN = lm.currentSCN + maxRange
			hitCap = true
		}
	}

	if err := lm.prepareLogsAndStartSession(ctx, conn, lm.currentSCN, endSCN, logFiles); err != nil {
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeMissingLogFile {
			var reduceWindowHint string
			switch lm.cfg.WindowStrategy {
			case WindowStrategyRedoVolume:
				reduceWindowHint = fmt.Sprintf("Reduce logminer.redo_volume_min / logminer.redo_volume_growth_max (current budget: %d x %d bytes per thread)", lm.redoVolume.selector.count, lm.redoVolume.maxRedoLogSizeInBytes)
			default:
				reduceWindowHint = fmt.Sprintf("Reduce logminer.scn_window_size (current: %d SCN units)", lm.cfg.SCNWindowSize)
			}
			//nolint:staticcheck
			return false, fmt.Errorf("preparing logs and starting session at position %d: %w\n\n"+
				"This error indicates archived redo logs have been purged before LogMiner could process them.\n"+
				"This typically happens when processing takes longer than Oracle's log retention period.\n\n"+
				"This can also happen after a flashback and OPEN RESETLOGS on the source database: if this\n"+
				"connector's last checkpoint predates the new incarnation's RESETLOGS_CHANGE#, no log file —\n"+
				"old or new incarnation — covers that gap. This is not a retention issue, and increasing\n"+
				"retention (below) will not help; only option 3 applies in that case.\n\n"+
				"To fix this issue:\n"+
				"1. Increase Oracle's archived log retention using RMAN:\n"+
				"   CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;\n\n"+
				"2. Improve processing performance:\n"+
				"   - %s to process smaller windows per cycle\n"+
				"   - Decrease logminer.backoff_interval (current: %v)\n"+
				"   - Increase input batching.count for better throughput\n"+
				"   - Use faster output (e.g., drop: {} for benchmarking)\n\n"+
				"3. Restart the connector from the current database SCN to skip missing logs:\n"+
				"   - Delete the checkpoint cache entry at checkpoint_cache_key and restart. A flashback rolls\n"+
				"     this row back rather than clearing it (with the default Oracle-based cache), so it will\n"+
				"     still be present and must be deleted explicitly, or the connector resumes from the same\n"+
				"     stale SCN and hits this error again.\n"+
				"   - This loses events between the last checkpoint and the restart. To avoid that, delete the\n"+
				"     checkpoint and set snapshot_mode to snapshot_and_stream at the same time — snapshot_mode\n"+
				"     alone has no effect, since a checkpoint that is still present skips snapshotting entirely.",
				lm.currentSCN, err, reduceWindowHint, lm.cfg.MiningBackoffInterval)
		}
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeRedoLogHeaderMismatch {
			lm.log.Debugf("ORA-01368: redo log sequence recycled before session could start (SCN range %d–%d); the log will be available as an archived log on next cycle", lm.currentSCN, endSCN)
			return false, nil
		}
		return false, fmt.Errorf("preparing logs and starting session at position %d: %w", lm.currentSCN, err)
	}

	// Query and process redoEvents from V$LOGMNR_CONTENTS
	// The session is already active, just query it
	if lastSCN, err := lm.queryLogMinerContents(ctx, conn, lm.currentSCN, endSCN, len(logFiles), lm.proc.processRedoEvent); err != nil {
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeRedoLogHeaderMismatch {
			// Resume just before the last processed SCN rather than from the start
			// of the window, so each retry makes progress. Rows at lastSCN are
			// processed again, as the query may have stopped part way through them.
			startSCN := lm.currentSCN
			if lastSCN > startSCN {
				lm.currentSCN = lastSCN - 1 // last row may have stopped part way through
			}
			lm.log.Warnf("ORA-01368: redo log sequence recycled mid-query (SCN range %d–%d); retrying from SCN %d — archived log will be used on next cycle", startSCN, endSCN, lm.currentSCN)
			return false, nil
		}
		return false, fmt.Errorf("querying logminer contents between %d and %d: %w", lm.currentSCN, endSCN, err)
	}

	switch lm.cfg.WindowStrategy {
	case WindowStrategyRedoVolume:
		lm.redoVolume.resetIfUncapped()
	default:
		lm.windowSize = adaptWindowSize(lm.windowSize, hitCap, lm.cfg.MinSCNWindowSize, lm.cfg.MaxSCNWindowSize, lm.cfg.SCNWindowSize)
	}
	lm.currentSCN = endSCN
	return endSCN >= dbCurrentSCN, nil
}

func (lm *LogMiner) loadLOBColumnTypes(ctx context.Context) (resErr error) {
	lm.proc.lobColTypes = make(map[string]string)
	if len(lm.tables) == 0 {
		return nil
	}

	// ALL_TAB_COLUMNS must run in PDB context in CDB mode — the LogMiner conn is
	// pinned to CDB$ROOT where PDB tables are not visible via ALL_TAB_COLUMNS.
	// Use a separate connection and switch context if needed.
	catalogConn, err := lm.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection for LOB column discovery: %w", err)
	}
	defer func() {
		if err := catalogConn.Close(); err != nil && resErr == nil {
			resErr = fmt.Errorf("closing catalog connection: %w", err)
		}
	}()

	if lm.cfg.PDBName != "" {
		// can't use parameterized queries here but we've validated on input.
		if _, err := catalogConn.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+lm.cfg.PDBName); err != nil {
			return fmt.Errorf("switching session to PDB %s for LOB column discovery: %w", lm.cfg.PDBName, err)
		}
		defer func() {
			if _, err := catalogConn.ExecContext(context.Background(), "ALTER SESSION SET CONTAINER = CDB$ROOT"); err != nil && resErr == nil {
				resErr = fmt.Errorf("switching session back to root container: %w", err)
			}
		}()
	}

	var (
		qb     strings.Builder
		qbArgs []any
	)
	qb.WriteString(`SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS WHERE DATA_TYPE IN ('CLOB', 'BLOB', 'NCLOB') AND (`)
	for i, t := range lm.tables {
		if i > 0 {
			qb.WriteString(" OR ")
		}
		fmt.Fprintf(&qb, "(OWNER = '%s' AND TABLE_NAME = '%s')",
			strings.ReplaceAll(strings.ToUpper(t.Schema), "'", "''"),
			strings.ReplaceAll(strings.ToUpper(t.Name), "'", "''"))
	}
	qb.WriteString(")")

	rows, err := catalogConn.QueryContext(ctx, qb.String(), qbArgs...)
	if err != nil {
		return fmt.Errorf("querying LOB column types: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			lm.log.Errorf("closing rows: %v", err)
		}
	}()

	for rows.Next() {
		var owner, tableName, columnName, dataType string
		if err := rows.Scan(&owner, &tableName, &columnName, &dataType); err != nil {
			return fmt.Errorf("scanning LOB column type row: %w", err)
		}
		// example: "TESTDB.PRODUCTS.DESCRIPTION" : "CLOB"
		k := fmt.Sprintf("%s.%s.%s", owner, tableName, columnName)
		lm.proc.lobColTypes[k] = dataType
	}

	return rows.Err()
}

// queryLogMinerContents streams the rows in (startSCN, endSCN] to processEvent.
// lastSCN is the SCN of the last event processed, and is returned alongside
// any error so a caller can resume from where the query stopped. selectedFileCount
// is only used for logging, under WindowStrategyRedoVolume.
func (lm *LogMiner) queryLogMinerContents(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64, selectedFileCount int, processEvent func(context.Context, *sqlredo.RedoEvent) error) (lastSCN uint64, err error) {
	if len(lm.tables) == 0 {
		return lastSCN, nil
	}

	// Use the pre-built query from initialization
	switch lm.cfg.WindowStrategy {
	case WindowStrategyRedoVolume:
		lm.log.Debugf("Executing LogMiner query with SCN range (scn=%d to %d, redo_volume budget=%d x %d bytes per thread, %d files selected)",
			startSCN, endSCN, lm.redoVolume.selector.count, lm.redoVolume.maxRedoLogSizeInBytes, selectedFileCount)
	default:
		lm.log.Debugf("Executing LogMiner query with SCN range (scn=%d to %d with window %d)", startSCN, endSCN, lm.windowSize)
	}
	if lm.contentStmt == nil {
		stmt, err := conn.PrepareContext(ctx, lm.logMinerQuery)
		if err != nil {
			return lastSCN, fmt.Errorf("preparing logminer contents query: %w", err)
		}
		lm.contentStmt = stmt
	}
	queryStart := time.Now()
	rows, err := lm.contentStmt.QueryContext(ctx, startSCN, endSCN)
	if err != nil {
		return lastSCN, fmt.Errorf("querying logminer: %w", err)
	}
	defer rows.Close()

	var (
		pending  *sqlredo.RedoEvent // accumulates CSF continuation fragments
		firstRow = true
	)
	for rows.Next() {
		if firstRow {
			elapsed := time.Since(queryStart)
			lm.timeToFirstRowMetric.Timing(elapsed.Nanoseconds())
			lm.log.Debugf("LogMiner query returned first row after %s (scn=%d to %d)", elapsed, startSCN, endSCN)
			firstRow = false
		}
		event := &sqlredo.RedoEvent{}
		var (
			commitSCN sql.NullInt64 // COMMIT_SCN can be NULL for uncommitted transactions
			csf       int64         // Continuation SQL Flag: 1 = more SQL in next row, 0 = complete
		)

		if err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.Operation,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&event.TransactionID,
			&commitSCN,
			&csf,
			&event.Username,
		); err != nil {
			return lastSCN, err
		}

		// CSF (Continuation SQL Flag): Oracle splits long SQL across multiple rows.
		// Rows with CSF=1 are continuation fragments; CSF=0 is the final (or only) row.
		// Concatenate all fragments before emitting the event.
		if pending != nil {
			// Append this fragment's SQL to the accumulated SQL.
			if event.SQLRedo.Valid {
				pending.SQLRedo.String += event.SQLRedo.String
			}
			if csf == 0 {
				// Final fragment — emit the accumulated event.
				if err := processEvent(ctx, pending); err != nil {
					return lastSCN, fmt.Errorf("processing redo event: %w", err)
				}
				// The first fragment's SCN, so a retry re-reads the whole statement.
				lastSCN = pending.SCN
				pending = nil
			}
			// If csf == 1, continue accumulating.
			continue
		}

		if csf == 1 {
			// Start accumulating a multi-part SQL.
			pending = event
			continue
		}

		if err := processEvent(ctx, event); err != nil {
			return lastSCN, fmt.Errorf("processing redo event: %w", err)
		}
		lastSCN = event.SCN
	}

	if err := rows.Err(); err != nil {
		return lastSCN, err
	}

	// capture timings if 0 rows
	if firstRow {
		elapsed := time.Since(queryStart)
		lm.timeToFirstRowMetric.Timing(elapsed.Nanoseconds())
		lm.log.Debugf("LogMiner query returned no rows after %s (scn=%d to %d)", elapsed, startSCN, endSCN)
	}

	// Flush any incomplete pending event (shouldn't happen in practice).
	if pending != nil {
		lm.log.Warnf("Incomplete CSF SQL sequence at end of result set (scn=%d, op=%s, txn=%s)", pending.SCN, pending.Operation, pending.TransactionID)
		if err := processEvent(ctx, pending); err != nil {
			return lastSCN, fmt.Errorf("processing redo event: %w", err)
		}
	}

	return lastSCN, nil
}

const (
	// logStatusArchived is the Status value GetLogsBySCNRange hardcodes for
	// every archive log record (see the query below). Oracle's V$LOG.STATUS
	// values (CURRENT/ACTIVE/INACTIVE/...) never take this value, so it
	// reliably distinguishes a fully-archived, immutable copy from an online
	// (still mutable) one, without depending on the Type/IsCurrent fields.
	logStatusArchived = "ARCHIVED"
	logStatusCurrent  = "CURRENT"
)

// LogFile represents a redo or archive log file
type LogFile struct {
	FileName  string
	FirstSCN  uint64
	NextSCN   uint64
	Sequence  int64
	Type      string // "ONLINE" or "ARCHIVED"
	IsCurrent bool
	Thread    int
	Status    string
	// SizeBytes is the file's on-disk size, budgeted by redo_volume
	// instead of a flat file count (see logFileSelector).
	SizeBytes uint64
}

// IsOpenCurrent reports whether this is the single open current redo log
// (see logStatusCurrent) - the only file whose NextSCN keeps advancing.
// Unlike IsCurrent, it is false for ACTIVE/INACTIVE logs that have already
// switched away.
func (lf *LogFile) IsOpenCurrent() bool {
	return lf.Status == logStatusCurrent
}

// IsArchived reports whether this is a fully-archived, immutable log copy,
// as opposed to an online one (CURRENT, ACTIVE, or INACTIVE) that Oracle
// could still be writing to or hasn't archived yet.
func (lf *LogFile) IsArchived() bool {
	return lf.Status == logStatusArchived
}

// LogFileCollector finds relevant log files to mine
type LogFileCollector struct {
	stmt *sql.Stmt
}

// NewLogFileCollector creates a new *LogFileCollector which is responsible for
// discovering the relevant log files to mine.
func NewLogFileCollector() *LogFileCollector {
	return &LogFileCollector{}
}

// GetLogsBySCNRange collects log files whose SCN range overlaps [startSCN, endSCN].
func (c *LogFileCollector) GetLogsBySCNRange(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64) ([]*LogFile, error) {
	query := `
		SELECT FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD, STATUS, BYTES
		FROM (

			-- Online redo logs that overlap [startSCN, endSCN]
			SELECT
				MIN(F.MEMBER) AS FILE_NAME,
				L.FIRST_CHANGE# FIRST_CHANGE,
				L.NEXT_CHANGE# NEXT_CHANGE,
				L.SEQUENCE# AS SEQ,
				'ONLINE' AS TYPE,
				L.THREAD# AS THREAD,
				L.STATUS AS STATUS,
				L.BYTES AS BYTES
			FROM V$LOGFILE F, V$LOG L
			WHERE (L.STATUS = 'CURRENT' OR L.NEXT_CHANGE# >= :1)
			AND L.FIRST_CHANGE# <= :2
			AND F.GROUP# = L.GROUP#
			GROUP BY L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.SEQUENCE#, L.THREAD#, L.STATUS, L.BYTES

			UNION

			-- Archive logs that overlap [startSCN, endSCN]
			SELECT
				A.NAME AS FILE_NAME,
				A.FIRST_CHANGE# FIRST_CHANGE,
				A.NEXT_CHANGE# NEXT_CHANGE,
				A.SEQUENCE# AS SEQ,
				'ARCHIVED' AS TYPE,
				A.THREAD# AS THREAD,
				'ARCHIVED' AS STATUS,
				A.BLOCKS * A.BLOCK_SIZE AS BYTES
			FROM V$ARCHIVED_LOG A, V$DATABASE D
			WHERE A.NAME IS NOT NULL
			AND A.ARCHIVED = 'YES'
			AND A.STATUS = 'A'
			AND A.NEXT_CHANGE# >= :1
			AND A.FIRST_CHANGE# <= :2
			AND A.RESETLOGS_CHANGE# = D.RESETLOGS_CHANGE#
			AND A.RESETLOGS_TIME = D.RESETLOGS_TIME
			AND A.DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
		ORDER BY SEQ`

	if c.stmt == nil {
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("preparing logs by SCN range query: %w", err)
		}
		c.stmt = stmt
	}

	rows, err := c.stmt.QueryContext(ctx, startSCN, endSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logs overlapping SCN range [%d, %d]: %w", startSCN, endSCN, err)
	}
	defer rows.Close()

	var archived, online []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		if err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread, &lf.Status, &lf.SizeBytes); err != nil {
			return nil, fmt.Errorf("scanning logs row: %w", err)
		}
		lf.IsCurrent = lf.Type == "ONLINE"
		if lf.IsCurrent {
			online = append(online, lf)
		} else {
			archived = append(archived, lf)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deduplicateLogs(archived, online), nil
}

// Close releases the prepared GetLogsBySCNRange statement, if any.
func (c *LogFileCollector) Close() error {
	var errs []error
	if c.stmt != nil {
		if err := c.stmt.Close(); err != nil {
			errs = append(errs, err)
		}
		c.stmt = nil
	}
	return errors.Join(errs...)
}

// deduplicateLogs merges archive and online log lists, preferring the archive
// copy when the same (thread, sequence) exists in both (archived logs guarantee
// completeness where as online logs are still being written to). This prevents
// ORA-01289 when V$ARCHIVED_LOG contains multiple registrations of the same
// physical file, or when a sequence appears in both V$LOG and V$ARCHIVED_LOG.
func deduplicateLogs(archived, online []*LogFile) []*LogFile {
	type logKey struct {
		thread   int
		sequence int64
	}

	archivedKeys := make(map[logKey]struct{}, len(archived))
	for _, f := range archived {
		archivedKeys[logKey{f.Thread, f.Sequence}] = struct{}{}
	}

	out := make([]*LogFile, 0, len(archived)+len(online))
	out = append(out, archived...)
	for _, f := range online {
		if _, covered := archivedKeys[logKey{f.Thread, f.Sequence}]; !covered {
			out = append(out, f)
		}
	}
	return out
}

// prepareLogsAndStartSession collects redo/archive logs for the given SCN range and
// starts (or restarts) a LogMiner session with explicit SCN bounds. Files are only reloaded
// (via ADD_LOGFILE) when the required set of logs that contain SCN range changes - so the session is kept
// open across consecutive windows that cover the same log files.
func (lm *LogMiner) prepareLogsAndStartSession(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64, preSelected []*LogFile) error {
	logFiles := preSelected
	if logFiles == nil {
		var err error
		if logFiles, err = lm.logCollector.GetLogsBySCNRange(ctx, conn, startSCN, endSCN); err != nil {
			return fmt.Errorf("collecting redo logs for logminer: %w", err)
		}
	}
	types := make([]string, len(logFiles))
	for i, f := range logFiles {
		types[i] = f.Type
	}
	// On databases where redo log switches are infrequent, a LogMiner session can stay
	// open for hours, accumulating server-side PGA (notably around online catalog
	// dictionary lookups) until Oracle kills it outright with ORA-04036.
	sessionExpired := lm.sessionMgr.IsExpired(lm.cfg.MaxSessionAge)
	if sessionExpired {
		lm.log.Debugf("LogMiner session has been open for %s, exceeding max_session_age of %s — forcing restart to release accumulated session memory",
			lm.sessionMgr.Age(), lm.cfg.MaxSessionAge)
	}

	if lm.sessionMgr.logFilesChanged(logFiles) || sessionExpired {
		// Log files have changed (first start or log switch), or the session has exceeded
		// its maximum age — full reload required.
		if lm.sessionMgr.IsActive() {
			if err := lm.sessionMgr.EndSession(ctx, conn); err != nil {
				lm.log.Errorf("Failed to end existing LogMiner session: %v", err)
			}
		}
		if err := lm.sessionMgr.AddLogFile(ctx, conn, logFiles); err != nil {
			return fmt.Errorf("loading %d log files into logminer: %w", len(logFiles), err)
		}
	}

	if err := lm.sessionMgr.StartSession(ctx, conn, startSCN, endSCN, false); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	lm.log.Debugf("Started LogMiner session from SCN %d to SCN %d", startSCN, endSCN)

	return nil
}

func toMessageEvent(dml *sqlredo.DMLEvent, scn uint64, checkpointSCN uint64, commitTimestamp time.Time) *replication.MessageEvent {
	var data map[string]any
	switch dml.Operation {
	case sqlredo.OpDelete:
		// column values are parsed into OldValues, not Data.
		data = dml.OldValues
	case sqlredo.OpUpdate:
		// merge new values onto old value for a current view that includes the PK
		data = make(map[string]any, len(dml.OldValues))
		maps.Copy(data, dml.OldValues)
		maps.Copy(data, dml.Data)
	default:
		data = dml.Data
	}

	m := &replication.MessageEvent{
		SCN:             replication.SCN(scn),
		CheckpointSCN:   replication.SCN(checkpointSCN),
		Schema:          dml.Schema,
		Table:           dml.Table,
		Data:            data,
		Timestamp:       dml.Timestamp,
		TransactionID:   dml.TransactionID.String(),
		CommitTimestamp: commitTimestamp,
		Username:        dml.Username,
	}

	switch dml.Operation {
	case sqlredo.OpInsert:
		m.Operation = replication.MessageOperationInsert
	case sqlredo.OpUpdate:
		m.Operation = replication.MessageOperationUpdate
	case sqlredo.OpDelete:
		m.Operation = replication.MessageOperationDelete
	}

	return m
}

func deferMiningCycle(currentSCN, dbCurrentSCN uint64, minWindowSize int) bool {
	// check to see if SCN window size is greater than configured value
	if minWindowSize <= 0 || dbCurrentSCN <= currentSCN {
		return false
	}
	return dbCurrentSCN-currentSCN < uint64(minWindowSize)
}

func adaptWindowSize(currentSize int, hitCap bool, minSize, maxSize, increment int) int {
	if hitCap {
		return min(currentSize+increment, maxSize)
	}
	return max(currentSize-increment, minSize)
}
