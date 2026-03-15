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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	goora "github.com/sijms/go-ora/v2/network"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// https://docs.oracle.com/en/error-help/db/ora-01291/
var errCodeMissingLogFile = 1291

// LogMiner tracks and streams all change events from the configured change
// tables tracked in tables.
type LogMiner struct {
	cfg           *Config
	tables        []replication.UserTable
	publisher     replication.ChangePublisher
	log           *service.Logger
	logCollector  *LogFileCollector
	currentSCN    uint64
	sessionMgr    *SessionManager
	db            *sql.DB
	SleepDuration time.Duration
	dmlParser     *sqlredo.Parser

	// Pre-built query string for LogMiner contents
	logMinerQuery string
	txnCache      TransactionCache
	// lobColTypes maps "SCHEMA.TABLE.COLUMN" (uppercase) to the Oracle data type
	// ("CLOB", "BLOB", or "NCLOB") for all LOB columns across the tracked tables.
	// Populated at startup by querying ALL_TAB_COLUMNS.
	lobColTypes map[string]string
	lobStates   map[string]*txnLOBState
}

// NewMiner creates a new instance of LogMiner responsible for paging through change events based on the tables param.
func NewMiner(db *sql.DB, userTables []replication.UserTable, publisher replication.ChangePublisher, cfg *Config, metrics *service.Metrics, logger *service.Logger) *LogMiner {
	// Build table filter condition once
	// Only filter DML operations (1=INSERT, 2=DELETE, 3=UPDATE) by table
	// Transaction control operations (6=START, 7=COMMIT, 36=ROLLBACK) don't have table info
	var buf strings.Builder
	if len(userTables) > 0 {
		buf.WriteString(" AND (")
		// Transaction control and LOB ops are returned unconditionally.
		// Both SELECT_LOB_LOCATOR (op 9) and LOB_WRITE (op 10) have TABLE_NAME set to
		// the internal LOB segment name (e.g. SYS_LOB…$$), not the actual table name.
		// Schema/table are instead parsed from the SQL_REDO of the LOB locator.
		buf.WriteString("OPERATION_CODE IN (6, 7, 36, 9, 10)")
		// DML carries the real table name — filter by configured tables.
		buf.WriteString(" OR (OPERATION_CODE IN (1, 2, 3) AND (") // Filter DML by table
		for i, t := range userTables {
			if i > 0 {
				buf.WriteString(" OR ")
			}
			fmt.Fprintf(&buf, "(SEG_OWNER = '%s' AND TABLE_NAME = '%s')", strings.ReplaceAll(t.Schema, "'", "''"), strings.ReplaceAll(t.Name, "'", "''"))
		}
		buf.WriteString(")))")
	}
	logMinerQuery := fmt.Sprintf(`
		SELECT
			SCN,
			SQL_REDO,
			OPERATION_CODE,
			TABLE_NAME,
			SEG_OWNER,
			TIMESTAMP,
			XID,
			COMMIT_SCN,
			CSF
		FROM V$LOGMNR_CONTENTS
		WHERE SCN > :1 AND SCN <= :2%s
	`, buf.String())

	lm := &LogMiner{
		cfg:       cfg,
		db:        db,
		tables:    userTables,
		publisher: publisher,
		log:       logger,

		// logminer specific
		logMinerQuery: logMinerQuery,
		logCollector:  NewLogFileCollector(),
		sessionMgr:    NewSessionManager(cfg, logger),
		txnCache:      NewInMemoryCache(cfg.MaxTransactionEvents, metrics, logger),
		dmlParser:     sqlredo.NewParser(),
	}
	lm.lobStates = make(map[string]*txnLOBState)
	return lm
}

// ReadChanges streams the change events from the configured SQL Server change tables.
func (lm *LogMiner) ReadChanges(ctx context.Context, startPos replication.SCN) error {
	// Acquire a dedicated connection so that all LogMiner session operations
	// (NLS settings, ADD_LOGFILE, START_LOGMNR, V$LOGMNR_CONTENTS queries) execute
	// on the same underlying Oracle session. Using lm.db directly risks different
	// calls being routed to different pool connections, breaking session-scoped state.
	conn, err := lm.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring dedicated LogMiner connection: %w", err)
	}
	defer conn.Close()

	if err := replication.ApplyNLSSettings(ctx, conn); err != nil {
		return fmt.Errorf("applying NLS settings for LogMiner: %w", err)
	}

	if err := lm.loadLOBColumnTypes(ctx, conn); err != nil {
		return fmt.Errorf("loading LOB column types: %w", err)
	}

	lm.currentSCN = uint64(startPos)
	lm.log.Infof("Starting streaming change events for %d table(s) beginning from SCN: %d", len(lm.tables), lm.currentSCN)

	defer func() {
		if lm.sessionMgr.IsActive() {
			if err := lm.sessionMgr.EndSession(ctx, conn); err != nil {
				if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
					lm.log.Errorf("ending LogMiner session on exit: %v", err)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if caughtUp, err := lm.miningCycle(ctx, conn); err != nil {
				return fmt.Errorf("mining logs: %w", err)
			} else if caughtUp {
				lm.log.Debugf("Caught up with redo logs, backing off..")
				time.Sleep(lm.cfg.MiningBackoffInterval)
			} else {
				time.Sleep(lm.cfg.MiningInterval)
			}
		}
	}
}

// FindStartPos finds the earliest possible SCN that exists within a log that's still available.
func (lm *LogMiner) FindStartPos(ctx context.Context) (replication.SCN, error) {
	query := `
		SELECT MIN(FIRST_CHANGE#) AS FIRST_SCN
		FROM (
			SELECT FIRST_CHANGE# FROM V$LOG
			UNION
			SELECT FIRST_CHANGE# FROM V$ARCHIVED_LOG
			WHERE NAME IS NOT NULL
			AND ARCHIVED = 'YES'
			AND STATUS = 'A'
			AND DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
	`

	var firstSCN uint64
	if err := lm.db.QueryRowContext(ctx, query).Scan(&firstSCN); err != nil {
		return 0, fmt.Errorf("querying oldest available SCN in logs: %w", err)
	}

	return replication.SCN(firstSCN), nil
}

func (lm *LogMiner) miningCycle(ctx context.Context, conn *sql.Conn) (caughtUp bool, err error) {
	// Get database's current SCN to know our target
	var dbCurrentSCN uint64
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&dbCurrentSCN); err != nil {
		return false, fmt.Errorf("fetching current SCN: %w", err)
	}

	if lm.currentSCN >= dbCurrentSCN {
		return true, nil
	}

	endSCN := dbCurrentSCN
	if maxRange := uint64(lm.cfg.SCNWindowSize); lm.currentSCN+maxRange < dbCurrentSCN {
		endSCN = lm.currentSCN + maxRange
	}

	// Restart the session on every cycle with explicit SCN bounds. Oracle's START_LOGMNR
	// with ENDSCN=0 freezes the session's view at session start time, making events written
	// after session start invisible. Per-window restart with explicit endSCN ensures all
	// events in [currentSCN, endSCN] are visible.
	if err := lm.prepareLogsAndStartSession(ctx, conn, lm.currentSCN, endSCN); err != nil {
		var oraErr *goora.OracleError
		if errors.As(err, &oraErr) && oraErr.ErrCode == errCodeMissingLogFile {
			//nolint:staticcheck
			return false, fmt.Errorf("preparing logs and starting session at position %d: %w\n\n"+
				"This error indicates archived redo logs have been purged before LogMiner could process them.\n"+
				"This typically happens when processing takes longer than Oracle's log retention period.\n\n"+
				"To fix this issue:\n"+
				"1. Increase Oracle's archived log retention using RMAN:\n"+
				"   CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 7 DAYS;\n\n"+
				"2. Improve processing performance:\n"+
				"   - Reduce logminer.scn_window_size (current: %d SCN units) to process smaller windows per cycle\n"+
				"   - Decrease logminer.backoff_interval (current: %v)\n"+
				"   - Increase input batching.count for better throughput\n"+
				"   - Use faster output (e.g., drop: {} for benchmarking)\n\n"+
				"3. Restart the connector from the current database SCN to skip missing logs:\n"+
				"   Note: This will result in data loss for events in the purged logs, so a snapshot may be required.",
				lm.currentSCN, err, lm.cfg.SCNWindowSize, lm.cfg.MiningBackoffInterval)
		}
		return false, fmt.Errorf("preparing logs and starting session at position %d: %w", lm.currentSCN, err)
	}

	// Query and process redoEvents from V$LOGMNR_CONTENTS
	// The session is already active, just query it
	redoEvents, err := lm.queryLogMinerContents(ctx, conn, lm.currentSCN, endSCN)
	if err != nil {
		return false, fmt.Errorf("querying logminer contents between %d and %d: %w", lm.currentSCN, endSCN, err)
	}

	// Process events and buffer transactions
	for _, redoEvent := range redoEvents {
		if err := lm.processRedoEvent(ctx, redoEvent); err != nil {
			return false, fmt.Errorf("process redo event: %w", err)
		}
	}

	lm.currentSCN = endSCN
	return endSCN >= dbCurrentSCN, nil
}

// processRedoEvent buffers emitted events until a commit or rollback event is processed at which
// point the buffer can be flushed to the Connect pipeline or dropped.
func (lm *LogMiner) processRedoEvent(ctx context.Context, redoEvent *sqlredo.RedoEvent) error {
	switch redoEvent.Operation {
	case sqlredo.OpStart:
		// Transaction started
		lm.txnCache.StartTransaction(redoEvent.TransactionID, redoEvent.SCN)

	case sqlredo.OpInsert, sqlredo.OpUpdate, sqlredo.OpDelete:
		// SQL_REDO should always be present for DML operations. If not, it's likely a temporary
		// table (Oracle doesn't generate redo for these) or an unsupported operation.
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			lm.log.Warnf("Skipping DML event with no SQL_REDO (operation=%s, table=%s.%s, scn=%d, txn=%s) - likely temporary table or unsupported operation",
				redoEvent.Operation, redoEvent.SchemaName.String, redoEvent.TableName.String, redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}

		// Parse sql insert/update/delete sql statements into key/value object
		event, err := lm.dmlParser.RedoEventToDMLEvent(redoEvent)
		if err != nil {
			return fmt.Errorf("parsing sql redo event into dml event: %w", err)
		}

		lm.txnCache.AddEvent(redoEvent.TransactionID, redoEvent.SCN, &event)

	case sqlredo.OpSelectLobLocator:
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			lm.log.Warnf("Skipping SELECT_LOB_LOCATOR with no SQL_REDO (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		info, err := sqlredo.ParseSelectLobLocator(redoEvent.SQLRedo.String)
		if err != nil {
			lm.log.Warnf("Failed to parse SELECT_LOB_LOCATOR SQL (scn=%d, txn=%s): %v\nSQL: %.500s", redoEvent.SCN, redoEvent.TransactionID, err, redoEvent.SQLRedo.String)
			return nil
		}
		// Resolve LOB type from the schema cache populated at startup.
		colTypeKey := strings.ToUpper(info.Schema + "." + info.Table + "." + info.Column)
		lobType := lm.lobColTypes[colTypeKey] // "CLOB", "BLOB", "NCLOB", or "" if unknown
		isBinary := lobType == "BLOB"

		state := lm.getOrCreateLOBState(redoEvent.TransactionID)
		key := lobKey{
			Schema:   info.Schema,
			Table:    info.Table,
			Column:   info.Column,
			PKString: fmt.Sprintf("%v", info.PKValues),
		}
		if _, exists := state.accumulators[key]; !exists {
			state.accumulators[key] = &LobAccumulator{
				Schema:   info.Schema,
				Table:    info.Table,
				Column:   info.Column,
				IsBinary: isBinary,
				PKValues: info.PKValues,
			}
		}
		state.activeKey = &key

	case sqlredo.OpLobWrite:
		state, exists := lm.lobStates[redoEvent.TransactionID]
		if !exists || state.activeKey == nil {
			lm.log.Warnf("Received LOB_WRITE without active LOB locator (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		acc := state.accumulators[*state.activeKey]
		if acc == nil {
			return nil
		}
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			return nil
		}
		// NCLOB LOB_WRITE SQL delivers data as a plain string literal (same as CLOB),
		// not as HEXTORAW. Only BLOB uses binary/hex encoding.
		writeInfo, err := sqlredo.ParseLobWrite(redoEvent.SQLRedo.String, acc.IsBinary)
		if err != nil {
			lm.log.Warnf("Failed to parse LOB_WRITE SQL (scn=%d, txn=%s): %v\nSQL (len=%d): %s", redoEvent.SCN, redoEvent.TransactionID, err, len(redoEvent.SQLRedo.String), redoEvent.SQLRedo.String)
			return nil
		}
		acc.AddFragment(writeInfo.Offset, writeInfo.Data)

	case sqlredo.OpCommit:
		// Flush all buffered events for given transaction ID
		if txn := lm.txnCache.GetTransaction(redoEvent.TransactionID); txn != nil {
			safeCheckpointSCN := redoEvent.SCN

			// InMemory cache specific behaviour
			if cache, ok := lm.txnCache.(*InMemoryCache); ok {
				// Compute the safe checkpoint SCN. If other transactions are still
				// open, we must not advance the checkpoint past their start SCN - 1,
				// otherwise a restart with in-memory cache would miss their already-seen DML events.
				if lowestOpenSCN := cache.LowWatermarkSCN(redoEvent.TransactionID); lowestOpenSCN != math.MaxUint64 && lowestOpenSCN > 0 {
					// We subtract 1 because the query resumes from the point before (i.e. SCN > checkpoint)
					if lowestOpenSCN-1 < safeCheckpointSCN {
						safeCheckpointSCN = lowestOpenSCN - 1
					}
				}
			}

			// Merge any accumulated LOB data into DML events before publishing.
			if state, ok := lm.lobStates[redoEvent.TransactionID]; ok {
				mergeLOBsIntoDMLEvents(state, txn.Events, lm.log)
				delete(lm.lobStates, redoEvent.TransactionID)
			}

			for _, dmlEvent := range txn.Events {
				msg := toMessageEvent(dmlEvent, redoEvent.SCN, safeCheckpointSCN)
				if err := lm.publisher.Publish(ctx, msg); err != nil {
					return fmt.Errorf("publishing event with SCN '%d': %w", redoEvent.SCN, err)
				}
			}

			lm.txnCache.CommitTransaction(redoEvent.TransactionID)
		}

	case sqlredo.OpRollback:
		// Discard all buffered events for this transaction
		delete(lm.lobStates, redoEvent.TransactionID)
		lm.txnCache.RollbackTransaction(redoEvent.TransactionID)
	}

	return nil
}

// loadLOBColumnTypes queries ALL_TAB_COLUMNS for the tracked tables and caches
// the data type of every CLOB, BLOB, and NCLOB column. The cache key is
// "SCHEMA.TABLE.COLUMN" (all uppercase); the value is the Oracle data type string.
func (lm *LogMiner) loadLOBColumnTypes(ctx context.Context, conn *sql.Conn) error {
	lm.lobColTypes = make(map[string]string)
	if len(lm.tables) == 0 {
		return nil
	}

	var qb strings.Builder
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

	rows, err := conn.QueryContext(ctx, qb.String())
	if err != nil {
		return fmt.Errorf("querying LOB column types: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var owner, tableName, columnName, dataType string
		if err := rows.Scan(&owner, &tableName, &columnName, &dataType); err != nil {
			return fmt.Errorf("scanning LOB column type row: %w", err)
		}
		key := owner + "." + tableName + "." + columnName
		lm.lobColTypes[key] = dataType
	}
	return rows.Err()
}

func (lm *LogMiner) getOrCreateLOBState(txnID string) *txnLOBState {
	if state, ok := lm.lobStates[txnID]; ok {
		return state
	}
	state := newTxnLOBState()
	lm.lobStates[txnID] = state
	return state
}

func (lm *LogMiner) queryLogMinerContents(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64) ([]*sqlredo.RedoEvent, error) {
	if len(lm.tables) == 0 {
		return nil, nil
	}

	// Use the pre-built query from initialization
	rows, err := conn.QueryContext(ctx, lm.logMinerQuery, startSCN, endSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logminer: %w", err)
	}
	defer rows.Close()

	var (
		events  []*sqlredo.RedoEvent
		pending *sqlredo.RedoEvent // accumulates CSF continuation fragments
	)
	for rows.Next() {
		event := &sqlredo.RedoEvent{}
		var (
			xid       []byte        // Oracle RAW type comes as []byte in Go
			commitSCN sql.NullInt64 // COMMIT_SCN can be NULL for uncommitted transactions
			csf       int64         // Continuation SQL Flag: 1 = more SQL in next row, 0 = complete
		)

		err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.Operation,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&xid,
			&commitSCN,
			&csf,
		)
		if err != nil {
			return nil, err
		}

		// Convert XID to hex string (matches Debezium's approach)
		// XID is Oracle's native transaction identifier (RAW(8) = 8 bytes)
		event.TransactionID = hex.EncodeToString(xid)

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
				events = append(events, pending)
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

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Flush any incomplete pending event (shouldn't happen in practice).
	if pending != nil {
		lm.log.Warnf("Incomplete CSF SQL sequence at end of result set (scn=%d, op=%s, txn=%s)", pending.SCN, pending.Operation, pending.TransactionID)
		events = append(events, pending)
	}

	return events, nil
}

// LogFile represents a redo or archive log file
type LogFile struct {
	FileName  string
	FirstSCN  uint64
	NextSCN   uint64
	Sequence  int64
	Type      string // "ONLINE" or "ARCHIVED"
	IsCurrent bool
	Thread    int
}

// LogFileCollector finds relevant log files to mine
type LogFileCollector struct{}

// NewLogFileCollector creates a new *LogFileCollector which is responsible for
// discovering the relevant log files to mine.
func NewLogFileCollector() *LogFileCollector {
	return &LogFileCollector{}
}

// GetLogs collects all log files containing changes from the given SCN
func (*LogFileCollector) GetLogs(ctx context.Context, conn *sql.Conn, offsetSCN uint64) ([]*LogFile, error) {
	query := `
		SELECT FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD
		FROM (

			-- Online redo logs that contain or come after our position
			SELECT
				MIN(F.MEMBER) AS FILE_NAME,
				L.FIRST_CHANGE# FIRST_CHANGE,
				L.NEXT_CHANGE# NEXT_CHANGE,
				L.SEQUENCE# AS SEQ,
				'ONLINE' AS TYPE,
				L.THREAD# AS THREAD
			FROM V$LOGFILE F, V$LOG L
			WHERE (L.STATUS = 'CURRENT' OR L.NEXT_CHANGE# >= :1)
			AND F.GROUP# = L.GROUP#
			GROUP BY L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.SEQUENCE#, L.THREAD#

			UNION

			-- Archive logs with changes after our position
			SELECT
				A.NAME AS FILE_NAME,
				A.FIRST_CHANGE# FIRST_CHANGE,
				A.NEXT_CHANGE# NEXT_CHANGE,
				A.SEQUENCE# AS SEQ,
				'ARCHIVED' AS TYPE,
				A.THREAD# AS THREAD
			FROM V$ARCHIVED_LOG A
			WHERE A.NAME IS NOT NULL
			AND A.ARCHIVED = 'YES'
			AND A.STATUS = 'A'
			AND A.NEXT_CHANGE# >= :1
			AND A.DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
		ORDER BY SEQ
	`

	rows, err := conn.QueryContext(ctx, query, offsetSCN)
	if err != nil {
		return nil, fmt.Errorf("querying all logs containing changes from SCN %d: %w", offsetSCN, err)
	}
	defer rows.Close()

	var archived, online []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		if err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread); err != nil {
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

// prepareLogsAndStartSession collects redo/archive logs for the given SCN range,
// loads them into LogMiner, and starts a new mining session.
// It is called on every mining cycle with explicit bounds. Passing ENDSCN=0 to
// START_LOGMNR would freeze the session's view at session-start time, making events
// written after that point invisible. An explicit endSCN ensures all events in
// [startSCN, endSCN] are accessible.
func (lm *LogMiner) prepareLogsAndStartSession(ctx context.Context, conn *sql.Conn, startSCN, endSCN uint64) error {
	// End existing session if active
	if lm.sessionMgr.IsActive() {
		if err := lm.sessionMgr.EndSession(ctx, conn); err != nil {
			lm.log.Errorf("Failed to end existing LogMiner session: %v", err)
		}
	}

	// Collect log files that contain changes from current SCN
	var (
		logFiles []*LogFile
		err      error
	)
	if logFiles, err = lm.logCollector.GetLogs(ctx, conn, startSCN); err != nil {
		return fmt.Errorf("collecting redo logs for logminer: %w", err)
	}
	lm.log.Debugf("Collected %d redo log file(s) for LogMiner", len(logFiles))

	if err := lm.sessionMgr.AddLogFile(ctx, conn, logFiles); err != nil {
		return fmt.Errorf("loading %d log files into logminer: %w", len(logFiles), err)
	}
	if err := lm.sessionMgr.StartSession(ctx, conn, startSCN, endSCN, false); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}

	lm.log.Debugf("Started LogMiner session from SCN %d to SCN %d", startSCN, endSCN)

	return nil
}

func toMessageEvent(dml *sqlredo.DMLEvent, scn uint64, checkpointSCN uint64) *replication.MessageEvent {
	m := &replication.MessageEvent{
		SCN:           replication.SCN(scn),
		CheckpointSCN: replication.SCN(checkpointSCN),
		Schema:        dml.Schema,
		Table:         dml.Table,
		Data:          dml.Data,
		Timestamp:     dml.Timestamp,
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
