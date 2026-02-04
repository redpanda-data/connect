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
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/dmlparser"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// ChangeEvent represents the final change event emitted to Kafka
type ChangeEvent struct {
	SCN       replication.SCN
	Operation string // "CREATE", "UPDATE", "DELETE"
	Schema    string
	Table     string
	Data      map[string]any
	Timestamp time.Time
	TxnID     string
}

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
	eventProc     *EventProcessor
	db            *sql.DB
	SleepDuration time.Duration
	dmlParser     *dmlparser.LogMinerDMLParser

	// Pre-built query string for LogMiner contents
	logMinerQuery string
	txnCache      TransactionCache

	// Session state tracking (for keeping session alive between iterations)
	sessionActive           bool
	currentLogFiles         []*LogFile
	currentRedoLogSequences []int64
}

// NewMiner creates a new instance of NewMiner, responsible
// for paging through change events based on the tables param.
func NewMiner(db *sql.DB, userTables []replication.UserTable, publisher replication.ChangePublisher, cfg *Config, logger *service.Logger) *LogMiner {
	// Build table filter condition once
	// Only filter DML operations (1=INSERT, 2=DELETE, 3=UPDATE) by table
	// Transaction control operations (6=START, 7=COMMIT, 36=ROLLBACK) don't have table info
	var tableFilter strings.Builder
	if len(userTables) > 0 {
		tableFilter.WriteString(" AND (")
		tableFilter.WriteString("OPERATION_CODE IN (6, 7, 36)")           // Allow all transaction control events
		tableFilter.WriteString(" OR (OPERATION_CODE IN (1, 2, 3) AND (") // Filter DML by table
		for i, t := range userTables {
			if i > 0 {
				tableFilter.WriteString(" OR ")
			}
			tableFilter.WriteString(fmt.Sprintf("(SEG_OWNER = '%s' AND TABLE_NAME = '%s')", t.Schema, t.Name))
		}
		tableFilter.WriteString(")))")
	}
	logMinerQuery := fmt.Sprintf(`
		SELECT
			SCN,
			SQL_REDO,
			-- SQL_UNDO,        -- Not used, only SQL_REDO is parsed
			OPERATION_CODE,
			TABLE_NAME,
			SEG_OWNER,
			TIMESTAMP,
			XID                  -- Oracle's native transaction identifier (RAW)
			-- XIDUSN,           -- Not used, XID contains this
			-- XIDSLT,           -- Not used, XID contains this
			-- XIDSQN            -- Not used, XID contains this
			-- RS_ID,            -- Not used
			-- SSN               -- Not used (only needed for ORDER BY)
		FROM V$LOGMNR_CONTENTS
		WHERE SCN >= :1 AND SCN < :2%s
		ORDER BY SCN, SSN
	`, tableFilter.String())

	lm := &LogMiner{
		cfg:           cfg,
		log:           logger,
		db:            db,
		tables:        userTables,
		publisher:     publisher,
		logMinerQuery: logMinerQuery,

		logCollector: NewLogFileCollector(db),
		sessionMgr:   NewSessionManager(db, cfg),
		eventProc:    NewEventProcessor(),
		txnCache:     NewInMemoryCache(logger),
		dmlParser:    dmlparser.New(),
	}
	return lm
}

// ReadChanges streams the change events from the configured SQL Server change tables.
func (lm *LogMiner) ReadChanges(ctx context.Context, startPos replication.SCN) error {
	// apply nls session for consistent logminer datetime output. (sql.DB is a connection pool,
	// so we need to ensure NLS settings are applied to the connection used by LogMiner).
	if err := replication.ApplyNLSSettings(ctx, lm.db); err != nil {
		return fmt.Errorf("applying NLS settings for LogMiner: %w", err)
	}

	// Determine starting SCN
	var scnSource string
	if startPos.IsValid() {
		// Resume from checkpoint/snapshot position
		lm.currentSCN = uint64(startPos)
		scnSource = "checkpoint"
	} else {
		// get current SCN from DB
		var scn uint64
		if err := lm.db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&scn); err != nil {
			return fmt.Errorf("fetching current SCN from database: %w", err)
		}
		lm.currentSCN = scn
		scnSource = "database"
		// lm.log.Infof("Starting from current SCN sourced from database: %d", lm.currentSCN)
	}

	lm.log.Infof("Starting streaming change events for %d table(s) beginning from SCN (sourced from %s): %d", len(lm.tables), scnSource, lm.currentSCN)

	defer func() {
		if lm.sessionActive {
			if err := lm.sessionMgr.EndSession(); err != nil {
				lm.log.Errorf("ending LogMiner session on exit: %v", err)
			}
			lm.sessionActive = false
		}
	}()

	// set initial log sequences
	if _, err := lm.checkLogSwitchOccurred(); err != nil {
		return fmt.Errorf("initializing redo log sequence tracking: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := lm.miningCycle(ctx); err != nil {
				return fmt.Errorf("mining logs: %w", err)
			}

			time.Sleep(lm.cfg.MiningBackoffInterval)
		}
	}
}

func (lm *LogMiner) miningCycle(ctx context.Context) error {
	// Get database's current SCN to know our target
	var dbCurrentSCN uint64
	if err := lm.db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&dbCurrentSCN); err != nil {
		return fmt.Errorf("fetching current SCN: %w", err)
	}

	if lm.currentSCN >= dbCurrentSCN {
		lm.log.Debugf("Caught up to current SCN %d, no new changes to process", dbCurrentSCN)
		return nil
	}

	// Calculate SCN range - process up to current SCN or a reasonable chunk,
	// limiting the range to avoid huge queries when there's a large gap
	endSCN := dbCurrentSCN
	maxRange := uint64(lm.cfg.MaxBatchSize)
	if endSCN-lm.currentSCN > maxRange {
		endSCN = lm.currentSCN + maxRange
	}

	// Check if we need to restart the session due to log switch
	logSwitched, err := lm.checkLogSwitchOccurred()
	if err != nil {
		return fmt.Errorf("checking for log switch: %w", err)
	}

	if logSwitched || !lm.sessionActive {
		lm.log.Infof("Restarting LogMiner session (log_switch=%t, session_active=%t)", logSwitched, lm.sessionActive)
		if err := lm.prepareLogsAndStartSession(lm.currentSCN); err != nil {
			return fmt.Errorf("preparing logs and starting session at position %d: %w", lm.currentSCN, err)
		}
	}

	// Query and process events from V$LOGMNR_CONTENTS
	// The session is already active, just query it
	events, err := lm.queryLogMinerContents(lm.currentSCN, endSCN)
	if err != nil {
		return fmt.Errorf("querying logminer contents between %d and %d: %w", lm.currentSCN, endSCN, err)
	}

	// Process events and buffer transactions
	for _, event := range events {
		if err := lm.processEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}
	}

	lm.log.Debugf("Processed %d events in SCN range %d - %d", len(events), lm.currentSCN, endSCN)
	lm.currentSCN = endSCN

	return nil
}

// processEvent buffers emitted events until a commit or rollback event is processed at which
// point the buffer can be flushed to the Connect pipeline or dropped.
func (lm *LogMiner) processEvent(ctx context.Context, rawEvent *dmlparser.LMEvent) error {
	switch rawEvent.Operation {
	case dmlparser.OpStart:
		// Transaction started
		lm.txnCache.StartTransaction(rawEvent.TransactionID, rawEvent.SCN)

	case dmlparser.OpInsert, dmlparser.OpUpdate, dmlparser.OpDelete:
		// SQL_REDO should always be present for DML operations. If not, it's likely a temporary
		// table (Oracle doesn't generate redo for these) or an unsupported operation.
		if !rawEvent.SQLRedo.Valid || rawEvent.SQLRedo.String == "" {
			lm.log.Warnf("Skipping DML event with no SQL_REDO (operation=%s, table=%s.%s, scn=%d, txn=%s) - likely temporary table or unsupported operation",
				rawEvent.Operation, rawEvent.SchemaName.String, rawEvent.TableName.String, rawEvent.SCN, rawEvent.TransactionID)
			return nil
		}

		// Parse sql insert/update/delete sql statements into key/value object
		//TODO: Should we do this, or some of it only after commit is received? Measure performance impact.
		event, err := lm.dmlParser.RawEventToDMLEvent(rawEvent)
		if err != nil {
			return fmt.Errorf("parsing sql query into object: %w", err)
		}

		lm.txnCache.AddEvent(rawEvent.TransactionID, event)
	case dmlparser.OpCommit:
		// Flush all buffered events for this transaction
		if txn := lm.txnCache.GetTransaction(rawEvent.TransactionID); txn != nil {
			for _, ev := range txn.Events {
				msg := lm.eventProc.toEventMessage(ev, rawEvent.SCN)
				if err := lm.publisher.Publish(ctx, msg); err != nil {
					return fmt.Errorf("publishing event with SCN '%d`: %w", rawEvent.SCN, err)
				}
			}

			lm.txnCache.CommitTransaction(rawEvent.TransactionID)
		}

	case dmlparser.OpRollback:
		// Discard all buffered events for this transaction
		lm.txnCache.RollbackTransaction(rawEvent.TransactionID)
	}

	return nil
}

func (lm *LogMiner) queryLogMinerContents(startSCN, endSCN uint64) ([]*dmlparser.LMEvent, error) {
	// Use the pre-built query from initialization
	rows, err := lm.db.Query(lm.logMinerQuery, startSCN, endSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logminer: %w", err)
	}
	defer rows.Close()

	// TODO: Can we grow this memory buffer and keep reusing it?
	var events []*dmlparser.LMEvent
	for rows.Next() {
		event := &dmlparser.LMEvent{}
		var xid []byte // Oracle RAW type comes as []byte in Go

		err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			// &event.SQLUndo,        // Not used, only SQL_REDO is parsed
			&event.OperationCode,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&xid,
		)
		if err != nil {
			return nil, err
		}

		// Convert XID to hex string (matches Debezium's approach)
		// XID is Oracle's native transaction identifier (RAW(8) = 8 bytes)
		event.TransactionID = hex.EncodeToString(xid)
		event.Operation = dmlparser.OperationFromCode(event.OperationCode)
		events = append(events, event)
	}

	return events, rows.Err()
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
type LogFileCollector struct {
	db *sql.DB
}

// NewLogFileCollector creates a new *LogFileCollector which is responsible for
// discovering the relevant log files to mine.
func NewLogFileCollector(db *sql.DB) *LogFileCollector {
	return &LogFileCollector{db: db}
}

// GetLogs collects all log files containing changes from the given SCN
func (lfc *LogFileCollector) GetLogs(offsetSCN uint64) ([]*LogFile, error) {
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
			WHERE (L.STATUS = 'CURRENT' OR L.NEXT_CHANGE# > :1)
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
			AND A.NEXT_CHANGE# > :1
			AND A.DEST_ID IN (
				SELECT DEST_ID
				FROM V$ARCHIVE_DEST_STATUS
				WHERE STATUS='VALID' AND TYPE='LOCAL' AND ROWNUM=1
			)
		)
		ORDER BY SEQ
	`

	rows, err := lfc.db.Query(query, offsetSCN)
	if err != nil {
		return nil, fmt.Errorf("querying all logs containing changes from SCN %d: %w", offsetSCN, err)
	}
	defer rows.Close()

	var logFiles []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		if err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread); err != nil {
			return nil, fmt.Errorf("scanning logs row: %w", err)
		}
		lf.IsCurrent = lf.Type == "ONLINE"
		logFiles = append(logFiles, lf)
	}

	return logFiles, rows.Err()
}

// checkLogSwitchOccurred detects if a redo log switch has occurred by comparing
// current redo log sequences with the previously tracked sequences.
// This is used to determine when the LogMiner session needs to be restarted to pick up new redo log files.
// A log switch occurs when a reodo log has reached a given size and been replaced with a new log, or a given time has exceeded.
func (lm *LogMiner) checkLogSwitchOccurred() (bool, error) {
	var (
		rows             *sql.Rows
		err              error
		currentSequences []int64
	)

	// Query current redo log sequences to compare with existing sequences
	if rows, err = lm.db.Query(`SELECT SEQUENCE# FROM V$LOG WHERE STATUS = 'CURRENT' ORDER BY SEQUENCE#`); err != nil {
		return false, fmt.Errorf("querying current redo log sequences: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return false, err
		}
		currentSequences = append(currentSequences, seq)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	// If we haven't tracked sequences yet, store them and return false
	if lm.currentRedoLogSequences == nil {
		lm.currentRedoLogSequences = currentSequences
		return false, nil
	}

	// Compare with previous sequences
	switch {
	// fast path, sizes are different
	case len(currentSequences) != len(lm.currentRedoLogSequences):
		lm.log.Debugf("Redo log switch detected: sequence count changed from %d to %d", len(lm.currentRedoLogSequences), len(currentSequences))
		lm.currentRedoLogSequences = currentSequences
		return true, nil
	default:
		// slow path, compare contents of array
		for i := range currentSequences {
			if currentSequences[i] != lm.currentRedoLogSequences[i] {
				lm.log.Debugf("Redo log switch detected: sequence changed from %v to %v", lm.currentRedoLogSequences, currentSequences)
				lm.currentRedoLogSequences = currentSequences
				return true, nil
			}
		}
	}

	return false, nil
}

// prepareLogsAndStartSession collects redo/archive logs for the given SCN,
// loads them into LogMiner, and starts a new mining session.
// This should be called initially and whenever a log switch is detected.
// The session is started without an endSCN boundary, allowing continuous mining.
func (lm *LogMiner) prepareLogsAndStartSession(startSCN uint64) error {
	// End existing session if active
	if lm.sessionActive {
		if err := lm.sessionMgr.EndSession(); err != nil {
			lm.log.Errorf("Failed to end existing LogMiner session: %v", err)
		}
		lm.sessionActive = false
	}

	// Collect log files that contain changes from current SCN
	logFiles, err := lm.logCollector.GetLogs(startSCN)
	if err != nil {
		return fmt.Errorf("collecting redo logs for logminer: %w", err)
	}
	lm.log.Debugf("Collected %d redo log file(s) for LogMiner", len(logFiles))
	lm.currentLogFiles = logFiles

	// Load redo logs into LogMiner
	for i, logFile := range logFiles {
		// if first log file, ensure we clear existing logs
		isFirstFile := i == 0
		if err := lm.sessionMgr.AddLogFile(logFile.FileName, isFirstFile); err != nil {
			return fmt.Errorf("loading log filename '%s' into logminer: %w", logFile.FileName, err)
		}
		lm.log.Debugf("Loaded redo log file %s into LogMiner", logFile.FileName)
	}

	// Start LogMiner session with no end boundary (endSCN=0) for continuous mining
	if err := lm.sessionMgr.StartSession(startSCN, 0, false); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}
	lm.sessionActive = true
	lm.log.Infof("Started persistent LogMiner session from SCN %d (no end boundary)", startSCN)

	return nil
}
