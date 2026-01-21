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
	"fmt"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/dmlparser"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// ChangeEvent represents the final change event emitted to Kafka
type ChangeEvent struct {
	Schema    string
	Table     string
	Operation string // "CREATE", "UPDATE", "DELETE"
	// Before    map[string]any
	// After     map[string]any
	Data      map[string]any
	SCN       int64
	Timestamp time.Time
	TxnID     string
}

// LogMinerEvent represents a row from V$LOGMNR_CONTENTS
type LogMinerEvent struct {
	SCN           int64
	SQLRedo       sql.NullString
	SQLUndo       sql.NullString
	Data          map[string]any
	Operation     Operation
	OperationCode int
	TableName     sql.NullString
	SchemaName    sql.NullString
	Timestamp     time.Time
	TransactionID string
}

// LogMiner tracks and streams all change events from the configured change
// tables tracked in tables.
type LogMiner struct {
	tables          []replication.UserDefinedTable
	backoffInterval time.Duration
	publisher       replication.ChangePublisher
	log             *service.Logger
	logCollector    *LogFileCollector
	currentSCN      uint64
	BatchSize       int
	sessionMgr      *SessionManager
	eventProc       *EventProcessor
	db              *sql.DB
	SleepDuration   time.Duration
	dmlParser       *dmlparser.LogMinerDMLParser

	// Pre-built query string for LogMiner contents
	logMinerQuery string
	txnCache      TransactionCache
}

// NewMiner creates a new instance of NewMiner, responsible
// for paging through change events based on the tables param.
func NewMiner(db *sql.DB, userTables []replication.UserDefinedTable, publisher replication.ChangePublisher, backoffInterval time.Duration, maxBatchSize int, logger *service.Logger) *LogMiner {
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
			SQL_UNDO,
			OPERATION_CODE,
			TABLE_NAME,
			SEG_OWNER,
			TIMESTAMP,
			XID,
			XIDUSN,
			XIDSLT,
			XIDSQN,
			RS_ID,
			SSN
		FROM V$LOGMNR_CONTENTS
		WHERE SCN >= :1 AND SCN < :2%s
		ORDER BY SCN, SSN
	`, tableFilter.String())

	lm := &LogMiner{
		log:             logger,
		db:              db,
		tables:          userTables,
		publisher:       publisher,
		backoffInterval: backoffInterval,
		BatchSize:       maxBatchSize,
		logCollector:    NewLogFileCollector(db),
		sessionMgr:      NewSessionManager(db),
		eventProc:       NewEventProcessor(),
		txnCache:        NewInMemoryCache(),
		dmlParser:       dmlparser.New(true),
		logMinerQuery:   logMinerQuery,
	}
	return lm
}

// ReadChanges streams the change events from the configured SQL Server change tables.
func (lm *LogMiner) ReadChanges(ctx context.Context, db *sql.DB, startPos replication.SCN) error {
	lm.log.Infof("Starting streaming of %d change table(s)", len(lm.tables))

	// Determine starting SCN
	if len(startPos) != 0 {
		// Resume from checkpoint
		lm.log.Infof("Resuming from recorded SCN position '%s'", startPos)
		// Parse startPos to uint64
		// TODO: Parse startPos string to uint64
	} else {
		// get current SCN from DB
		var scn uint64
		if err := lm.db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&scn); err != nil {
			return fmt.Errorf("fetching current SCN: %w", err)
		}
		lm.currentSCN = scn
		lm.log.Infof("Starting from current SCN: %d", lm.currentSCN)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := lm.miningCycle(ctx); err != nil {
				return fmt.Errorf("mining logs: %w", err)
			}

			time.Sleep(lm.backoffInterval)
		}
	}
}

// emitChangeEvent sends a change event to the output (Kafka, etc.)
func (lm *LogMiner) emitChangeEvent(ctx context.Context, event *ChangeEvent) {
	msg := replication.MessageEvent{
		// SCN:       event.SCN,
		Operation: event.Operation,
		Schema:    event.Schema,
		Table:     event.Table,
		Data:      event.Data,
	}
	lm.publisher.Publish(ctx, msg)
}

func (lm *LogMiner) miningCycle(ctx context.Context) error {
	// 1. Get the database's current SCN to know our target
	var dbCurrentSCN uint64
	if err := lm.db.QueryRow("SELECT CURRENT_SCN FROM V$DATABASE").Scan(&dbCurrentSCN); err != nil {
		return fmt.Errorf("fetching current SCN: %w", err)
	}

	// If we've caught up, nothing to do
	if lm.currentSCN >= dbCurrentSCN {
		lm.log.Infof("Caught up to current SCN %d, no new changes to process", dbCurrentSCN)
		return nil
	}

	// 2. Calculate SCN range - process up to current SCN or a reasonable chunk
	endSCN := dbCurrentSCN
	// Limit the range to avoid huge queries when there's a large gap
	maxRange := uint64(lm.BatchSize)
	if endSCN-lm.currentSCN > maxRange {
		endSCN = lm.currentSCN + maxRange
	}

	// 3. Collect log files that contain changes from current SCN
	logFiles, err := lm.logCollector.GetLogs(lm.currentSCN)
	if err != nil {
		return fmt.Errorf("collecting redo logs for logminer: %w", err)
	}
	lm.log.Debugf("Collected %d redo log file(s) for LogMiner", len(logFiles))

	// 4. Load redo logs into LogMiner
	for i, logFile := range logFiles {
		// if first log file, ensure we clear existing logs
		isFirstFile := i == 0
		if err := lm.sessionMgr.AddLogFile(logFile.FileName, isFirstFile); err != nil {
			return fmt.Errorf("loading log filename '%s' into logminer: %w", logFile.FileName, err)
		}

		lm.log.Debugf("Loaded redo log file %s into LogMiner", logFile.FileName)
	}

	// 4. Start LogMiner session with ONLINE_CATALOG strategy
	if err := lm.sessionMgr.StartSession(lm.currentSCN, endSCN, false); err != nil {
		return fmt.Errorf("starting logminer session: %w", err)
	}
	defer lm.sessionMgr.EndSession()
	lm.log.Debugf("Started LogMiner session: SCN %d to %d", lm.currentSCN, endSCN)

	// 5. Query and process events from V$LOGMNR_CONTENTS
	events, err := lm.queryLogMinerContents(lm.currentSCN, endSCN)
	if err != nil {
		return fmt.Errorf("querying logminer contents: %w", err)
	}

	// 6. Process events and buffer transactions
	for _, event := range events {
		if err := lm.processEvent(ctx, event); err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}
	}

	lm.log.Debugf("Found and processed %d events in SCN range %d - %d", len(events), lm.currentSCN, endSCN)
	lm.currentSCN = endSCN

	return nil
}

// processEvent buffers emitted events until a commit or rollback event is processed at which
// point the buffer can be flushed to the Connect pipeline or dropped.
func (lm *LogMiner) processEvent(ctx context.Context, event *LogMinerEvent) error {
	switch event.Operation {
	case OpStart:
		// Transaction started
		lm.txnCache.StartTransaction(event.TransactionID, event.SCN)

	case OpInsert, OpUpdate, OpDelete:
		// parse sql insert/update/delete statements into key/value object
		if event.SQLRedo.Valid {
			data, err := lm.dmlParser.Parse(event.SQLRedo.String)
			if err != nil {
				return fmt.Errorf("parsing sql query into object: %w", err)
			}
			event.Data = data.NewValues
		}

		dmlEvent, err := lm.eventProc.ParseDML(event)
		if err != nil {
			return fmt.Errorf("failed to parse DML event: %w", err)
		}

		// Buffer DML events in transaction
		lm.txnCache.AddEvent(event.TransactionID, dmlEvent)
	case OpCommit:
		// Flush all buffered events for this transaction
		if txn := lm.txnCache.GetTransaction(event.TransactionID); txn != nil {
			for _, dmlEvent := range txn.Events {
				changeEvent := lm.eventProc.ConvertToChangeEvent(dmlEvent, event.SCN)
				// TODO: Refactor to use replication.ChangeEvent
				lm.emitChangeEvent(ctx, changeEvent)
			}
			lm.txnCache.CommitTransaction(event.TransactionID)
			lm.log.Debugf("Committed transaction %s with %d events at SCN %d", event.TransactionID, len(txn.Events), event.SCN)
		}

	case OpRollback:
		// Discard all buffered events for this transaction
		lm.log.Debugf("Discarding transaction due to rollback")
		lm.txnCache.RollbackTransaction(event.TransactionID)
	}

	return nil
}

func (lm *LogMiner) queryLogMinerContents(startSCN, endSCN uint64) ([]*LogMinerEvent, error) {
	// Use the pre-built query from initialization
	rows, err := lm.db.Query(lm.logMinerQuery, startSCN, endSCN)
	if err != nil {
		return nil, fmt.Errorf("querying logminer: %w", err)
	}
	defer rows.Close()

	var events []*LogMinerEvent
	for rows.Next() {
		event := &LogMinerEvent{}
		var xidUsn, xidSlt, xidSqn int64
		var xid string
		var rsId, ssn sql.NullString

		err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.SQLUndo,
			&event.OperationCode,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&xid,
			&xidUsn,
			&xidSlt,
			&xidSqn,
			&rsId,
			&ssn,
		)
		if err != nil {
			return nil, err
		}

		// Construct transaction ID from components (XID as RAW doesn't format well)
		event.TransactionID = fmt.Sprintf("%d.%d.%d", xidUsn, xidSlt, xidSqn)
		event.Operation = operationFromCode(event.OperationCode)
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

func NewLogFileCollector(db *sql.DB) *LogFileCollector {
	return &LogFileCollector{db: db}
}

// GetLogs collects all log files containing changes from the given SCN
func (lfc *LogFileCollector) GetLogs(offsetSCN uint64) ([]*LogFile, error) {
	query := `
		SELECT FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD
		FROM (
			-- Current online redo logs
			SELECT
				MIN(F.MEMBER) AS FILE_NAME,
				L.FIRST_CHANGE# FIRST_CHANGE,
				L.NEXT_CHANGE# NEXT_CHANGE,
				L.SEQUENCE# AS SEQ,
				'ONLINE' AS TYPE,
				L.THREAD# AS THREAD
			FROM V$LOGFILE F, V$LOG L
			WHERE L.STATUS = 'CURRENT'
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
		return nil, err
	}
	defer rows.Close()

	var logFiles []*LogFile
	for rows.Next() {
		lf := &LogFile{}
		err := rows.Scan(&lf.FileName, &lf.FirstSCN, &lf.NextSCN, &lf.Sequence, &lf.Type, &lf.Thread)
		if err != nil {
			return nil, err
		}
		lf.IsCurrent = lf.Type == "ONLINE"
		logFiles = append(logFiles, lf)
	}

	return logFiles, rows.Err()
}
