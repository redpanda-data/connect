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
	"log"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// ChangeEvent represents the final change event emitted to Kafka
type ChangeEvent struct {
	Schema    string
	Table     string
	Operation string // "CREATE", "UPDATE", "DELETE"
	Before    map[string]any
	After     map[string]any
	SCN       int64
	Timestamp time.Time
	TxnID     string
}

// LogMinerEvent represents a row from V$LOGMNR_CONTENTS
type LogMinerEvent struct {
	SCN           int64
	SQLRedo       string
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
	BatchSize       uint8
	sessionMgr      *SessionManager
	eventProc       *EventProcessor
	db              *sql.DB
	SleepDuration   time.Duration

	txnCache *TransactionCache
}

// NewMiner creates a new instance of NewMiner, responsible
// for paging through change events based on the tables param.
func NewMiner(db *sql.DB, tables []replication.UserDefinedTable, publisher replication.ChangePublisher, backoffInterval time.Duration, logger *service.Logger) *LogMiner {
	s := &LogMiner{
		tables:          tables,
		publisher:       publisher,
		backoffInterval: backoffInterval,
		logCollector:    NewLogFileCollector(db),
		sessionMgr:      NewSessionManager(db),
		db:              db,
		SleepDuration:   5 * time.Second,
		log:             logger,
	}
	return s
}

// ReadChanges streams the change events from the configured SQL Server change tables.
func (lm *LogMiner) ReadChanges(ctx context.Context, db *sql.DB, startPos replication.SCN) error {
	lm.log.Infof("Starting streaming %d change table(s)", len(lm.tables))
	var (
	// startLSN replication.SCN // load last checkpoint; nil means start from beginning in tables
	// endLSN   replication.SCN // often set to fn_cdc_get_max_lsn(); nil means no upper bound
	// lastLSN  replication.SCN
	)

	if len(startPos) != 0 {
		// startLSN = startPos
		// lastLSN = startPos
		lm.log.Infof("Resuming from recorded LSN position '%s'", startPos)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := lm.miningCycle(ctx); err != nil {
				log.Printf("Mining cycle error: %v", err)
				return err
			}
			time.Sleep(lm.SleepDuration)
		}
	}
}

// emitChangeEvent sends a change event to the output (Kafka, etc.)
func (c *LogMiner) emitChangeEvent(event *ChangeEvent) {
	// In real implementation, this would send to Kafka
	log.Printf("EMIT: %s on %s.%s at SCN %d",
		event.Operation, event.Schema, event.Table, event.SCN)
}

func (lm *LogMiner) miningCycle(_ context.Context) error { // 1. Collect log files that contain changes from current SCN
	logFiles, err := lm.logCollector.GetLogs(lm.currentSCN)
	if err != nil {
		return fmt.Errorf("failed to collect logs: %w", err)
	}
	log.Printf("Collected %d log files for mining", len(logFiles))

	// 2. Calculate SCN range for this batch
	endSCN := lm.currentSCN + uint64(lm.BatchSize)

	// 3. Register log files with LogMiner
	if err := lm.sessionMgr.RemoveAllLogFiles(); err != nil {
		return fmt.Errorf("failed to remove old log files: %w", err)
	}

	// for _, logFile := range logFiles {
	// 	if err := lm.sessionMgr.AddLogFile(logFile.FileName); err != nil {
	// 		return fmt.Errorf("failed to add log file %s: %w", logFile.FileName, err)
	// 	}
	// }
	for i, logFile := range logFiles {
		if err := lm.sessionMgr.AddLogFile(logFile.FileName, i == 0); err != nil {
			return fmt.Errorf("failed to add log file %s: %w", logFile.FileName, err)
		}
	}

	// 4. Start LogMiner session with ONLINE_CATALOG strategy
	if err := lm.sessionMgr.StartSession(lm.currentSCN, endSCN, false); err != nil {
		return fmt.Errorf("failed to start LogMiner session: %w", err)
	}
	defer lm.sessionMgr.EndSession()

	// 5. Query and process events from V$LOGMNR_CONTENTS
	events, err := lm.queryLogMinerContents(lm.currentSCN, endSCN)
	if err != nil {
		return fmt.Errorf("failed to query LogMiner contents: %w", err)
	}

	// 6. Process events and buffer transactions
	for _, event := range events {
		if err := lm.processEvent(event); err != nil {
			return fmt.Errorf("failed to process event: %w", err)
		}
	}

	log.Printf("Processed %d events in SCN range %d-%d", len(events), lm.currentSCN, endSCN)
	lm.currentSCN = endSCN

	return nil
}

func (lm *LogMiner) processEvent(event *LogMinerEvent) error {
	switch event.Operation {
	case OpStart:
		// Transaction started
		lm.txnCache.StartTransaction(event.TransactionID, event.SCN)

	case OpInsert, OpUpdate, OpDelete:
		// Buffer DML event in transaction
		dmlEvent, err := lm.eventProc.ParseDML(event)
		if err != nil {
			return fmt.Errorf("failed to parse DML: %w", err)
		}
		lm.txnCache.AddEvent(event.TransactionID, dmlEvent)

	case OpCommit:
		// Emit all buffered events for this transaction
		txn := lm.txnCache.GetTransaction(event.TransactionID)
		if txn != nil {
			for _, dmlEvent := range txn.Events {
				changeEvent := lm.eventProc.ConvertToChangeEvent(dmlEvent, event.SCN)
				lm.emitChangeEvent(changeEvent)
			}
			lm.txnCache.CommitTransaction(event.TransactionID)
			log.Printf("Committed transaction %s with %d events at SCN %d",
				event.TransactionID, len(txn.Events), event.SCN)
		}

	case OpRollback:
		// Discard all buffered events for this transaction
		lm.txnCache.RollbackTransaction(event.TransactionID)
	}

	return nil
}

func (lm *LogMiner) queryLogMinerContents(startSCN, endSCN uint64) ([]*LogMinerEvent, error) {
	query := `
		SELECT
			SCN,
			SQL_REDO,
			OPERATION_CODE,
			TABLE_NAME,
			SEG_OWNER,
			TIMESTAMP,
			XID,
			XIDUSN,
			XIDSLOT,
			XIDSQN,
			RS_ID,
			SSN
		FROM V$LOGMNR_CONTENTS
		WHERE SCN >= :1 AND SCN < :2
		AND (
			OPERATION_CODE IN (1, 2, 3, 6, 7, 36)  -- INSERT, DELETE, UPDATE, START, COMMIT, ROLLBACK
		)
		ORDER BY SCN, SSN
	`

	rows, err := lm.db.Query(query, startSCN, endSCN)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*LogMinerEvent
	for rows.Next() {
		event := &LogMinerEvent{}
		var xidUsn, xidSlot, xidSqn int64
		var rsId, ssn sql.NullString

		err := rows.Scan(
			&event.SCN,
			&event.SQLRedo,
			&event.OperationCode,
			&event.TableName,
			&event.SchemaName,
			&event.Timestamp,
			&event.TransactionID,
			&xidUsn,
			&xidSlot,
			&xidSqn,
			&rsId,
			&ssn,
		)
		if err != nil {
			return nil, err
		}

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
