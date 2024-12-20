// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Snapshot represents a structure that prepares a transaction
// and creates mysql consistent snapshot inside the transaction
type Snapshot struct {
	db *sql.DB
	tx *sql.Tx

	lockConn     *sql.Conn
	snapshotConn *sql.Conn

	logger *service.Logger
}

// NewSnapshot creates new snapshot instance
func NewSnapshot(logger *service.Logger, db *sql.DB) *Snapshot {
	return &Snapshot{
		db:     db,
		logger: logger,
	}
}

func (s *Snapshot) prepareSnapshot(ctx context.Context) (*position, error) {
	var err error
	// Create a separate connection for FTWRL
	s.lockConn, err = s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock connection: %v", err)
	}

	// Create another connection for the snapshot
	s.snapshotConn, err = s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot connection: %v", err)
	}

	// Start a consistent snapshot transaction
	s.tx, err = s.snapshotConn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %v", err)
	}

	/*
		START TRANSACTION WITH CONSISTENT SNAPSHOT ensures a consistent view of database state
		when reading historical data during CDC initialization. Without it, concurrent writes
		could create inconsistencies between binlog position and table snapshots, potentially
		missing or duplicating events. The snapshot prevents other transactions from modifying
		the data being read, maintaining referential integrity across tables while capturing
		the initial state.
	*/
	if _, err := s.tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}

		return nil, fmt.Errorf("failed to start consistent snapshot: %v", err)
	}

	/*
		FLUSH TABLES WITH READ LOCK is executed after CONSISTENT SNAPSHOT to:
		1. Force MySQL to flush all data from memory to disk
		2. Prevent any writes to tables while we read the binlog position

		This lock MUST be released quickly to avoid blocking other connections. Only use it
		to capture the binlog coordinates, then release immediately with UNLOCK TABLES.
	*/
	if _, err := s.lockConn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}
		return nil, fmt.Errorf("failed to acquire global read lock: %v", err)
	}

	// Get binary log position (while locked)
	pos, err := s.getCurrentBinlogPosition(ctx)
	if err != nil {
		// Make sure to release the lock if we fail
		if _, eErr := s.lockConn.ExecContext(ctx, "UNLOCK TABLES"); eErr != nil {
			return nil, eErr
		}

		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}
		return nil, fmt.Errorf("failed to get binlog position: %v", err)
	}

	// Release the global read lock immediately after getting the binlog position
	if _, err := s.lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}
		return nil, fmt.Errorf("failed to release global read lock: %v", err)
	}

	return &pos, nil
}

func (s *Snapshot) getTablePrimaryKeys(ctx context.Context, table string) ([]string, error) {
	// Get primary key columns for the table
	rows, err := s.tx.QueryContext(ctx, fmt.Sprintf(`
SELECT COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_NAME = '%s' AND CONSTRAINT_NAME = 'PRIMARY'
ORDER BY ORDINAL_POSITION;
  `, table))
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key: %v", err)
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

	if len(pks) == 0 {
		return nil, fmt.Errorf("unable to find primary key for table %s - does the table exist and does it have a primary key set?", table)
	}

	return pks, nil
}

func (s *Snapshot) querySnapshotTable(ctx context.Context, table string, pk []string, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
	snapshotQueryParts := []string{
		"SELECT * FROM " + table,
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, s.buildOrderByClause(pk))

		snapshotQueryParts = append(snapshotQueryParts, "LIMIT ?")
		q := strings.Join(snapshotQueryParts, " ")
		s.logger.Infof("Querying snapshot: %s", q)
		return s.tx.QueryContext(ctx, strings.Join(snapshotQueryParts, " "), limit)
	}

	var lastSeenPkVals []any
	var placeholders []string
	for _, pkCol := range *lastSeenPkVal {
		lastSeenPkVals = append(lastSeenPkVals, pkCol)
		placeholders = append(placeholders, "?")
	}

	snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("WHERE (%s) > (%s)", strings.Join(pk, ", "), strings.Join(placeholders, ", ")))
	snapshotQueryParts = append(snapshotQueryParts, s.buildOrderByClause(pk))
	snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("LIMIT %d", limit))
	q := strings.Join(snapshotQueryParts, " ")
	s.logger.Infof("Querying snapshot: %s", q)
	return s.tx.QueryContext(ctx, q, lastSeenPkVals...)
}

func (s *Snapshot) buildOrderByClause(pk []string) string {
	if len(pk) == 1 {
		return "ORDER BY " + pk[0]
	}

	return "ORDER BY " + strings.Join(pk, ", ")
}

func (s *Snapshot) getCurrentBinlogPosition(ctx context.Context) (position, error) {
	var (
		offset uint32
		file   string
		// binlogDoDB, binlogIgnoreDB intentionally non-used
		// required to scan response
		binlogDoDB      any
		binlogIgnoreDB  any
		executedGtidSet any
	)

	row := s.snapshotConn.QueryRowContext(ctx, "SHOW MASTER STATUS")
	if err := row.Scan(&file, &offset, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
		return position{}, err
	}

	return position{
		Name: file,
		Pos:  offset,
	}, nil
}

func (s *Snapshot) releaseSnapshot(_ context.Context) error {
	if s.tx != nil {
		if err := s.tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %v", err)
		}
	}

	// reset transaction
	s.tx = nil
	return nil
}

func (s *Snapshot) close() error {
	if s.tx != nil {
		if err := s.tx.Rollback(); err != nil {
			return fmt.Errorf("unable to rollback transaction: %w", err)
		}
		s.tx = nil
	}
	for _, conn := range []*sql.Conn{s.lockConn, s.snapshotConn} {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil {
			return fmt.Errorf("unable to close connection: %w", err)
		}
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("unable to close db: %w", err)
	}
	return nil
}
