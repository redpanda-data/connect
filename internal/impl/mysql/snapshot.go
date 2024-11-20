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

	"github.com/go-mysql-org/go-mysql/mysql"
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
	ctx    context.Context
}

// NewSnapshot creates new snapshot instance
func NewSnapshot(ctx context.Context, logger *service.Logger, db *sql.DB) *Snapshot {
	return &Snapshot{
		db:     db,
		ctx:    ctx,
		logger: logger,
	}
}

func (s *Snapshot) prepareSnapshot(ctx context.Context) (*mysql.Position, error) {
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

	// 1. Start a consistent snapshot transaction
	s.tx, err = s.snapshotConn.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %v", err)
	}

	// Execute START TRANSACTION WITH CONSISTENT SNAPSHOT
	if _, err := s.tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}

		return nil, fmt.Errorf("failed to start consistent snapshot: %v", err)
	}

	// 2. Acquire global read lock (minimizing lock time)
	if _, err := s.lockConn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}
		return nil, fmt.Errorf("failed to acquire global read lock: %v", err)
	}

	// 3. Get binary log position (while locked)
	pos, err := s.getCurrentBinlogPosition()
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

	// 4. Release the global read lock immediately
	if _, err := s.lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		if rErr := s.tx.Rollback(); rErr != nil {
			return nil, rErr
		}
		return nil, fmt.Errorf("failed to release global read lock: %v", err)
	}

	return &pos, nil
}

func (s *Snapshot) getRowsCount(table string) (int, error) {
	var count int
	if err := s.tx.QueryRowContext(s.ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

func (s *Snapshot) getTablePrimaryKeys(table string) ([]string, error) {
	// Get primary key columns for the table
	rows, err := s.tx.QueryContext(s.ctx, fmt.Sprintf(`
SELECT COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_NAME = '%s' AND CONSTRAINT_NAME = 'PRIMARY';
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

	return pks, nil
}

func (s *Snapshot) querySnapshotTable(table string, pk []string, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
	snapshotQueryParts := []string{
		"SELECT * FROM " + table,
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, s.buildOrderByClause(pk))

		snapshotQueryParts = append(snapshotQueryParts, "LIMIT ?")
		q := strings.Join(snapshotQueryParts, " ")
		s.logger.Infof("Querying snapshot: %s", q)
		return s.tx.QueryContext(s.ctx, strings.Join(snapshotQueryParts, " "), limit)
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
	return s.tx.QueryContext(s.ctx, q, lastSeenPkVals...)
}

func (s *Snapshot) buildOrderByClause(pk []string) string {
	if len(pk) == 1 {
		return "ORDER BY " + pk[0]
	}

	return "ORDER BY " + strings.Join(pk, ", ")
}

func (s *Snapshot) getCurrentBinlogPosition() (mysql.Position, error) {
	var (
		position uint32
		file     string
		// binlogDoDB, binlogIgnoreDB intentionally non-used
		// required to scan response
		binlogDoDB      interface{}
		binlogIgnoreDB  interface{}
		executedGtidSet interface{}
	)

	row := s.snapshotConn.QueryRowContext(context.Background(), "SHOW MASTER STATUS")
	if err := row.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet); err != nil {
		return mysql.Position{}, err
	}

	return mysql.Position{
		Name: file,
		Pos:  position,
	}, nil
}

func (s *Snapshot) releaseSnapshot(ctx context.Context) error {
	if s.tx != nil {
		if err := s.tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %v", err)
		}
	}

	if s.lockConn != nil {
		s.lockConn.Close()
	}

	if s.snapshotConn != nil {
		s.snapshotConn.Close()
	}

	return nil
}
