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
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Snapshot represents a structure that prepares a transaction
// and creates mysql consistent snapshot inside the transaction
type Snapshot struct {
	db          *sql.DB
	lockConn    *sql.Conn
	workerConns []*sql.Conn
	// workerTxs holds one consistent-snapshot transaction per worker, established
	// inside the FLUSH TABLES WITH READ LOCK window so all readers see identical data.
	// Workers pull tables from a queue and reuse their transaction across multiple tables.
	workerTxs []*sql.Tx

	logger *service.Logger
}

// NewSnapshot creates new snapshot instance.
func NewSnapshot(logger *service.Logger, db *sql.DB) *Snapshot {
	return &Snapshot{
		db:     db,
		logger: logger,
	}
}

func (s *Snapshot) prepareSnapshot(ctx context.Context, tables []string, maxWorkers int) (*position, error) {
	if len(tables) == 0 {
		return nil, errors.New("no tables provided")
	}

	var err error
	s.lockConn, err = s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("create lock connection: %v", err)
	}

	/*
		FLUSH TABLES WITH READ LOCK prevents any writes to tables while we:
		  1. Establish consistent-snapshot transactions on all worker connections
		  2. Read the binlog position

		The lock MUST be released quickly to avoid blocking other connections.

		See https://dev.mysql.com/doc/refman/8.4/en/flush.html#flush-tables
	*/
	lockQuery := buildFlushAndLockTablesQuery(tables)
	s.logger.Infof("Acquiring table-level read locks with: %s", lockQuery)
	if _, err := s.lockConn.ExecContext(ctx, lockQuery); err != nil {
		return nil, fmt.Errorf("acquire table-level read locks: %w", err)
	}

	unlockTables := func() error {
		if _, err := s.lockConn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			return fmt.Errorf("release table-level read locks: %w", err)
		}
		return nil
	}

	// Open one transaction per worker (up to min(maxWorkers, len(tables))) while
	// the read lock is still held. This guarantees all workers see the exact same
	// database state. Workers pull tables from a queue and reuse their transaction
	// across multiple tables — START TRANSACTION WITH CONSISTENT SNAPSHOT
	// establishes a read view of the entire database, not a single table, so a
	// single transaction can safely read multiple tables at the same snapshot point.
	//
	// NOTE: BeginTx is called first to obtain a *sql.Tx handle, then
	// START TRANSACTION WITH CONSISTENT SNAPSHOT is executed on it, which
	// implicitly replaces the transaction with a consistent-snapshot one.
	// The go-sql-driver/mysql driver does not expose this directly via TxOptions.
	numWorkers := min(maxWorkers, len(tables))
	s.workerConns = make([]*sql.Conn, 0, numWorkers)
	s.workerTxs = make([]*sql.Tx, 0, numWorkers)
	for range numWorkers {
		conn, connErr := s.db.Conn(ctx)
		if connErr != nil {
			return nil, errors.Join(
				fmt.Errorf("open worker connection: %w", connErr),
				unlockTables())
		}
		s.workerConns = append(s.workerConns, conn)
		tx, txErr := conn.BeginTx(ctx, &sql.TxOptions{
			ReadOnly:  true,
			Isolation: sql.LevelRepeatableRead,
		})
		if txErr != nil {
			return nil, errors.Join(
				fmt.Errorf("start worker transaction: %w", txErr),
				unlockTables())
		}
		s.workerTxs = append(s.workerTxs, tx)
		if _, txErr = tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); txErr != nil {
			return nil, errors.Join(
				fmt.Errorf("start consistent snapshot for worker: %w", txErr),
				unlockTables())
		}
	}

	// Capture the binlog position while the read lock is still held.
	pos, err := s.getCurrentBinlogPosition(ctx)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("get binlog position: %w", err),
			unlockTables())
	}

	// Release the table locks immediately after capturing the binlog position.
	if err := unlockTables(); err != nil {
		return nil, err
	}

	return &pos, nil
}

func buildFlushAndLockTablesQuery(tables []string) string {
	var sb strings.Builder
	sb.WriteString("FLUSH TABLES ")
	for i, table := range tables {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "`%s`", table)
	}
	sb.WriteString(" WITH READ LOCK")
	return sb.String()
}

func (*Snapshot) getTablePrimaryKeys(ctx context.Context, tx *sql.Tx, table string) ([]string, error) {
	pkSql := `
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE TABLE_NAME = '%s' AND CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = DATABASE()
ORDER BY ORDINAL_POSITION
`

	rows, err := tx.QueryContext(ctx, fmt.Sprintf(pkSql, table))
	if err != nil {
		return nil, fmt.Errorf("get primary key: %v", err)
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
		return nil, fmt.Errorf("iterate table: %s", err)
	}

	if len(pks) == 0 {
		return nil, fmt.Errorf("unable to find primary key for table %s - does the table exist and does it have a primary key set?", table)
	}

	return pks, nil
}

func (s *Snapshot) querySnapshotTable(ctx context.Context, tx *sql.Tx, table string, pk []string, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
	snapshotQueryParts := []string{
		"SELECT * FROM " + table,
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
		snapshotQueryParts = append(snapshotQueryParts, "LIMIT ?")
		q := strings.Join(snapshotQueryParts, " ")
		s.logger.Debugf("Querying snapshot: %s", q)
		return tx.QueryContext(ctx, q, limit)
	}

	var lastSeenPkVals []any
	var placeholders []string
	for _, pkCol := range pk {
		val, ok := (*lastSeenPkVal)[pkCol]
		if !ok {
			return nil, fmt.Errorf("primary key column '%s' not found in last seen values", pkCol)
		}
		lastSeenPkVals = append(lastSeenPkVals, val)
		placeholders = append(placeholders, "?")
	}

	snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("WHERE (%s) > (%s)", strings.Join(pk, ", "), strings.Join(placeholders, ", ")))
	snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
	snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("LIMIT %d", limit))
	q := strings.Join(snapshotQueryParts, " ")
	s.logger.Debugf("Querying snapshot: %s", q)
	return tx.QueryContext(ctx, q, lastSeenPkVals...)
}

func buildOrderByClause(pk []string) string {
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

	scanRow := func(row *sql.Row) error {
		return row.Scan(&file, &offset, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet)
	}

	// "SHOW BINARY LOG STATUS" replaces "SHOW MASTER STATUS" IN MySQL 8.4+
	if err := scanRow(s.lockConn.QueryRowContext(ctx, "SHOW BINARY LOG STATUS")); err != nil {
		if err = scanRow(s.lockConn.QueryRowContext(ctx, "SHOW MASTER STATUS")); err != nil {
			return position{}, err
		}
	}

	return position{
		Name: file,
		Pos:  offset,
	}, nil
}

func (s *Snapshot) releaseSnapshot(_ context.Context) error {
	var errs []error
	for idx, tx := range s.workerTxs {
		if err := tx.Commit(); err != nil {
			errs = append(errs, fmt.Errorf("commit transaction for worker %d: %w", idx, err))
		}
	}
	s.workerTxs = nil
	for idx, conn := range s.workerConns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close worker connection %d: %w", idx, err))
			}
		}
	}
	s.workerConns = nil
	return errors.Join(errs...)
}

func (s *Snapshot) close() error {
	var errs []error

	for _, tx := range s.workerTxs {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			errs = append(errs, fmt.Errorf("rollback transaction: %w", err))
		}
	}
	s.workerTxs = nil
	for idx, conn := range s.workerConns {
		if conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close worker connection %d: %w", idx, err))
			}
		}
	}
	s.workerConns = nil

	if s.lockConn != nil {
		if err := s.lockConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close lock connection: %w", err))
		}
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db: %w", err))
		}
	}

	return errors.Join(errs...)
}
