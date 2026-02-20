// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"
)

// ChangePublisher is responsible for handling and processing of a replication.MessageEvent.
type ChangePublisher interface {
	Publish(ctx context.Context, msg *MessageEvent) error
	Close()
}

// UserTable represents a found user's OracleDB table (called a user-table).
type UserTable struct {
	Schema string
	Name   string
}

// FullName returns a string of the table name including the schema (ie <schemaname>.<tablename>).
func (t *UserTable) FullName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

// VerifyUserTables verifies underlying user tables based on supplied
// include and exclude filters, validating change tracking is enabled.
func VerifyUserTables(ctx context.Context, db *sql.DB, tableFilter *confx.RegexpFilter, log *service.Logger) ([]UserTable, error) {
	sql := `
	SELECT OWNER AS SchemeName, TABLE_NAME AS TableName
	FROM DBA_TABLES
	WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER', 'GGSYS', 'ANONYMOUS', 'CTXSYS', 'DVSYS', 'DVF', 'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'WMSYS', 'XDB')
	ORDER BY OWNER, TABLE_NAME`
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("fetching user tables from dba_tables for verification: %w", err)
	}
	defer rows.Close()

	var userTables []UserTable
	for rows.Next() {
		var ut UserTable
		if err := rows.Scan(&ut.Schema, &ut.Name); err != nil {
			return nil, fmt.Errorf("scanning dba_tables row for user tables: %w", err)
		}
		if tableFilter.Matches(fmt.Sprintf("%s.%s", ut.Schema, ut.Name)) {
			userTables = append(userTables, ut)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating through dba_tables for user tables: %w", err)
	}

	if len(userTables) == 0 {
		return nil, errors.New("no user tables found for given include and exclude filters")
	}

	// perform a simple check that the tables are tracked, we could verify what columns are tracked but a simple check feels sufficient.
	for i, tbl := range userTables {
		var logGroupsCnt int
		if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ALL_LOG_GROUPS WHERE OWNER = :1 AND TABLE_NAME = :2`, tbl.Schema, tbl.Name).Scan(&logGroupsCnt); err != nil {
			return nil, fmt.Errorf("querying log groups for table '%s': %w", tbl.FullName(), err)
		}
		if logGroupsCnt == 0 {
			return nil, fmt.Errorf("supplemental logging not enabled for table '%s' - no log groups found", tbl.FullName())
		}
		userTables[i] = tbl
	}

	for _, t := range userTables {
		log.Infof("Found user table '%s'", t.FullName())
	}

	return userTables, nil
}

// ApplyNLSSettings ensures consistent datetime formatting for connection session.
// This is important for reading redo_logs and ensures consistency with snapshotting.
func ApplyNLSSettings(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"); err != nil {
		return fmt.Errorf("setting NLS_DATE_FORMAT: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'"); err != nil {
		return fmt.Errorf("setting NLS_TIMESTAMP_FORMAT: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'"); err != nil {
		return fmt.Errorf("setting NLS_TIMESTAMP_TZ_FORMAT: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'"); err != nil {
		return fmt.Errorf("setting NLS_NUMERIC_CHARACTERS: %w", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET TIME_ZONE = '00:00'"); err != nil {
		return fmt.Errorf("setting session timezone: %w", err)
	}
	return nil
}
