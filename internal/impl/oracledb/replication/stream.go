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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/confx"
)

// change represents a logical change row from the change table.
type change struct {
	startSCN   SCN // varbinary(10)
	endSCN     SCN // varbinary(10)
	operation  OpType
	updateMask []byte
	seqVal     []byte
	commandID  int
	columns    map[string]any
}

func (c *change) reset() {
	if c != nil {
		for k := range c.columns {
			delete(c.columns, k)
		}
		c.startSCN = nil
		c.endSCN = nil
		c.updateMask = nil
		c.seqVal = nil
		c.operation = 0
		c.commandID = 0
	}
}

// mapScannedValue takes an already-scanned value and column type, and converts it
// to the appropriate Go type for JSON marshaling.
func mapScannedValue(val any, colType *sql.ColumnType) any {
	if val == nil {
		return nil
	}

	switch colType.DatabaseTypeName() {
	// Decimals come as []byte from the driver, convert to json.Number to preserve precision
	case "DECIMAL", "NUMERIC":
		if b, ok := val.([]byte); ok {
			return json.Number(string(b))
		}
	}

	return val
}

// ChangePublisher is responsible for handling and processing of a replication.MessageEvent.
type ChangePublisher interface {
	Publish(ctx context.Context, msg MessageEvent) error
}

// UserDefinedTable represents a found user's SQL Server table (called a user-defined table) in SQL.
type UserDefinedTable struct {
	Schema        string
	Name          string
	LogGroupTypes map[string]string
	startSCN      SCN
}

// ToChangeTable returns a string in the SQL Server change table format of cdc.<schema>_<tablename>_CT.
func (t *UserDefinedTable) ToChangeTable() string {
	return t.FullName()
}

// FullName returns a string of the table name including the schema (ie dbo.<tablename>).
func (t *UserDefinedTable) FullName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

// VerifyUserDefinedTables verifies underlying user defined tables based on supplied
// include and exclude filters, validating change tracking is enabled.
func VerifyUserDefinedTables(ctx context.Context, db *sql.DB, tableFilter *confx.RegexpFilter, log *service.Logger) ([]UserDefinedTable, error) {
	sql := `
	SELECT OWNER AS SchemeName, TABLE_NAME AS TableName
	FROM DBA_TABLES
	WHERE OWNER NOT IN ('SYS', 'SYSTEM', 'OUTLN', 'DBSNMP', 'APPQOSSYS', 'DBSFWUSER', 'GGSYS', 'ANONYMOUS', 'CTXSYS', 'DVSYS', 'DVF', 'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OJVMSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'WMSYS', 'XDB')
	ORDER BY OWNER, TABLE_NAME`
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("fetching user defined tables from user_tables for verification: %w", err)
	}

	var userTables []UserDefinedTable
	for rows.Next() {
		var ut UserDefinedTable
		if err := rows.Scan(&ut.Schema, &ut.Name); err != nil {
			return nil, fmt.Errorf("scanning user_tables row for user defined tables: %w", err)
		}
		if tableFilter.Matches(fmt.Sprintf("%s.%s", ut.Schema, ut.Name)) {
			userTables = append(userTables, ut)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating through user_tables for user defined tables: %w", err)
	}

	if len(userTables) == 0 {
		return nil, errors.New("no user defined tables found for given include and exclude filters")
	}

	// perform a simple check that the tables are tracked, we could verify what columns are tracked but a simple check feels sufficient.
	for i, tbl := range userTables {
		var logGroupsCnt int
		if err = db.QueryRow(`SELECT COUNT(*) FROM ALL_LOG_GROUPS WHERE OWNER = :1 AND TABLE_NAME = :2`, tbl.Schema, tbl.Name).Scan(&logGroupsCnt); err != nil {
			return nil, fmt.Errorf("querying log groups for table '%s': %w", tbl.FullName(), err)
		}
		if logGroupsCnt == 0 {
			return nil, fmt.Errorf("supplemental logging not enabled for table '%s' - no log groups found", tbl.FullName())
		}
		userTables[i] = tbl
	}

	for _, t := range userTables {
		log.Infof("Found table '%s'", t.FullName())
	}

	return userTables, nil
}
