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

// changeTableRowIter is responsible for handling the iteration of change table records, row by row.
// It moves to the next row, sorts them by min-heap based on LSN ordering criteria,
// parses the data and sends it for processing.
type changeTableRowIter struct {
	table    UserDefinedTable
	rows     *sql.Rows
	cols     []string
	colTypes []*sql.ColumnType
	current  *change
	log      *service.Logger

	vals []any
}

// newChangeTableRowIter returns an custom row iterator for the given changeTable.
func newChangeTableRowIter(
	ctx context.Context,
	db *sql.DB,
	changeTable UserDefinedTable,
	fromLSN, toSCN SCN,
	logger *service.Logger,
) (*changeTableRowIter, error) {
	// Note: LSN is varbinary type so can sort correctly for LSNs
	// Inspired by Debezium https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerConnection.java?plain=1#L177

	// "Sequence of the operation as represented in the transaction log. Should not be used for ordering. Instead, use the __$command_id column"
	// source: https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17
	q := fmt.Sprintf("SELECT * FROM %s WITH (NOLOCK) WHERE (? IS NULL OR [__$start_lsn] > ?) AND (? IS NULL OR [__$start_lsn] <= ?) ORDER BY [__$start_lsn] ASC, [__$command_id] ASC, [__$operation] ASC", changeTable.ToChangeTable())
	rows, err := db.QueryContext(ctx, q, fromLSN, fromLSN, toSCN, toSCN) //nolint:rowserrcheck
	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, err
	}

	// pre-allocate slice of pointers for sql.Scan operations
	vals := make([]any, len(cols))
	for i := range vals {
		var v any
		vals[i] = &v
	}

	iter := &changeTableRowIter{
		table:    changeTable,
		rows:     rows,
		cols:     cols,
		colTypes: colTypes,
		vals:     vals,
		log:      logger,
	}
	// Prime the iterator by loading the first row
	if err := iter.next(); err != nil {
		// Already exhausted iterator
		closeErr := iter.Close()
		return nil, errors.Join(err, closeErr)
	}

	return iter, nil
}

func (ct *changeTableRowIter) next() error {
	if !ct.rows.Next() {
		// consult iterator error result before we can infer it's due to no rows.
		if err := ct.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	// read row into ct.vals, reusing pre-allocated slice of pointer
	if err := ct.rows.Scan(ct.vals...); err != nil {
		return err
	}

	if ct.current == nil {
		ct.current = &change{columns: make(map[string]any, len(ct.cols))}
	} else {
		ct.current.reset()
	}

	if err := ct.mapValsToChange(ct.vals, ct.current); err != nil {
		return fmt.Errorf("mapping change table columns to iterator row: %w", err)
	}

	return nil
}

func (ct *changeTableRowIter) Close() error {
	return ct.rows.Close()
}

// mapValsToChange maps the values from vals to the dst out parameter.
func (ct *changeTableRowIter) mapValsToChange(vals []any, dst *change) error {
	for i, c := range ct.cols {
		v := *(vals[i].(*any))
		switch c {
		case "__$start_lsn":
			if b, ok := v.([]byte); ok {
				dst.startSCN = b
			} else {
				return errors.New("failed to map 'start_lsn' column from change table")
			}
		case "__$end_lsn":
			// "In SQL Server 2012 (11.x), this column is always NULL."
			// https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver16
			if b, ok := v.([]byte); ok {
				dst.endSCN = b
			} else if v == nil {
				dst.endSCN = nil
			} else {
				ct.log.Warnf("failed to map 'end_lsn' column from change table")
			}
		case "__$update_mask":
			if b, ok := v.([]byte); ok {
				dst.updateMask = b
			} else {
				return errors.New("failed to map 'update_mask' column from change table")
			}
		case "__$operation":
			switch x := v.(type) {
			case int64:
				dst.operation = OpType(x)
			case int32:
				dst.operation = OpType(x)
			default:
				return errors.New("failed to map 'operation' column from change table")
			}
		case "__$command_id":
			switch x := v.(type) {
			case int64:
				dst.commandID = int(x)
			case int32:
				dst.commandID = int(x)
			default:
				return errors.New("failed to map 'command_id' column from change table")
			}
		case "__$seqval":
			if b, ok := v.([]byte); ok {
				dst.seqVal = b
			} else {
				return errors.New("failed to map 'seqval' column from change table")
			}
		default:
			if ct.colTypes[i] != nil {
				dst.columns[c] = mapScannedValue(v, ct.colTypes[i])
			} else {
				dst.columns[c] = v
			}
		}
	}
	return nil
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
