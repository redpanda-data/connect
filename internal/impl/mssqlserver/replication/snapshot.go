// Copyright 2025 Redpanda Data, Inc.
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
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// Snapshot is responsible for creating snapshots of existing tables based on the Tables configuration value.
type Snapshot struct {
	db *sql.DB
	tx *sql.Tx

	snapshotConn *sql.Conn

	tables                  []UserTable
	publisher               ChangePublisher
	log                     *service.Logger
	snapshotStatusMetric    *service.MetricGauge
	snapshotRowsTotalMetric *service.MetricCounter
}

// NewSnapshot creates a new instance of Snapshot capable of snapshotting provided tables.
// It does this by creating a transaction with snapshot level isolation before paging through rows, sending them to be batched.
func NewSnapshot(
	db *sql.DB,
	tables []UserTable,
	publisher ChangePublisher,
	logger *service.Logger,
	metrics *service.Metrics) *Snapshot {
	return &Snapshot{
		db:                      db,
		tables:                  tables,
		publisher:               publisher,
		log:                     logger,
		snapshotStatusMetric:    metrics.NewGauge("microsoft_sql_server_snapshot_status", "table"),
		snapshotRowsTotalMetric: metrics.NewCounter("microsoft_sql_server_snapshot_rows_processed_total", "table"),
	}
}

// Prepare performs initial validation, creating of connections in preparation for snapshotting tables.
func (s *Snapshot) Prepare(ctx context.Context) (LSN, error) {
	if len(s.tables) == 0 {
		return nil, errors.New("no tables provided")
	}

	var err error

	// create separate connection for snapshotting
	if s.snapshotConn, err = s.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshot connection: %v", err)
	}

	// Use context.Background() because we want the Tx to be long lived, we explicitly close it in the close method
	if s.tx, err = s.snapshotConn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot}); err != nil {
		return nil, fmt.Errorf("starting snapshot transaction: %v", err)
	}

	var maxLSN LSN
	// capture max LSN _after_ beginning snapshot transaction
	if err := s.snapshotConn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err != nil {
		return nil, err
	} else if len(maxLSN) == 0 {
		// rare, but possible if the user enabled CDC on a table seconds before running snapshot or the agent has stopped working for some reason
		return nil, errors.New("unable to captue max_lsn, this can be due to reasons such as the log scanning agent has stopped")
	}

	return maxLSN, nil
}

// Read starts the process of iterating through each table, reading rows based on maxBatchSize, sending the row as a
// replication.MessageEvent to the configured publisher.
func (s *Snapshot) Read(ctx context.Context, maxBatchSize int) error {
	s.log.Infof("Starting snapshot of %d table(s)", len(s.tables))

	for _, table := range s.tables {
		s.snapshotStatusMetric.Set(0, table.FullName())
	}

	for _, table := range s.tables {
		tablePks, err := s.getTablePrimaryKeys(ctx, table)
		if err != nil {
			return err
		}
		s.log.Tracef("Primary keys for table '%s': %v", table, tablePks)
		lastSeenPksValues := map[string]any{}
		for _, pk := range tablePks {
			lastSeenPksValues[pk] = nil
		}

		var numRowsProcessed int

		fullTableName := table.FullName()
		s.log.Infof("Beginning snapshot process for table '%s'", fullTableName)
		for {
			s.snapshotRowsTotalMetric.Incr(int64(numRowsProcessed), fullTableName)
			var batchRows *sql.Rows
			if numRowsProcessed == 0 {
				batchRows, err = s.querySnapshotTable(ctx, table, tablePks, nil, maxBatchSize)
			} else {
				batchRows, err = s.querySnapshotTable(ctx, table, tablePks, lastSeenPksValues, maxBatchSize)
			}
			if err != nil {
				return fmt.Errorf("failed to execute snapshot table query: %s", err)
			}

			types, err := batchRows.ColumnTypes()
			if err != nil {
				return fmt.Errorf("failed to fetch column types: %w", err)
			}

			values, mappers := prepSnapshotScannerAndMappers(types)

			columns, err := batchRows.Columns()
			if err != nil {
				return fmt.Errorf("failed to fetch columns: %w", err)
			}

			var batchRowsCount int
			for batchRows.Next() {
				numRowsProcessed++
				batchRowsCount++

				if err := batchRows.Scan(values...); err != nil {
					return err
				}

				row := map[string]any{}
				for idx, value := range values {
					v, err := mappers[idx](value)
					if err != nil {
						return err
					}
					row[columns[idx]] = v
					if _, ok := lastSeenPksValues[columns[idx]]; ok {
						lastSeenPksValues[columns[idx]] = value
					}
				}

				m := MessageEvent{
					Table:     table.Name,
					Schema:    table.Schema,
					Data:      row,
					Operation: MessageOperationRead.String(),
					LSN:       nil,
				}
				if err := s.publisher.Publish(ctx, m); err != nil {
					return fmt.Errorf("handling snapshot table row: %w", err)
				}
			}

			if err := batchRows.Err(); err != nil {
				return fmt.Errorf("iterating snapshot table row: %w", err)
			}

			if batchRowsCount < maxBatchSize {
				break
			}
		}

		s.snapshotStatusMetric.Set(1, fullTableName)
		s.log.Infof("Completed snapshot process for table '%s'", fullTableName)
	}
	return nil
}

func (s *Snapshot) getTablePrimaryKeys(ctx context.Context, table UserTable) ([]string, error) {
	pkSQL := `
	SELECT c.name AS column_name FROM sys.indexes i
	JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
	JOIN sys.tables t ON i.object_id = t.object_id
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	WHERE i.is_primary_key = 1 AND t.name = ? AND s.name = ?
	ORDER BY ic.key_ordinal;`

	rows, err := s.tx.QueryContext(ctx, pkSQL, table.Name, table.Schema)
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
		return nil, fmt.Errorf("discovering primary keys for table '%s': %w", table.FullName(), err)
	}

	if len(pks) == 0 {
		return nil, fmt.Errorf("unable to find primary key for table '%s' - does the table exist and does it have a primary key set?", table.FullName())
	}
	return pks, nil
}

func (s *Snapshot) querySnapshotTable(
	ctx context.Context,
	table UserTable,
	pk []string,
	lastSeenPkVal map[string]any,
	limit int,
) (*sql.Rows, error) {
	snapshotQueryParts := []string{
		fmt.Sprintf("SELECT TOP (%d) * FROM [%s].[%s]", limit, table.Schema, table.Name),
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))

		q := strings.Join(snapshotQueryParts, " ")
		return s.tx.QueryContext(ctx, q)
	}

	var lastSeenPkVals []any
	var placeholders []string
	for _, pkCol := range lastSeenPkVal {
		lastSeenPkVals = append(lastSeenPkVals, pkCol)
		placeholders = append(placeholders, "?")
	}

	ph1 := strings.Join(pk, ", ")
	ph2 := strings.Join(placeholders, ", ")
	res := fmt.Sprintf("WHERE (%s) > (%s)", ph1, ph2)
	snapshotQueryParts = append(snapshotQueryParts, res)
	snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
	q := strings.Join(snapshotQueryParts, " ")
	return s.tx.QueryContext(ctx, q, lastSeenPkVals...)
}

// Close safely closes all open connections opened for the snapshotting process.
// It should be called after a non-recoverale error or once the snapshot process has completed.
func (s *Snapshot) Close() error {
	var errs []error

	if s.tx != nil {
		if err := s.tx.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("rollback transaction: %w", err))
		}
		s.tx = nil
	}

	for _, conn := range []*sql.Conn{s.snapshotConn} {
		if conn == nil {
			continue
		}
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close connection: %w", err))
		}
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close db: %w", err))
		}
	}

	return errors.Join(errs...)
}

func prepSnapshotScannerAndMappers(cols []*sql.ColumnType) (values []any, mappers []func(any) (any, error)) {
	stringMapping := func(mapper func(s string) (any, error)) func(any) (any, error) {
		return func(v any) (any, error) {
			s, ok := v.(*sql.NullString)
			if !ok {
				return nil, fmt.Errorf("expected %T got %T", "", v)
			}
			if !s.Valid {
				return nil, nil
			}
			return mapper(s.String)
		}
	}
	for _, col := range cols {
		var val any
		var mapper func(any) (any, error)

		switch col.DatabaseTypeName() {
		case "BINARY", "VARBINARY", "VARBINARY(MAX)", "IMAGE":
			val = new(sql.Null[[]byte])
			mapper = snapshotValueMapper[[]byte]
		case "DATETIME", "DATETIME2", "SMALLDATETIME", "DATE", "TIME", "DATETIMEOFFSET":
			val = new(sql.NullTime)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullTime)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", time.Time{}, v)
				}
				if !s.Valid {
					return nil, nil
				}
				return s.Time, nil
			}
		case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
			val = new(sql.NullInt64)
			mapper = func(v any) (any, error) {
				s, ok := v.(*sql.NullInt64)
				if !ok {
					return nil, fmt.Errorf("expected %T got %T", int64(0), v)
				}
				if !s.Valid {
					return nil, nil
				}
				return int(s.Int64), nil
			}
		case "DECIMAL", "NUMERIC":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				return json.Number(s), nil
			})
		case "FLOAT", "DOUBLE":
			val = new(sql.Null[float64])
			mapper = snapshotValueMapper[float64]
		case "JSON":
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (v any, err error) {
				err = json.Unmarshal([]byte(s), &v)
				return
			})
		default:
			val = new(sql.Null[string])
			mapper = snapshotValueMapper[string]
		}
		values = append(values, val)
		mappers = append(mappers, mapper)
	}
	return
}

func buildOrderByClause(pk []string) string {
	if len(pk) == 1 {
		return "ORDER BY " + pk[0]
	}

	return "ORDER BY " + strings.Join(pk, ", ")
}

func snapshotValueMapper[T any](v any) (any, error) {
	s, ok := v.(*sql.Null[T])
	if !ok {
		var e T
		return nil, fmt.Errorf("expected %T got %T", e, v)
	}
	if !s.Valid {
		return nil, nil
	}
	return s.V, nil
}
