// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

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

type snapshot struct {
	db *sql.DB
	tx *sql.Tx

	snapshotConn *sql.Conn
	lockConn     *sql.Conn
	message      chan MessageEvent
	tables       []string

	logger *service.Logger
}

// NewSnapshot creates a new instance of Snapshot.
func NewSnapshot(db *sql.DB, tables []string, logger *service.Logger) *snapshot {
	return &snapshot{
		db:      db,
		tables:  tables,
		logger:  logger,
		message: make(chan MessageEvent),
	}
}

func (s *snapshot) Message() chan MessageEvent {
	return s.message
}

func (s *snapshot) prepare(ctx context.Context) (LSN, error) {
	if len(s.tables) == 0 {
		return nil, errors.New("no tables provided")
	}

	var err error
	// Create a separate connection for table locks
	if s.lockConn, err = s.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("create lock connection: %v", err)
	}

	if s.snapshotConn, err = s.db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("creating snapshot connection: %v", err)
	}

	// TODO: Before snapshotting, can we verify snapshotting isolation on the given tables are enabled?

	// Use context.Background() because we want the Tx to be long lived, we explicitly close it in the close method
	if s.tx, err = s.snapshotConn.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot}); err != nil {
		return nil, fmt.Errorf("starting snapshot transaction: %v", err)
	}

	var toLSN LSN
	// capture max LSN _after_ beginning snapshot transaction
	if err := s.snapshotConn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&toLSN); err != nil {
		return nil, err
	}

	return toLSN, nil
}

func (s *snapshot) getTablePrimaryKeys(ctx context.Context, table string) ([]string, error) {
	pkSql := `
	SELECT c.name AS column_name FROM sys.indexes i
	JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
	JOIN sys.tables t ON i.object_id = t.object_id
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	WHERE i.is_primary_key = 1 AND t.name = ? AND s.name = SCHEMA_NAME()
	ORDER BY ic.key_ordinal;`

	rows, err := s.tx.QueryContext(ctx, pkSql, table)
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
		return nil, fmt.Errorf("discovering primary keys for table '%s': %w", table, err)
	}

	if len(pks) == 0 {
		return nil, fmt.Errorf("unable to find primary key for table %s - does the table exist and does it have a primary key set?", table)
	}

	return pks, nil
}

func (s *snapshot) querySnapshotTable(ctx context.Context, table string, pk []string, lastSeenPkVal *map[string]any, limit int) (*sql.Rows, error) {
	snapshotQueryParts := []string{
		fmt.Sprintf("SELECT TOP (%d) * FROM %s", limit, table),
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))

		q := strings.Join(snapshotQueryParts, " ")
		return s.tx.QueryContext(ctx, q)
	}

	var lastSeenPkVals []any
	var placeholders []string
	for _, pkCol := range *lastSeenPkVal {
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

func (s *snapshot) close() error {
	var errs []error

	if s.tx != nil {
		if err := s.tx.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("rollback transaction: %w", err))
		}
		s.tx = nil
	}

	for _, conn := range []*sql.Conn{s.lockConn, s.snapshotConn} {
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

func (s *snapshot) read(ctx context.Context, maxBatchSize int, handle func(c MessageEvent) error) error {
	// TODO: Process tables in parallel
	for _, table := range s.tables {
		tablePks, err := s.getTablePrimaryKeys(ctx, table)
		if err != nil {
			return err
		}
		s.logger.Tracef("primary keys for table %s: %v", table, tablePks)
		lastSeenPksValues := map[string]any{}
		for _, pk := range tablePks {
			lastSeenPksValues[pk] = nil
		}

		var numRowsProcessed int

		s.logger.Infof("Beginning snapshot process for table '%s'", table)
		for {
			var batchRows *sql.Rows
			if numRowsProcessed == 0 {
				batchRows, err = s.querySnapshotTable(ctx, table, tablePks, nil, maxBatchSize)
			} else {
				batchRows, err = s.querySnapshotTable(ctx, table, tablePks, &lastSeenPksValues, maxBatchSize)
			}
			if err != nil {
				return fmt.Errorf("failed to execute snapshot table query: %s", err)
			}

			types, err := batchRows.ColumnTypes()
			if err != nil {
				return fmt.Errorf("failed to fetch column types: %s", err)
			}

			values, mappers := prepSnapshotScannerAndMappers(types)

			columns, err := batchRows.Columns()
			if err != nil {
				return fmt.Errorf("failed to fetch columns: %s", err)
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
					LSN:       nil,
					Operation: int(MessageOperationRead),
					Table:     table,
					Data:      row,
				}
				if err := handle(m); err != nil {
					return fmt.Errorf("handling snapshot table row: %w", err)
				}

				// select {
				// // case s.message <- MessageEvent{
				// // 	LSN:       nil,
				// // 	Operation: int(MessageOperationRead),
				// // 	Table:     table,
				// // 	Data:      row,
				// // }:
				// case <-ctx.Done():
				// 	return ctx.Err()
				// }
			}

			if err := batchRows.Err(); err != nil {
				return fmt.Errorf("iterating snapshot table row: %w", err)
			}

			if batchRowsCount < maxBatchSize {
				break
			}
		}
		s.logger.Infof("Completed snapshot process for table '%s'", table)
	}
	return nil
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
