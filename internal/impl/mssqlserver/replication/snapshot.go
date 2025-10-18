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

	"golang.org/x/sync/errgroup"
)

// Snapshot is responsible for creating snapshots of existing tables based on the Tables configuration value.
type Snapshot struct {
	db                      *sql.DB
	tables                  []UserDefinedTable
	publisher               ChangePublisher
	log                     *service.Logger
	snapshotStatusMetric    *service.MetricGauge
	snapshotRowsTotalMetric *service.MetricCounter
}

// NewSnapshot creates a new instance of Snapshot capable of snapshotting provided tables.
// It does this by creating a transaction with snapshot level isolation before paging
// through rows, sending them to be batched.
func NewSnapshot(
	connectionString string,
	tables []UserDefinedTable,
	publisher ChangePublisher,
	logger *service.Logger,
	metrics *service.Metrics,
) (*Snapshot, error) {
	db, err := sql.Open("mssql", connectionString)
	if err != nil {
		return nil, fmt.Errorf("connecting to microsoft sql server for snapshotting: %w", err)
	}
	s := &Snapshot{
		db:                      db,
		tables:                  tables,
		publisher:               publisher,
		log:                     logger,
		snapshotStatusMetric:    metrics.NewGauge("microsoft_sql_server_snapshot_status", "table"),
		snapshotRowsTotalMetric: metrics.NewCounter("microsoft_sql_server_snapshot_rows_processed_total", "table"),
	}
	return s, nil
}

// Prepare performs initial validation and captures the max LSN in preparation for snapshotting tables.
func (s *Snapshot) Prepare(ctx context.Context) (LSN, error) {
	if len(s.tables) == 0 {
		return nil, errors.New("no tables provided")
	}

	var maxLSN LSN
	// capture max LSN before beginning snapshot transactions
	if err := s.db.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&maxLSN); err != nil {
		return nil, err
	} else if len(maxLSN) == 0 {
		// rare, but possible if the user enabled CDC on a table seconds before running snapshot or the agent has stopped working for some reason
		return nil, errors.New("unable to captue max_lsn, this can be due to reasons such as the log scanning agent has stopped")
	}

	return maxLSN, nil
}

// snapshotTable is responsible for managing the entire process of replicating data from the table specified.
func (s *Snapshot) snapshotTable(ctx context.Context, table UserDefinedTable, maxBatchSize int) func() error {
	return func() error {
		var (
			err       error
			tx        *sql.Tx
			tableName = table.FullName()
		)
		l := s.log.With("src_table", tableName)
		l.Infof("Launching snapshot of table '%s'", tableName)

		// BeginTx opens/reuses a dedicated connection for the given table-based transaction, using context.Background()
		// because we want the transaction to be long lived. We explicitly rollback/commit it on function exit
		if tx, err = s.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSnapshot}); err != nil {
			return fmt.Errorf("starting snapshot transaction: %w", err)
		}
		defer func() {
			if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					l.Errorf("Failed to rollback snapshot transaction: %v", rbErr)
				}
			} else {
				if cmErr := tx.Commit(); cmErr != nil {
					l.Errorf("Failed to commit snapshot transaction: %v", cmErr)
				}
			}
		}()

		var tablePks []string
		tablePks, err = getTablePrimaryKeys(ctx, tx, table)
		if err != nil {
			return err
		}
		l.Tracef("Primary keys for table '%s': %v", table, tablePks)
		lastSeenPksValues := map[string]any{}
		for _, pk := range tablePks {
			lastSeenPksValues[pk] = nil
		}

		var numRowsProcessed int
		for {
			var batchRows *sql.Rows
			if numRowsProcessed == 0 {
				batchRows, err = querySnapshotTable(ctx, tx, table, tablePks, nil, maxBatchSize)
			} else {
				batchRows, err = querySnapshotTable(ctx, tx, table, tablePks, lastSeenPksValues, maxBatchSize)
			}
			if err != nil {
				return fmt.Errorf("failed to execute snapshot table query: %s", err)
			}

			var types []*sql.ColumnType
			types, err = batchRows.ColumnTypes()
			if err != nil {
				return fmt.Errorf("failed to fetch column types: %w", err)
			}

			values, mappers := prepSnapshotScannerAndMappers(types)

			var columns []string
			columns, err = batchRows.Columns()
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
				var v any
				for idx, value := range values {
					v, err = mappers[idx](value)
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
				if err = s.publisher.Publish(ctx, m); err != nil {
					return fmt.Errorf("handling snapshot table row: %w", err)
				}
			}

			if err = batchRows.Err(); err != nil {
				return fmt.Errorf("iterating snapshot table row: %w", err)
			}
			s.snapshotRowsTotalMetric.Incr(int64(batchRowsCount), tableName)
			if batchRowsCount < maxBatchSize {
				break
			}
		}

		s.snapshotStatusMetric.Set(1, tableName)
		l.Infof("Table snapshot completed, %d rows processed", numRowsProcessed)

		return nil
	}
}

// Read launches N number of go routines (based on maxWorkers) and starts the process of
// iterating through each table, reading rows based on maxBatchSize, sending the row as a
// replication.MessageEvent to the configured publisher.
func (s *Snapshot) Read(ctx context.Context, maxWorkers, maxBatchSize int) error {
	s.log.Infof("Starting snapshot of %d table(s) using %d configured readers", len(s.tables), maxWorkers)

	for _, table := range s.tables {
		s.snapshotStatusMetric.Set(0, table.FullName())
	}

	wg, ctx := errgroup.WithContext(ctx)
	wg.SetLimit(maxWorkers)

	for _, table := range s.tables {
		wg.Go(s.snapshotTable(ctx, table, maxBatchSize))
	}

	if err := wg.Wait(); err != nil {
		return fmt.Errorf("processing snapshots: %w", err)
	}

	return nil
}

func getTablePrimaryKeys(ctx context.Context, tx *sql.Tx, table UserDefinedTable) ([]string, error) {
	pkSQL := `
	SELECT c.name AS column_name FROM sys.indexes i
	JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
	JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
	JOIN sys.tables t ON i.object_id = t.object_id
	JOIN sys.schemas s ON t.schema_id = s.schema_id
	WHERE i.is_primary_key = 1 AND t.name = ? AND s.name = ?
	ORDER BY ic.key_ordinal;`

	rows, err := tx.QueryContext(ctx, pkSQL, table.Name, table.Schema)
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

func querySnapshotTable(
	ctx context.Context,
	tx *sql.Tx,
	table UserDefinedTable,
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
		return tx.QueryContext(ctx, q)
	}

	var (
		lastSeenPkVals []any
		placeholders   []string
	)
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
	return tx.QueryContext(ctx, q, lastSeenPkVals...)
}

// Close safely closes all open connections opened for the snapshotting process.
// It should be called after a non-recoverale error or once the snapshot process has completed.
func (s *Snapshot) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("closing database connection: %w", err)
		}
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
