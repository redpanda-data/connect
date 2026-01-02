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
	db, err := sql.Open("oracle", connectionString)
	if err != nil {
		return nil, fmt.Errorf("connecting to oracle database for snapshotting: %w", err)
	}
	s := &Snapshot{
		db:                      db,
		tables:                  tables,
		publisher:               publisher,
		log:                     logger,
		snapshotStatusMetric:    metrics.NewGauge("oracledb_cdc_snapshot_status", "table"),
		snapshotRowsTotalMetric: metrics.NewCounter("oracledb_cdc_snapshot_rows_total", "table"),
	}
	return s, nil
}

// Prepare prepares the snapshot by starting a transaction with appropriate isolation level.
// Returns the current SCN for the snapshot.
func (s *Snapshot) Prepare(ctx context.Context) (SCN, error) {
	if len(s.tables) == 0 {
		return nil, errors.New("no tables provided")
	}

	var currentSCN SCN
	if err := s.db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&currentSCN); err != nil {
		return nil, fmt.Errorf("getting current SCN for snapshot: %w", err)
	}

	// s.log.Infof("Starting snapshot at SCN: %s", currentSCN)
	return currentSCN, nil
}

// Read launches N number of go routines (based on maxWorkers) and starts the process of
// iterating through each table, reading rows based on maxBatchSize, sending the row as a
// replication.MessageEvent to the configured publisher.
func (s *Snapshot) Read(ctx context.Context, maxWorkers int, maxBatchSize int) error {
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

		// BeginTx opens/reuses a dedicated connection for the given table-based transaction
		// Oracle drivers don't support TxOptions, so we use default and set properties explicitly
		if tx, err = s.db.BeginTx(ctx, nil); err != nil {
			return fmt.Errorf("starting snapshot transaction: %w", err)
		}

		// Set transaction to read-only mode
		// In Oracle, READ ONLY transactions automatically provide serializable isolation
		if _, err = tx.ExecContext(ctx, "SET TRANSACTION READ ONLY"); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("setting transaction read-only: %w", err)
		}
		defer func() {
			if err != nil {
				// sql package automatically rolls back transaction if context is cancelled
				if !errors.Is(err, context.Canceled) {
					if rbErr := tx.Rollback(); rbErr != nil {
						l.Errorf("Failed to rollback snapshot transaction: %v", rbErr)
					}
					return
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
					SCN:       nil,
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

		if err := tx.Commit(); err != nil {
			l.Errorf("Failed to commit snapshot transaction: %v", err)
		}
		s.snapshotStatusMetric.Set(1, tableName)
		l.Infof("Table snapshot completed, %d rows processed", numRowsProcessed)

		return nil
	}
}

func getTablePrimaryKeys(ctx context.Context, tx *sql.Tx, table UserDefinedTable) ([]string, error) {
	// Oracle data dictionary query for primary key columns
	// Note: Oracle stores identifiers in uppercase by default unless created with quotes
	pkSQL := `
		SELECT acc.column_name
		FROM all_constraints ac
		JOIN all_cons_columns acc
			ON ac.constraint_name = acc.constraint_name
			AND ac.owner = acc.owner
		WHERE ac.constraint_type = 'P'
			AND UPPER(ac.table_name) = UPPER(:1)
			AND UPPER(ac.owner) = UPPER(:2)
		ORDER BY acc.position`

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
	// Oracle uses FETCH FIRST instead of TOP, and it comes at the end
	snapshotQueryParts := []string{
		fmt.Sprintf("SELECT * FROM %s.%s", table.Schema, table.Name),
	}

	if lastSeenPkVal == nil {
		snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
		snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("FETCH FIRST %d ROWS ONLY", limit))

		q := strings.Join(snapshotQueryParts, " ")
		return tx.QueryContext(ctx, q)
	}

	// Build lexicographic comparison for composite keys
	// For pk [col1, col2, col3], generates:
	// WHERE (col1 > ?) OR (col1 = ? AND col2 > ?) OR (col1 = ? AND col2 = ? AND col3 > ?)
	// Oracle uses positional parameters (:1, :2, etc.) or named parameters
	var (
		lastSeenPkVals []any
		conditions     []string
		paramIdx       int
	)

	for i := range pk {
		var condParts []string
		// Add equality conditions for all previous columns
		for j := range i {
			paramIdx++
			condParts = append(condParts, fmt.Sprintf("%s = :%d", pk[j], paramIdx))
			lastSeenPkVals = append(lastSeenPkVals, lastSeenPkVal[pk[j]])
		}
		// Add greater-than condition for current column
		paramIdx++
		condParts = append(condParts, fmt.Sprintf("%s > :%d", pk[i], paramIdx))
		lastSeenPkVals = append(lastSeenPkVals, lastSeenPkVal[pk[i]])

		conditions = append(conditions, "("+strings.Join(condParts, " AND ")+")")
	}

	res := "WHERE " + strings.Join(conditions, " OR ")
	snapshotQueryParts = append(snapshotQueryParts, res)
	snapshotQueryParts = append(snapshotQueryParts, buildOrderByClause(pk))
	snapshotQueryParts = append(snapshotQueryParts, fmt.Sprintf("FETCH FIRST %d ROWS ONLY", limit))
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

		// Oracle database type names
		switch col.DatabaseTypeName() {
		case "RAW", "LONG RAW", "BLOB":
			val = new(sql.Null[[]byte])
			mapper = snapshotValueMapper[[]byte]
		case "DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
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
		case "NUMBER", "INTEGER", "INT", "SMALLINT", "FLOAT":
			// Oracle NUMBER type can represent both integers and decimals
			// Scan as string to preserve precision and avoid conversion errors
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				return json.Number(s), nil
			})
		case "BINARY_FLOAT", "BINARY_DOUBLE":
			val = new(sql.Null[float64])
			mapper = snapshotValueMapper[float64]
		case "CLOB", "NCLOB", "LONG":
			// Character large objects - handle as string
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (any, error) {
				return s, nil
			})
		case "JSON":
			// Oracle 21c+ native JSON type
			val = new(sql.NullString)
			mapper = stringMapping(func(s string) (v any, err error) {
				err = json.Unmarshal([]byte(s), &v)
				return
			})
		default:
			// Default to string for VARCHAR2, CHAR, NVARCHAR2, NCHAR, etc.
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
