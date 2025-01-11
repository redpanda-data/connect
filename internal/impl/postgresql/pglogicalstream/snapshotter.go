// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgtype"

	"errors"

	_ "github.com/lib/pq"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

// SnapshotCreationResponse is a structure that contains the name of the snapshot that was created
type SnapshotCreationResponse struct {
	ExportedSnapshotName string
}

// Snapshotter is a structure that allows the creation of a snapshot of a database at a given point in time
// At the time we initialize logical replication - we specify what we want to export the snapshot.
// This snapshot exists until the connection that created the replication slot remains open.
// Therefore Snapshotter opens another connection to the database and sets the transaction to the snapshot.
// This allows you to read the data that was in the database at the time of the snapshot creation.
type Snapshotter struct {
	pool   *sql.DB
	logger *service.Logger
	// Only needed for older PG versions, holds the snapshot open for the reader
	snapshotTxn *sql.Tx
	// The TXN for the snapshot phase
	readerTxn *sql.Tx

	snapshotName string

	version int
}

// NewSnapshotter creates a new Snapshotter instance
func NewSnapshotter(dbDSN string, logger *service.Logger, version int) (*Snapshotter, error) {
	pgConn, err := openPgConnectionFromConfig(dbDSN)
	if err != nil {
		return nil, err
	}

	return &Snapshotter{
		pool:    pgConn,
		logger:  logger,
		version: version,
	}, nil
}

func (s *Snapshotter) initSnapshotTransaction(ctx context.Context) (SnapshotCreationResponse, error) {
	if s.version > 14 {
		return SnapshotCreationResponse{}, errors.New("snapshot is exported by default for versions above PG14")
	}
	if s.snapshotTxn != nil {
		return SnapshotCreationResponse{}, errors.New("snapshot already exists")
	}

	var snapshotName sql.NullString

	tx, err := s.pool.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return SnapshotCreationResponse{}, fmt.Errorf("unable to begin a tx to export a snapshot: %w pg version: %d", err, s.version)
	}
	s.snapshotTxn = tx
	snapshotRow := tx.QueryRowContext(ctx, `SELECT pg_export_snapshot();`)
	if snapshotRow.Err() != nil {
		return SnapshotCreationResponse{}, fmt.Errorf("unable to get snapshot name: %w", snapshotRow.Err())
	}

	if err = snapshotRow.Scan(&snapshotName); err != nil {
		return SnapshotCreationResponse{}, fmt.Errorf("cant scan snapshot name into string: %w", err)
	}

	return SnapshotCreationResponse{ExportedSnapshotName: snapshotName.String}, nil
}

func (s *Snapshotter) setTransactionSnapshotName(snapshotName string) {
	s.snapshotName = snapshotName
}

func (s *Snapshotter) prepare(ctx context.Context) error {
	if s.snapshotName == "" {
		return errors.New("snapshot name is not set")
	}
	if s.readerTxn != nil {
		return errors.New("reader txn already open")
	}
	// Use a background context because we explicitly want the Tx to be long lived, we explicitly close it in the close method
	tx, err := s.pool.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("unable to start reader txn: %w", err)
	}
	s.readerTxn = tx
	sq, err := sanitize.SQLQuery("SET TRANSACTION SNAPSHOT $1;", s.snapshotName)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, sq); err != nil {
		return err
	}
	return nil
}

func (s *Snapshotter) findAvgRowSize(ctx context.Context, table TableFQN) (avgRowSize sql.NullInt64, err error) {
	row := s.readerTxn.QueryRowContext(ctx, fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table))
	if row.Err() != nil {
		return avgRowSize, fmt.Errorf("cannot get avg row size due to query failure: %w", err)
	}
	if err = row.Scan(&avgRowSize); err != nil {
		return avgRowSize, fmt.Errorf("cannot get avg row size: %w", err)
	}
	return avgRowSize, nil
}

func (s *Snapshotter) prepareScannersAndGetters(columnTypes []*sql.ColumnType) ([]any, []func(any) (any, error)) {
	scanArgs := make([]any, len(columnTypes))
	valueGetters := make([]func(any) (any, error), len(columnTypes))

	for i, v := range columnTypes {
		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				str := v.(*sql.NullString)
				if !str.Valid {
					return nil, nil
				}
				return str.String, nil
			}
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullBool)
				if !val.Valid {
					return nil, nil
				}
				return val.Bool, nil
			}
		case "INT4":
			scanArgs[i] = new(sql.NullInt64)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullInt64)
				if !val.Valid {
					return nil, nil
				}
				return val.Int64, nil
			}
		case "JSONB":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				str := v.(*sql.NullString)
				if !str.Valid {
					return nil, nil
				}
				payload := str.String
				if payload == "" {
					return payload, nil
				}
				var dst any
				if err := json.Unmarshal([]byte(v.(*sql.NullString).String), &dst); err != nil {
					return nil, err
				}

				return dst, nil
			}
		case "INET":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				inet := pgtype.Inet{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := inet.Scan(val.String); err != nil {
					return nil, err
				}

				return inet.IPNet.String(), nil
			}
		case "TSRANGE":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.Tsrange{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				vv, _ := newArray.Value()
				return vv, nil
			}
		case "_INT4":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.Int4Array{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				return newArray.Elements, nil
			}
		case "_TEXT":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				newArray := pgtype.TextArray{}
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				if err := newArray.Scan(val.String); err != nil {
					return nil, err
				}

				return newArray.Elements, nil
			}
		default:
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v any) (any, error) {
				val := v.(*sql.NullString)
				if !val.Valid {
					return nil, nil
				}
				return val.String, nil
			}
		}
	}

	return scanArgs, valueGetters
}

func (s *Snapshotter) calculateBatchSize(availableMemory uint64, estimatedRowSize uint64) int {
	// Adjust this factor based on your system's memory constraints.
	// This example uses a safety factor of 0.8 to leave some memory headroom.
	safetyFactor := 0.6
	batchSize := int(float64(availableMemory) * safetyFactor / float64(estimatedRowSize))
	if batchSize < 1 {
		batchSize = 1
	}

	return batchSize
}

func (s *Snapshotter) querySnapshotData(ctx context.Context, table TableFQN, lastSeenPk map[string]any, pkColumns []string, limit int) (rows *sql.Rows, err error) {
	s.logger.Debugf("Query snapshot table: %v, limit: %v, lastSeenPkVal: %v, pk: %v", table, limit, lastSeenPk, pkColumns)

	if lastSeenPk == nil {
		// NOTE: All strings passed into here have been validated or derived from the code/database, therefore not prone to SQL injection.
		sq, err := sanitize.SQLQuery(fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d;", table.String(), strings.Join(pkColumns, ", "), limit))
		if err != nil {
			return nil, err
		}
		return s.readerTxn.QueryContext(ctx, sq)
	}

	var (
		placeholders      []string
		lastSeenPksValues []any
	)

	for i, col := range pkColumns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		lastSeenPksValues = append(lastSeenPksValues, lastSeenPk[col])
	}

	lastSeenPlaceHolders := "(" + strings.Join(placeholders, ", ") + ")"
	pkAsTuple := "(" + strings.Join(pkColumns, ", ") + ")"

	// NOTE: All strings passed into here have been validated or derived from the code/database, therefore not prone to SQL injection.
	sq, err := sanitize.SQLQuery(fmt.Sprintf("SELECT * FROM %s WHERE %s > %s ORDER BY %s LIMIT %d;", table.String(), pkAsTuple, lastSeenPlaceHolders, strings.Join(pkColumns, ", "), limit), lastSeenPksValues...)
	if err != nil {
		return nil, err
	}

	return s.readerTxn.QueryContext(ctx, sq)
}

func (s *Snapshotter) releaseSnapshot() error {
	if s.version < 14 && s.snapshotTxn != nil {
		if err := s.snapshotTxn.Commit(); err != nil {
			return err
		}
		s.snapshotTxn = nil
	}
	if err := s.readerTxn.Commit(); err != nil {
		return err
	}
	s.readerTxn = nil
	return nil
}

func (s *Snapshotter) closeConn() error {
	if s.readerTxn != nil {
		if err := s.readerTxn.Rollback(); err != nil {
			return err
		}
		s.readerTxn = nil
	}
	if s.snapshotTxn != nil {
		if err := s.snapshotTxn.Rollback(); err != nil {
			return err
		}
		s.snapshotTxn = nil
	}
	if err := s.pool.Close(); err != nil {
		return err
	}

	return nil
}
