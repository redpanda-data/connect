// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"fmt"

	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/redpanda-data/benthos/v4/public/service"
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
	pgConnection             *sql.DB
	snapshotCreateConnection *sql.DB
	logger                   *service.Logger

	snapshotName string

	version int
}

// NewSnapshotter creates a new Snapshotter instance
func NewSnapshotter(dbConf pgconn.Config, logger *service.Logger, version int) (*Snapshotter, error) {
	pgConn, err := openPgConnectionFromConfig(dbConf)
	if err != nil {
		return nil, err
	}

	snapshotCreateConnection, err := openPgConnectionFromConfig(dbConf)
	if err != nil {
		return nil, err
	}

	return &Snapshotter{
		pgConnection:             pgConn,
		snapshotCreateConnection: snapshotCreateConnection,
		logger:                   logger,
		version:                  version,
	}, nil
}

func (s *Snapshotter) initSnapshotTransaction() (SnapshotCreationResponse, error) {
	if s.version > 14 {
		return SnapshotCreationResponse{}, errors.New("snapshot is exported by default for versions above PG14")
	}

	var snapshotName sql.NullString

	snapshotRow, err := s.pgConnection.Query(`BEGIN; SELECT pg_export_snapshot();`)
	if err != nil {
		return SnapshotCreationResponse{}, fmt.Errorf("cant get exported snapshot for initial streaming %w", err)
	}

	if snapshotRow.Err() != nil {
		return SnapshotCreationResponse{}, fmt.Errorf("can get avg row size due to query failure: %w", snapshotRow.Err())
	}

	if snapshotRow.Next() {
		if err = snapshotRow.Scan(&snapshotName); err != nil {
			return SnapshotCreationResponse{}, fmt.Errorf("cant scan snapshot name into string: %w", err)
		}
	} else {
		return SnapshotCreationResponse{}, errors.New("cant get avg row size; 0 rows returned")
	}

	return SnapshotCreationResponse{ExportedSnapshotName: snapshotName.String}, nil
}

func (s *Snapshotter) setTransactionSnapshotName(snapshotName string) {
	s.snapshotName = snapshotName
}

func (s *Snapshotter) prepare() error {
	if s.snapshotName == "" {
		return errors.New("snapshot name is not set")
	}

	if _, err := s.pgConnection.Exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
		return err
	}
	if _, err := s.pgConnection.Exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName)); err != nil {
		return err
	}

	return nil
}

func (s *Snapshotter) findAvgRowSize(table string) (sql.NullInt64, error) {
	var (
		avgRowSize sql.NullInt64
		rows       *sql.Rows
		err        error
	)
	if rows, err = s.pgConnection.Query(fmt.Sprintf(`SELECT SUM(pg_column_size('%s.*')) / COUNT(*) FROM %s;`, table, table)); err != nil {
		return avgRowSize, fmt.Errorf("can get avg row size due to query failure: %w", err)
	}

	if rows.Err() != nil {
		return avgRowSize, fmt.Errorf("can get avg row size due to query failure: %w", rows.Err())
	}

	if rows.Next() {
		if err = rows.Scan(&avgRowSize); err != nil {
			return avgRowSize, fmt.Errorf("can get avg row size: %w", err)
		}
	} else {
		return avgRowSize, errors.New("can get avg row size; 0 rows returned")
	}

	return avgRowSize, nil
}

func (s *Snapshotter) prepareScannersAndGetters(columnTypes []*sql.ColumnType) ([]interface{}, []func(interface{}) interface{}) {
	scanArgs := make([]interface{}, len(columnTypes))
	valueGetters := make([]func(interface{}) interface{}, len(columnTypes))

	for i, v := range columnTypes {
		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v interface{}) interface{} { return v.(*sql.NullString).String }
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			valueGetters[i] = func(v interface{}) interface{} { return v.(*sql.NullBool).Bool }
		case "INT4":
			scanArgs[i] = new(sql.NullInt64)
			valueGetters[i] = func(v interface{}) interface{} { return v.(*sql.NullInt64).Int64 }
		default:
			scanArgs[i] = new(sql.NullString)
			valueGetters[i] = func(v interface{}) interface{} { return v.(*sql.NullString).String }
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

func (s *Snapshotter) querySnapshotData(table string, pk string, limit, offset int) (rows *sql.Rows, err error) {
	s.logger.Infof("Query snapshot table: %v, limit: %v, offset: %v, pk: %v", table, limit, offset, pk)
	return s.pgConnection.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d OFFSET %d;", table, pk, limit, offset))
}

func (s *Snapshotter) releaseSnapshot() error {
	if s.version < 14 && s.snapshotCreateConnection != nil {
		if _, err := s.snapshotCreateConnection.Exec("COMMIT;"); err != nil {
			return err
		}
	}

	_, err := s.pgConnection.Exec("COMMIT;")
	return err
}

func (s *Snapshotter) closeConn() error {
	if s.pgConnection != nil {
		return s.pgConnection.Close()
	}

	if s.snapshotCreateConnection != nil {
		return s.snapshotCreateConnection.Close()
	}

	return nil
}
