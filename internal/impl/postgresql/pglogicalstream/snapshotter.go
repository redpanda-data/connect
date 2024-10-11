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

// Snapshotter is a structure that allows the creation of a snapshot of a database at a given point in time
// At the time we initialize logical replication - we specify what we want to export the snapshot.
// This snapshot exists until the connection that created the replication slot remains open.
// Therefore Snapshotter opens another connection to the database and sets the transaction to the snapshot.
// This allows you to read the data that was in the database at the time of the snapshot creation.
type Snapshotter struct {
	pgConnection *sql.DB
	logger       *service.Logger

	snapshotName string
}

// NewSnapshotter creates a new Snapshotter instance
func NewSnapshotter(dbConf pgconn.Config, snapshotName string, logger *service.Logger) (*Snapshotter, error) {
	pgConn, err := openPgConnectionFromConfig(dbConf)

	return &Snapshotter{
		pgConnection: pgConn,
		snapshotName: snapshotName,
		logger:       logger,
	}, err
}

func (s *Snapshotter) prepare() error {
	if _, err := s.pgConnection.Exec("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;"); err != nil {
		return err
	}
	if _, err := s.pgConnection.Exec(fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s';", s.snapshotName)); err != nil {
		return err
	}

	return nil
}

func (s *Snapshotter) GetRowsCountPerTable(tableNames []string) (map[string]int, error) {
	tables := make(map[string]int)
	rows, err := s.pgConnection.Query("SELECT table_name, count(*) FROM information_schema.tables WHERE table_name in (?) GROUP BY table_name;", tableNames)
	if err != nil {
		return tables, err
	}

	for rows.Next() {
		var tableName string
		var count int
		if err := rows.Scan(&tableName, &count); err != nil {
			return tables, err
		}
		tables[tableName] = count
	}

	return tables, nil
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
	_, err := s.pgConnection.Exec("COMMIT;")
	return err
}

func (s *Snapshotter) closeConn() error {
	if s.pgConnection != nil {
		return s.pgConnection.Close()
	}

	return nil
}
