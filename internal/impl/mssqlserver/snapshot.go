// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package mssqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type LSN []byte

type Snapshot struct {
	db *sql.DB
	tx *sql.Tx

	snapshotConn *sql.Conn

	logger *service.Logger
}

// NewSnapshot creates a new instance of Snapshot.
func NewSnapshot(logger *service.Logger, db *sql.DB) *Snapshot {
	return &Snapshot{
		db:     db,
		logger: logger,
	}
}

func (s *Snapshot) prepare(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return errors.New("no tables provided")
	}

	var err error
	// Create a separate connection for snapshotting
	s.snapshotConn, err = s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("creating snapshot connection: %v", err)
	}

	// Verify snapshot isolation is enabled
	// pkSql := `SELECT name, snapshot_isolation_state_desc, is_read_committed_snapshot_on FROM sys.databases WHERE name = '%s';`
	// rows, err := s.tx.QueryContext(ctx, fmt.Sprintf(pkSql, table))
	// if err != nil {
	// 	return fmt.Errorf("get primary key: %v", err)
	// }
	// defer rows.Close()

	// Use context.Background() because we want the Tx to be long lived, we explicitly close it in the close method
	s.tx, err = s.snapshotConn.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelSnapshot,
	})
	if err != nil {
		return fmt.Errorf("starting snapshot transaction: %v", err)
	}

	return nil
}
