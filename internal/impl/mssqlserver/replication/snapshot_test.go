// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/mssqlservertest"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_Snapshot_(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	connStr, db := mssqlservertest.SetupTestWithMicrosoftSQLServerVersion(t, "2022-latest")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("SinglePrimaryKey", func(t *testing.T) {
		createTableSQL := `
		CREATE TABLE dbo.single_key_test (
			id INT NOT NULL PRIMARY KEY,
			data NVARCHAR(100)
		);`
		require.NoError(t, db.CreateTableWithCDCEnabledIfNotExists(t.Context(), "dbo.single_key_test", createTableSQL))

		var totalRows int
		for i := range 50 {
			totalRows++
			db.MustExec("INSERT INTO dbo.single_key_test (id, data) VALUES (?, ?)", i, "test-data")
		}

		publisher := &publisherStub{}
		tables := []replication.UserDefinedTable{
			{Schema: "dbo", Name: "single_key_test"},
		}

		snapshot, err := replication.NewSnapshot(connStr, tables, publisher, service.NewLoggerFromSlog(log), nil)
		require.NoError(t, err)
		defer snapshot.Close()

		lsn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, lsn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 12)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})

	t.Run("TwoColumnCompositeKey_WithPagination", func(t *testing.T) {
		createTableSQL := `
		CREATE TABLE dbo.composite_key_test (
			col1 INT NOT NULL,
			col2 INT NOT NULL,
			data NVARCHAR(100),
			PRIMARY KEY (col1, col2)
		);`
		require.NoError(t, db.CreateTableWithCDCEnabledIfNotExists(t.Context(), "dbo.composite_key_test", createTableSQL))

		var totalRows int
		for i := range 10 {
			for j := range 5 {
				totalRows++
				db.MustExec("INSERT INTO dbo.composite_key_test (col1, col2, data) VALUES (?, ?, ?)", i, j, "test-data")
			}
		}

		// Create publisher to collect messages
		publisher := &publisherStub{}
		tables := []replication.UserDefinedTable{
			{Schema: "dbo", Name: "composite_key_test"},
		}

		snapshot, err := replication.NewSnapshot(connStr, tables, publisher, service.NewLoggerFromSlog(log), nil)
		require.NoError(t, err)
		defer snapshot.Close()

		lsn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, lsn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 10)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})

	t.Run("TwoColumnCompositeKey_WithPagination", func(t *testing.T) {
		createTableSQL := `
		CREATE TABLE dbo.three_col_key_test (
			col1 INT NOT NULL,
			col2 INT NOT NULL,
			col3 INT NOT NULL,
			data NVARCHAR(100),
			PRIMARY KEY (col1, col2, col3)
		);`
		require.NoError(t, db.CreateTableWithCDCEnabledIfNotExists(t.Context(), "dbo.three_col_key_test", createTableSQL))

		var totalRows int
		for i := range 5 {
			for j := range 3 {
				for k := range 4 {
					totalRows++
					db.MustExec("INSERT INTO dbo.three_col_key_test (col1, col2, col3, data) VALUES (?, ?, ?, ?)", i, j, k, "test-data")
				}
			}
		}

		publisher := &publisherStub{}
		tables := []replication.UserDefinedTable{
			{Schema: "dbo", Name: "three_col_key_test"},
		}

		snapshot, err := replication.NewSnapshot(connStr, tables, publisher, service.NewLoggerFromSlog(log), nil)
		require.NoError(t, err)
		defer snapshot.Close()

		lsn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, lsn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 8)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})
}

// publisherStub implements ChangePublisher interface for testing
type publisherStub struct {
	messages []replication.MessageEvent
	mu       sync.Mutex
}

func (m *publisherStub) Publish(_ context.Context, msg replication.MessageEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
	return nil
}

func (m *publisherStub) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.messages)
}
