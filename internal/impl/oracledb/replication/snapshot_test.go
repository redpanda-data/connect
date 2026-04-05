// Copyright 2026 Redpanda Data, Inc.
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
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/oracledbtest"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	connStr, db := oracledbtest.SetupTestWithOracleDBVersion(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Create all tables upfront before running subtests. Oracle requires SCNs to advance
	// after DDL before SET TRANSACTION READ ONLY can provide a consistent read (ORA-01466).
	// Creating tables here and sleeping gives the DDL time to settle before any snapshot runs.
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "TESTDB.single_key_test", `
		CREATE TABLE TESTDB.single_key_test (
			id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			data NVARCHAR2(100)
		)`))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "TESTDB.composite_key_test", `
		CREATE TABLE TESTDB.composite_key_test (
			col1 NUMBER NOT NULL,
			col2 NUMBER NOT NULL,
			data NVARCHAR2(100),
			CONSTRAINT composite_key_test_pk PRIMARY KEY (col1, col2)
		)`))
	require.NoError(t, db.CreateTableWithSupplementalLoggingIfNotExists(t.Context(), "TESTDB.three_col_key_test", `
		CREATE TABLE TESTDB.three_col_key_test (
			col1 NUMBER NOT NULL,
			col2 NUMBER NOT NULL,
			col3 NUMBER NOT NULL,
			data NVARCHAR2(100),
			CONSTRAINT three_col_key_test_pk PRIMARY KEY (col1, col2, col3)
		)`))

	// Wait for DDL changes to settle in Oracle's redo logs before taking snapshots.
	time.Sleep(2 * time.Second)

	t.Run("SinglePrimaryKey", func(t *testing.T) {
		var totalRows int
		for range 50 {
			totalRows++
			db.MustExec("INSERT INTO TESTDB.single_key_test (data) VALUES (:1)", "test-data")
		}

		publisher := &publisherStub{}
		tables := []replication.UserTable{
			{Schema: "TESTDB", Name: "SINGLE_KEY_TEST"},
		}

		snapshot, err := replication.NewSnapshot(t.Context(), connStr, tables, publisher, false, service.NewLoggerFromSlog(log), service.MockResources().Metrics(), "")
		require.NoError(t, err)
		defer snapshot.Close()

		scn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotZero(t, scn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 12)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})

	t.Run("TwoColumnCompositeKey_WithPagination", func(t *testing.T) {
		var totalRows int
		for i := range 10 {
			for j := range 5 {
				totalRows++
				db.MustExec("INSERT INTO TESTDB.composite_key_test (col1, col2, data) VALUES (:1, :2, :3)", i, j, "test-data")
			}
		}

		publisher := &publisherStub{}
		tables := []replication.UserTable{
			{Schema: "TESTDB", Name: "COMPOSITE_KEY_TEST"},
		}

		snapshot, err := replication.NewSnapshot(t.Context(), connStr, tables, publisher, false, service.NewLoggerFromSlog(log), service.MockResources().Metrics(), "")
		require.NoError(t, err)
		defer snapshot.Close()

		scn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotZero(t, scn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 10)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})

	t.Run("ThreeColumnCompositeKey_WithPagination", func(t *testing.T) {
		var totalRows int
		for i := range 5 {
			for j := range 3 {
				for k := range 4 {
					totalRows++
					db.MustExec("INSERT INTO TESTDB.three_col_key_test (col1, col2, col3, data) VALUES (:1, :2, :3, :4)", i, j, k, "test-data")
				}
			}
		}

		publisher := &publisherStub{}
		tables := []replication.UserTable{
			{Schema: "TESTDB", Name: "THREE_COL_KEY_TEST"},
		}

		snapshot, err := replication.NewSnapshot(t.Context(), connStr, tables, publisher, false, service.NewLoggerFromSlog(log), service.MockResources().Metrics(), "")
		require.NoError(t, err)
		defer snapshot.Close()

		scn, err := snapshot.Prepare(t.Context())
		require.NoError(t, err)
		require.NotZero(t, scn)

		// Read snapshot with small batch size to trigger pagination
		err = snapshot.Read(t.Context(), 1, 8)
		require.NoError(t, err)

		assert.Equalf(t, totalRows, publisher.count(), "Expected all %d rows to be captured during snapshot", totalRows)
	})
}

// publisherStub implements the replication.ChangePublisher interface for testing.
type publisherStub struct {
	messages []*replication.MessageEvent
	mu       sync.Mutex
}

func (p *publisherStub) Publish(_ context.Context, msg *replication.MessageEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, msg)
	return nil
}

func (*publisherStub) Close() {}

func (p *publisherStub) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}
