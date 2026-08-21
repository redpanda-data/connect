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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/confx"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/mssqlservertest"
	"github.com/redpanda-data/connect/v4/internal/impl/mssqlserver/replication"
)

// includeFilterFor builds a confx.RegexpFilter that matches only the given
// fully-qualified table name (e.g. "dbo.mytable").
func includeFilterFor(t *testing.T, fullTableName string) *confx.RegexpFilter {
	t.Helper()
	include, err := confx.ParseRegexpPatterns([]string{"^" + regexp.QuoteMeta(fullTableName) + "$"})
	require.NoError(t, err)
	return &confx.RegexpFilter{Include: include}
}

// TestIntegration_VerifyUserDefinedTables_CaptureInstanceResolution covers
// resolving a user table's CDC change table via cdc.change_tables (keyed off
// source_object_id) rather than assuming the default <schema>_<table> capture
// instance naming convention.
func TestIntegration_VerifyUserDefinedTables_CaptureInstanceResolution(t *testing.T) {
	integration.CheckSkip(t)

	_, db := mssqlservertest.SetupTestWithMicrosoftSQLServerVersion(t)
	log := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(io.Discard, nil)))

	t.Run("CustomNamedCaptureInstance", func(t *testing.T) {
		const (
			fullTableName   = "dbo.custom_capture_test"
			captureInstance = "OracleGG_914102297"
		)
		db.MustExec(`CREATE TABLE dbo.custom_capture_test (id INT NOT NULL PRIMARY KEY, data NVARCHAR(100));`)
		// Simulate CDC having already been enabled on this table by another
		// tool (e.g. Oracle GoldenGate) under an arbitrarily named capture
		// instance, rather than the SQL Server default <schema>_<table>.
		db.MustEnableCDCWithCaptureInstance(t.Context(), fullTableName, captureInstance)

		tables, err := replication.VerifyUserDefinedTables(t.Context(), db.DB, includeFilterFor(t, fullTableName), log)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, captureInstance, tables[0].CaptureInstance)
		assert.Equal(t, fmt.Sprintf("cdc.%s_CT", captureInstance), tables[0].ToChangeTable())

		// Insert a row and confirm it's actually picked up from the
		// correctly-named change table when streaming.
		db.MustExec("INSERT INTO dbo.custom_capture_test (id, data) VALUES (?, ?)", 1, "hello")

		require.Eventually(t, func() bool {
			var count int
			q := fmt.Sprintf("SELECT COUNT(*) FROM cdc.[%s_CT]", captureInstance)
			if err := db.QueryRowContext(t.Context(), q).Scan(&count); err != nil {
				return false
			}
			return count >= 1
		}, 2*time.Minute, time.Second, "expected inserted row to appear in custom-named change table")

		publisher := &publisherStub{}
		streaming := replication.NewChangeTableStream(tables, publisher, 200*time.Millisecond, log)

		streamCtx, cancel := context.WithCancel(t.Context())
		errCh := make(chan error, 1)
		go func() {
			errCh <- streaming.ReadChangeTables(streamCtx, db.DB, nil)
		}()

		require.Eventually(t, func() bool {
			return publisher.count() >= 1
		}, 2*time.Minute, 100*time.Millisecond, "expected change to be streamed from the resolved custom capture instance")

		cancel()
		select {
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("ReadChangeTables returned non-cancellation error after context cancel: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("ReadChangeTables did not return after context cancellation")
		}

		publisher.mu.Lock()
		defer publisher.mu.Unlock()
		require.NotEmpty(t, publisher.messages)
		msg := publisher.messages[0]
		assert.Equal(t, "dbo", msg.Schema)
		assert.Equal(t, "custom_capture_test", msg.Table)
		assert.Equal(t, "insert", msg.Operation)
		data, ok := msg.Data.(map[string]any)
		require.True(t, ok, "expected msg.Data to be map[string]any, got %T", msg.Data)
		assert.EqualValues(t, 1, data["id"])
		assert.Equal(t, "hello", data["data"])
	})

	t.Run("DefaultNamedCaptureInstance", func(t *testing.T) {
		const fullTableName = "dbo.default_capture_test"
		db.MustExec(`CREATE TABLE dbo.default_capture_test (id INT NOT NULL PRIMARY KEY);`)
		db.MustEnableCDC(t.Context(), fullTableName)

		tables, err := replication.VerifyUserDefinedTables(t.Context(), db.DB, includeFilterFor(t, fullTableName), log)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "dbo_default_capture_test", tables[0].CaptureInstance)
		assert.Equal(t, "cdc.dbo_default_capture_test_CT", tables[0].ToChangeTable())
	})

	t.Run("AmbiguousMultipleCaptureInstances", func(t *testing.T) {
		const fullTableName = "dbo.ambiguous_capture_test"
		db.MustExec(`CREATE TABLE dbo.ambiguous_capture_test (id INT NOT NULL PRIMARY KEY);`)
		// SQL Server allows up to two capture instances per source table.
		db.MustEnableCDCWithCaptureInstance(t.Context(), fullTableName, "capture_alpha")
		db.MustEnableCDCWithCaptureInstance(t.Context(), fullTableName, "capture_beta")

		_, err := replication.VerifyUserDefinedTables(t.Context(), db.DB, includeFilterFor(t, fullTableName), log)
		require.Error(t, err)
		require.ErrorContains(t, err, "capture_alpha")
		require.ErrorContains(t, err, "capture_beta")
		require.ErrorContains(t, err, "multiple CDC capture instances")
	})
}
