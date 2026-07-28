// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pgtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestIntegrationSignallingConfiguration(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.custom_signal_table (id VARCHAR(32), type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)

	t.Run("supports signal tables", func(t *testing.T) {
		received, _ := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling
    stream_snapshot: true
    signal_table_name: custom_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL))

		// Wait for the initial snapshot to complete before inserting streaming records.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 2, received.Len())
		}, 25*time.Second, 100*time.Millisecond)

		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, 4, received.Len())
		}, 25*time.Second, 100*time.Millisecond)

		require.ElementsMatch(t, received.All(), []any{
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		})
	})

	t.Run("errors when signal table does not exist", func(t *testing.T) {
		_, logs := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_missing_signal_table
    stream_snapshot: true
    signal_table_name: does_not_exist
    schema: dbo
    tables:
      - events
`, databaseURL))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			var found bool
			for _, m := range logs.Messages() {
				if strings.Contains(m, "signal table") && strings.Contains(m, "does not exist") {
					found = true
					break
				}
			}
			assert.True(c, found, "expected a connect error naming the missing signal table, got: %v", logs.Messages())
		}, 25*time.Second, 100*time.Millisecond)
	})

	t.Run("errors when signal table is missing required columns", func(t *testing.T) {
		db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.wrong_shape_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), payload TEXT)`)

		_, logs := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_wrong_shape_signal_table
    stream_snapshot: true
    signal_table_name: wrong_shape_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL))

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			var found bool
			for _, m := range logs.Messages() {
				if strings.Contains(m, "signal table") && strings.Contains(m, "missing required column") && strings.Contains(m, "data") {
					found = true
					break
				}
			}
			assert.True(c, found, "expected a connect error naming the missing data column, got: %v", logs.Messages())
		}, 25*time.Second, 100*time.Millisecond)
	})
}

func TestIntegrationSignallingDisabledWhenTableNameEmpty(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	// Shaped exactly like a signal table, and replicated as an ordinary data
	// table below - with signal_table_name left unset, it must never be
	// treated as one.
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)

	received, _ := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_disabled
    stream_snapshot: true
    schema: dbo
    tables:
      - events
      - rpcn_signal_table
`, databaseURL))

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, received.Len())
	}, 25*time.Second, 100*time.Millisecond)

	received.Reset()

	// This row is shaped exactly like a log signal. With signal_table_name
	// unset, it must be forwarded as an ordinary message rather than detected
	// as a signal.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('log', '{"message": "Signal message"}')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, received.Len())
	}, 25*time.Second, 100*time.Millisecond)

	require.ElementsMatch(t, received.All(), []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
}

// TestIntegrationSignallingDetectedWithoutInterruptingStream verifies that a
// recognized log signal is detected and forwarded downstream like any other
// message, and streaming is never paused, flushed early, or restarted
// because of it (see postgresSignaller.Listen and processStream's handling
// of the detected signal in input_pg_stream.go).
func TestIntegrationSignallingDetectedWithoutInterruptingStream(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)

	received, logs := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_detect_only
    stream_snapshot: true
    signal_table_name: rpcn_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL))

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, received.Len())
	}, 25*time.Second, 100*time.Millisecond)

	received.Reset()

	// A real log signal, immediately followed by an ordinary insert. If
	// detection incorrectly paused or restarted the stream, the second
	// insert would be delayed well past the assertion window below.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('log', '{"message": "Hello World"}')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-signal')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, received.Len())
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		var found bool
		for _, m := range logs.Messages() {
			if strings.Contains(m, "Hello World") {
				found = true
				break
			}
		}
		assert.True(c, found, "expected a log entry for the recognized log signal, got: %v", logs.Messages())
	}, 5*time.Second, 100*time.Millisecond)

	// No re-snapshot should ever follow - give it a moment, then confirm
	// nothing beyond the signal row and the one ordinary insert ever arrives.
	time.Sleep(500 * time.Millisecond)

	require.ElementsMatch(t, received.All(), []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
}

func TestIntegrationSignallingMalformedRowStillPublished(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)

	received, _ := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_malformed
    stream_snapshot: true
    signal_table_name: rpcn_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL))

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, received.Len())
	}, 25*time.Second, 100*time.Millisecond)

	received.Reset()

	// data is left NULL, so Listen fails to parse this row as a signal
	// ("expected string for ...data column, got <nil>"). That must not stop
	// it from being published like any other row.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type) VALUES ('log')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-malformed-signal')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, received.Len())
	}, 5*time.Second, 100*time.Millisecond)

	require.ElementsMatch(t, received.All(), []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
}

// TestIntegrationSignalTableNameWithEmptyTablesReplicatesAllTables verifies
// that setting signal_table_name while leaving tables empty (the documented
// FOR ALL TABLES mode - see the tables field docs in input_pg_stream.go)
// does not collapse the publication down to just the signal table.
func TestIntegrationSignalTableNameWithEmptyTablesReplicatesAllTables(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	received, logs := startSignallingStream(t, fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_all_tables
    signal_table_name: rpcn_signal_table
    schema: dbo
`, databaseURL))

	// There's no initial snapshot to synchronize on since tables is empty,
	// so wait for the stream to confirm replication has actually started
	// before inserting.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		var started bool
		for _, m := range logs.Messages() {
			if strings.Contains(m, "Started logical replication on slot") {
				started = true
				break
			}
		}
		assert.True(c, started, "expected replication to have started")
	}, 25*time.Second, 100*time.Millisecond)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('hello')`)
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('log', '{"message": "hi"}')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 2, received.Len())
	}, 25*time.Second, 100*time.Millisecond)

	require.ElementsMatch(t, received.All(), []any{
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
	})
}

func startSignallingStream(t *testing.T, inputYAML string) (*pgtest.ReceivedMessages, *pgtest.TestLogCapture) {
	t.Helper()

	logs := &pgtest.TestLogCapture{}
	streamOutBuilder := service.NewStreamBuilder()
	streamOutBuilder.SetLogger(slog.New(logs))
	require.NoError(t, streamOutBuilder.AddInputYAML(inputYAML))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	received := &pgtest.ReceivedMessages{}
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		for _, msg := range batch {
			data, err := msg.AsStructured()
			if err != nil {
				return err
			}
			m := data.(map[string]any)
			if _, ok := m["lsn"]; ok {
				m["lsn"] = "XXX/XXX"
			}
			delete(m, "schema")
			delete(m, "commit_ts_ms")
			received.Add(m)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())
	t.Cleanup(func() {
		require.NoError(t, streamOut.StopWithin(10*time.Second))
	})

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	return received, logs
}
