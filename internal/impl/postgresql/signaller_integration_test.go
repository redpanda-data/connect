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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
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

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling
    stream_snapshot: true
    signal_table_name: custom_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	var (
		received []any
		mu       sync.Mutex
	)
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
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
			received = append(received, m)
		}
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)
	license.InjectTestService(streamOut.Resources())
	t.Cleanup(func() {
		require.NoError(t, streamOut.StopWithin(5*time.Second))
	})

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	// Wait for the initial snapshot to complete before inserting streaming records.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 2)
	}, 25*time.Second, 100*time.Millisecond)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 4)
	}, 25*time.Second, 100*time.Millisecond)

	mu.Lock()
	require.ElementsMatch(t, received, []any{
		map[string]any{"operation": "read", "table": "events"},
		map[string]any{"operation": "read", "table": "events"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
	mu.Unlock()
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

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_disabled
    stream_snapshot: true
    schema: dbo
    tables:
      - events
      - rpcn_signal_table
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	var (
		received []any
		mu       sync.Mutex
	)
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
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
			received = append(received, m)
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

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 1)
	}, 25*time.Second, 100*time.Millisecond)

	mu.Lock()
	received = nil
	mu.Unlock()

	// This row is shaped exactly like a log signal. With signal_table_name
	// unset, it must be forwarded as an ordinary message rather than detected
	// as a signal.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('log', '{"message": "Signal message"}')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 2)
	}, 25*time.Second, 100*time.Millisecond)

	mu.Lock()
	require.ElementsMatch(t, received, []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
	mu.Unlock()
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

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_detect_only
    stream_snapshot: true
    signal_table_name: rpcn_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL)

	logs := &testLogCapture{}
	streamOutBuilder := service.NewStreamBuilder()
	streamOutBuilder.SetLogger(slog.New(logs))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	var (
		received []any
		mu       sync.Mutex
	)
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
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
			received = append(received, m)
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

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 1)
	}, 25*time.Second, 100*time.Millisecond)

	mu.Lock()
	received = nil
	mu.Unlock()

	// A real log signal, immediately followed by an ordinary insert. If
	// detection incorrectly paused or restarted the stream, the second
	// insert would be delayed well past the assertion window below.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('log', '{"message": "Hello World"}')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-signal')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 2)
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

	mu.Lock()
	require.ElementsMatch(t, received, []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
	mu.Unlock()
}

func TestIntegrationSignallingMalformedRowStillPublished(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)

	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling_malformed
    stream_snapshot: true
    signal_table_name: rpcn_signal_table
    schema: dbo
    tables:
      - events
`, databaseURL)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))
	require.NoError(t, streamOutBuilder.AddProcessorYAML(`mapping: 'root = @'`))

	var (
		received []any
		mu       sync.Mutex
	)
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
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
			received = append(received, m)
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

	// Wait for the initial snapshot row from dbo.events.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 1)
	}, 25*time.Second, 100*time.Millisecond)

	mu.Lock()
	received = nil
	mu.Unlock()

	// data is left NULL, so Listen fails to parse this row as a signal
	// ("expected string for ...data column, got <nil>"). That must not stop
	// it from being published like any other row.
	db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type) VALUES ('log')`)
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-malformed-signal')`)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Len(c, received, 2)
	}, 5*time.Second, 100*time.Millisecond)

	mu.Lock()
	require.ElementsMatch(t, received, []any{
		map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
		map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
	})
	mu.Unlock()
}

type testLogCapture struct {
	mu       sync.Mutex
	messages []string
}

func (c *testLogCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, r.Message)
	return nil
}

func (c *testLogCapture) Messages() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.messages...)
}

func (c *testLogCapture) WithAttrs([]slog.Attr) slog.Handler     { return c }
func (c *testLogCapture) WithGroup(string) slog.Handler          { return c }
func (*testLogCapture) Enabled(context.Context, slog.Level) bool { return true }
