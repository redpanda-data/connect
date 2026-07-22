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

func TestIntegrationSignalling(t *testing.T) {
	integration.CheckSkip(t)
	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	db.MustExec(`CREATE SCHEMA IF NOT EXISTS dbo`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.products (id SERIAL PRIMARY KEY, name TEXT)`)
	// dbo.newtable table doesn't exist in replication slot
	db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.newtable (id SERIAL PRIMARY KEY, name TEXT)`)

	// Pre-insert an events row so the snapshot phase produces a message.
	// The signal table must NOT appear in snapshot output.
	db.MustExec(`INSERT INTO dbo.events (name) VALUES ('initial')`)
	db.MustExec(`INSERT INTO dbo.products (name) VALUES ('initial')`)

	// elements accumulates the expected items across subtests; each subtest
	// appends its contribution before asserting with ElementsMatch.
	var elements []any
	elements = append(elements,
		map[string]any{"operation": "read", "table": "events"},
		map[string]any{"operation": "read", "table": "products"},
	)

	template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling
    stream_snapshot: true
    signal_table_name: rpcn_signal_table
    schema: dbo
    tables:
      - events
      - products
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

	go func() {
		if err := streamOut.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()

	t.Run("Captures initial snapshot and streaming on start up", func(t *testing.T) {
		// Wait for the initial snapshot row from dbo.events and dbo.products.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 2)
		}, 25*time.Second, 100*time.Millisecond)

		// insert streaming records
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('stream')`)
		db.MustExec(`INSERT INTO dbo.products (name) VALUES ('stream')`)

		elements = append(elements,
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "products"},
		)

		// Wait for streaming records of dbo.events and dbo.products
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 4)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "products"},
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
			map[string]any{"operation": "insert", "table": "products", "lsn": "XXX/XXX"},
		})
		mu.Unlock()
	})

	t.Run("Can signal snapshot of one table more than once in a row", func(t *testing.T) {
		mu.Lock()
		received = nil // reset to assert for this test
		mu.Unlock()

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{"data-collections": ["dbo.events"]}')`)

		// Wait for the re-snapshot to complete: the signal row is published as a
		// normal message plus two snapshot reads of the events table.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 3)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "events"},
		})
		mu.Unlock()

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{"data-collections": ["dbo.events"]}')`)

		// Wait for the second re-snapshot: another signal row plus two snapshot reads,
		// accumulated on top of the previous three.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 6)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "events"},
		})
		mu.Unlock()
	})

	t.Run("Can signal snapshot of multiple tables", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		// Insert streaming records and let them arrive via WAL before firing the
		// signal, so they don't get counted as snapshot reads.
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('evt1')`)
		db.MustExec(`INSERT INTO dbo.products (name) VALUES ('evt1')`)

		elements = append(elements,
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "products"},
		)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 2)
		}, 25*time.Second, 100*time.Millisecond)

		// Reset once WAL inserts are consumed so the snapshot reads are isolated.
		mu.Lock()
		received = nil
		mu.Unlock()

		// The signal row itself is published as a normal message alongside the
		// snapshot reads, so include it in the expected set.
		elements = append(elements, map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"})
		expected := len(elements)

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{"data-collections": ["dbo.events", "dbo.products"]}')`)

		// Wait for the re-snapshot reads: one per row across both tables.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, expected)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, elements)
		mu.Unlock()
	})

	t.Run("Resumes streaming all configured tables after snapshot", func(t *testing.T) {
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('evt2')`)
		db.MustExec(`INSERT INTO dbo.products (name) VALUES ('new2')`)

		elements = append(elements,
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
			map[string]any{"operation": "insert", "table": "products", "lsn": "XXX/XXX"},
		)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, len(elements))
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, elements)
		mu.Unlock()
	})

	t.Run("Can resnapshot new tables but not append to the replication slot", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		db.MustExec(`CREATE TABLE IF NOT EXISTS dbo.temptable (id SERIAL PRIMARY KEY, name TEXT)`)

		// Insert streaming records and let them arrive via WAL before firing the
		// signal, so they don't get counted as snapshot reads.
		db.MustExec(`INSERT INTO dbo.temptable (name) VALUES ('evt1')`)
		db.MustExec(`INSERT INTO dbo.temptable (name) VALUES ('evt2')`)

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{"data-collections": ["dbo.temptable"]}')`)

		// Wait for the signal row plus one snapshot read per temptable row.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 3)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "rpcn_signal_table", "lsn": "XXX/XXX"},
			map[string]any{"operation": "read", "table": "temptable"},
			map[string]any{"operation": "read", "table": "temptable"},
		})
		mu.Unlock()
	})

	t.Run("Ignores signal and continues streaming when data-collections is empty", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		// A signal with an empty data-collections is a no-op: no snapshot runs and
		// WAL streaming continues. The signal row is still published as a normal
		// message; only the subsequent streaming insert and the signal row arrive.
		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{}')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('post-noop')`)

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
	})

	t.Run("Drops signal row with malformed JSON data and continues streaming", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', 'not-valid-json{')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-bad-json')`)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 1)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		})
		mu.Unlock()
	})

	t.Run("Drops signal row with NULL data column and continues streaming", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		// Omitting data leaves it NULL, so row["data"].(string) fails, Listen
		// returns an error, and the row is skipped entirely by the caller.
		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type) VALUES ('execute-snapshot')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-null-data')`)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 1)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		})
		mu.Unlock()
	})

	t.Run("Drops signal row with NULL type column and continues streaming", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		// Omitting type leaves it NULL, so row["type"].(string) fails, Listen
		// returns an error, and the row is skipped entirely by the caller.
		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (data) VALUES ('{"data-collections": ["dbo.events"]}')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-null-type')`)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 1)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		})
		mu.Unlock()
	})

	t.Run("Forwards signal row with unrecognized type without triggering a re-snapshot", func(t *testing.T) {
		mu.Lock()
		received = nil
		mu.Unlock()

		db.MustExec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('unsupported-signal', '{"data-collections": ["dbo.events"]}')`)
		db.MustExec(`INSERT INTO dbo.events (name) VALUES ('after-unsupported-type')`)

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
	})

	require.NoError(t, streamOut.StopWithin(10*time.Second))
}

func TestControlSignalTableNames(t *testing.T) {
	tests := []struct {
		name            string
		dataCollections []string
		schema          string
		want            []string
	}{
		{
			name:            "empty data-collections returns nil",
			dataCollections: nil,
			schema:          "dbo",
			want:            nil,
		},
		{
			name:            "matching schema.table extracts table name",
			dataCollections: []string{"dbo.events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "multiple entries same schema",
			dataCollections: []string{"dbo.events", "dbo.products"},
			schema:          "dbo",
			want:            []string{"events", "products"},
		},
		{
			name:            "cross-schema entry is excluded",
			dataCollections: []string{"other.events"},
			schema:          "dbo",
			want:            []string{},
		},
		{
			name:            "mixed: matching and non-matching schemas",
			dataCollections: []string{"dbo.events", "other.products"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "schema match is case-insensitive",
			dataCollections: []string{"DBO.events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
		{
			name:            "entry without schema prefix is included",
			dataCollections: []string{"events"},
			schema:          "dbo",
			want:            []string{"events"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tableNamesFromSchema(tt.dataCollections, tt.schema)
			assert.Equal(t, tt.want, got)
		})
	}
}
