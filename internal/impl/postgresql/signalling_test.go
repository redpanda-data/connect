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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO:
// - Signal new snapshot after streaming has finished
// -- No tables, new single table, multiple tables (including new), table that doesn't exist
// -- Resumes streaming configured tables after snapshot

func TestIntegrationSignalling(t *testing.T) {
	t.Run("Snapshot completes, handles signal to resnapshot then continues streaming", func(t *testing.T) {
		integration.CheckSkip(t)
		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)

		_, err = db.Exec(`CREATE SCHEMA IF NOT EXISTS dbo`)
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS dbo.rpcn_signal_table (id SERIAL PRIMARY KEY, type VARCHAR(32), data TEXT)`)
		require.NoError(t, err)
		_, err = db.Exec(`CREATE TABLE IF NOT EXISTS dbo.events (id SERIAL PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)

		// Pre-insert an events row so the snapshot phase produces a message.
		// The signal table must NOT appear in snapshot output.
		_, err = db.Exec(`INSERT INTO dbo.events (name) VALUES ('initial')`)
		require.NoError(t, err)

		template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_signalling
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

		var received []any
		var mu sync.Mutex
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

		// Wait for the initial snapshot row from dbo.events.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 1)
		}, 25*time.Second, 100*time.Millisecond)

		// Insert a signal during streaming. The signal INSERT must be suppressed
		// and must not appear in received, but it must trigger a re-snapshot.
		_, err = db.Exec(`INSERT INTO dbo.rpcn_signal_table (type, data) VALUES ('execute-snapshot', '{"data-collections": ["dbo.events"]}')`)
		require.NoError(t, err)

		// Wait for the re-snapshot to complete: received gains a second read of the
		// same events row. No signal table row must appear.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 2)
		}, 25*time.Second, 100*time.Millisecond)

		// Now insert an events row. It arrives via WAL after the re-snapshot, so it
		// must appear as an insert, not a read.
		_, err = db.Exec(`INSERT INTO dbo.events (name) VALUES ('evt1')`)
		require.NoError(t, err)

		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			mu.Lock()
			defer mu.Unlock()
			assert.Len(c, received, 3)
		}, 25*time.Second, 100*time.Millisecond)

		mu.Lock()
		require.ElementsMatch(t, received, []any{
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "read", "table": "events"},
			map[string]any{"operation": "insert", "table": "events", "lsn": "XXX/XXX"},
		})
		mu.Unlock()

		require.NoError(t, streamOut.StopWithin(10*time.Second))
	})
}
