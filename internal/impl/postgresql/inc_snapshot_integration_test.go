// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pgstream

import (
	"context"
	"encoding/json"
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

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pgtest"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// signalIncrementalSnapshot asks for a backfill of tables. It waits for the
// replication slot first: a signal inserted before the slot exists is not in
// the streamed WAL, so the connector never sees it.
func signalIncrementalSnapshot(t *testing.T, db *pgtest.TestDB, slotName string, tables ...string) {
	t.Helper()

	require.Eventually(t, func() bool {
		var found bool
		err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slotName).Scan(&found)
		return err == nil && found
	}, time.Minute, 50*time.Millisecond, "replication slot %s was never created", slotName)

	payload, err := json.Marshal(map[string]any{"tables": tables})
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO rpcn_signal (type, data) VALUES ('snapshot-execute', $1)`, string(payload))
	require.NoError(t, err)
}

type incSnapshotStream struct {
	inputYAML string
	cacheYAML string
	consume   func(context.Context, service.MessageBatch) error
	logs      *pgtest.TestLogCapture
}

func runIncSnapshotStream(t *testing.T, cfg incSnapshotStream) (stop func()) {
	t.Helper()

	builder := service.NewStreamBuilder()
	if cfg.logs != nil {
		builder.SetLogger(slog.New(cfg.logs))
	} else {
		require.NoError(t, builder.SetLoggerYAML(`level: DEBUG`))
	}
	require.NoError(t, builder.AddInputYAML(cfg.inputYAML))

	cacheYAML := cfg.cacheYAML
	if cacheYAML == "" {
		cacheYAML = "label: snap_cache\nmemory: {}"
	}
	require.NoError(t, builder.AddCacheYAML(cacheYAML))

	consume := cfg.consume
	if consume == nil {
		consume = func(context.Context, service.MessageBatch) error { return nil }
	}
	require.NoError(t, builder.AddBatchConsumerFunc(consume))

	stream, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		// Cancellation is how the harness ends the stream, not a failure.
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("stream error: %v", err)
		}
	}()

	var once sync.Once
	stop = func() {
		t.Helper()
		once.Do(func() {
			require.NoError(t, stream.StopWithin(20*time.Second))
			select {
			case <-stopped:
			case <-time.After(30 * time.Second):
				require.Fail(t, "stream did not stop in time")
			}
		})
	}

	t.Cleanup(func() {
		once.Do(func() { _ = stream.StopWithin(10 * time.Second) })
	})
	return stop
}

func TestIntegrationIncrementalSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	type incrementalSnapshotRow struct {
		id        int64
		operation string
	}

	t.Run("Concurrent Writes", func(t *testing.T) {
		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)

		// Pre-existing rows: only ever observable via the incremental snapshot
		// backfill, since the replication slot is created after these commits.
		const numPreExisting = 1000
		for range numPreExisting {
			_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ('pre', NOW())`)
			require.NoError(t, err)
		}

		template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_incremental_concurrent
    schema: public
    heartbeat_interval: 500ms
    tables:
      - flights
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 20
        checkpoint_cache: snap_cache
`, databaseURL)

		var (
			mu   sync.Mutex
			rows []incrementalSnapshotRow
		)

		stop := runIncSnapshotStream(t, incSnapshotStream{
			inputYAML: template,
			consume: func(_ context.Context, batch service.MessageBatch) error {
				mu.Lock()
				defer mu.Unlock()
				for _, msg := range batch {
					// The signal row streams like any other insert, and its
					// serial id collides with the ids under test.
					if table, _ := msg.MetaGet("table"); table == "rpcn_signal" {
						continue
					}
					data, err := msg.AsStructured()
					if err != nil {
						return err
					}
					id, err := data.(map[string]any)["id"].(json.Number).Int64()
					if err != nil {
						return err
					}
					op, _ := msg.MetaGet("operation")
					rows = append(rows, incrementalSnapshotRow{id: id, operation: op})
				}
				return nil
			},
		})

		// Ask for the backfill now the slot exists.
		signalIncrementalSnapshot(t, db, "test_slot_incremental_concurrent", "flights")

		// Wait for at least one backfill row before writing concurrently: it
		// proves the coordinator's max-PK bound is already frozen, so every
		// subsequent insert gets a serial PK above it -- immune to the
		// double-delivery race documented on Coordinator.OnStreamedRow.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(rows) >= 1
		}, 30*time.Second, 50*time.Millisecond, "did not observe any snapshot backfill rows before starting the concurrent writer")

		// Write new rows while the backfill is still running, racing chunk
		// reads against live inserts on the same table.
		const numConcurrent = 1000
		writerDone := make(chan struct{})
		// require calls FailNow, which is invalid off the test goroutine: it
		// would stop the writer early and surface as a missing-rows failure
		// instead of the insert error. Carry the error out and assert it
		// here, where closing writerDone orders the write before this read.
		var writerErr error
		go func() {
			defer close(writerDone)
			for range numConcurrent {
				if _, err := db.Exec(`INSERT INTO flights (name, created_at) VALUES ('concurrent', NOW())`); err != nil {
					writerErr = err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
		<-writerDone
		require.NoError(t, writerErr, "concurrent writer failed")

		var totalRows int64
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM flights`).Scan(&totalRows))
		require.EqualValues(t, numPreExisting+numConcurrent, totalRows)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return int64(len(rows)) >= totalRows
		}, 60*time.Second, 100*time.Millisecond, "did not observe every row from the pre-existing backfill and the concurrent writes")

		stop()

		// Every row, backfill or live, must be observed exactly once: dedup must
		// neither drop nor double-deliver.
		mu.Lock()
		defer mu.Unlock()
		counts := make(map[int64]int, len(rows))
		for _, r := range rows {
			counts[r.id]++
		}
		assert.Len(t, counts, int(totalRows), "expected exactly %d distinct rows to be observed", totalRows)
		for id, count := range counts {
			assert.Equal(t, 1, count, "row id %d observed %d times, expected exactly once", id, count)
		}
	})

	// Non-integer keys, run through the real decoders. The window keys rows
	// by the formatted key, so if the snapshot and streaming paths disagree
	// on a type no live row evicts its buffered copy and the stale snapshot
	// row lands after the update that superseded it.
	for _, keyed := range []struct {
		name    string
		table   string
		ddl     string
		insert  string
		keyCols []string
	}{
		{
			name:    "UUIDKey",
			table:   "uuid_keyed",
			ddl:     `CREATE TABLE uuid_keyed (id uuid PRIMARY KEY, name TEXT)`,
			insert:  `INSERT INTO uuid_keyed (id, name) VALUES (gen_random_uuid(), 'orig')`,
			keyCols: []string{"id"},
		},
		{
			name:    "CompositeUUIDTextKey",
			table:   "uuid_text_keyed",
			ddl:     `CREATE TABLE uuid_text_keyed (id uuid, tenant TEXT, name TEXT, PRIMARY KEY (id, tenant))`,
			insert:  `INSERT INTO uuid_text_keyed (id, tenant, name) VALUES (gen_random_uuid(), 'tenant-a', 'orig')`,
			keyCols: []string{"id", "tenant"},
		},
	} {
		t.Run("Concurrent Updates/"+keyed.name, func(t *testing.T) {
			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
			require.NoError(t, err)

			_, err = db.Exec(keyed.ddl)
			require.NoError(t, err)

			// Inserted before the slot exists, so only the snapshot can
			// deliver them.
			const numPreExisting = 400
			for range numPreExisting {
				_, err = db.Exec(keyed.insert)
				require.NoError(t, err)
			}

			template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_inc_%s
    schema: public
    heartbeat_interval: 500ms
    tables:
      - %s
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 50
        checkpoint_cache: snap_cache
`, databaseURL, keyed.table, keyed.table)

			// Key on the formatted primary key, mirroring how the dedup window
			// keys rows, so a decode mismatch shows up as extra keys as well
			// as a stale value.
			var (
				mu      sync.Mutex
				latest  = map[string]string{}
				updates int
				reads   int
			)

			stop := runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: template,
				consume: func(_ context.Context, batch service.MessageBatch) error {
					mu.Lock()
					defer mu.Unlock()
					for _, msg := range batch {
						// The signal row streams like any other insert, and its
						// serial id collides with the ids under test.
						if table, _ := msg.MetaGet("table"); table == "rpcn_signal" {
							continue
						}
						data, err := msg.AsStructured()
						if err != nil {
							return err
						}
						fields, ok := data.(map[string]any)
						if !ok {
							return fmt.Errorf("unexpected payload shape %T", data)
						}
						key := make([]string, 0, len(keyed.keyCols))
						for _, col := range keyed.keyCols {
							key = append(key, fmt.Sprint(fields[col]))
						}
						op, _ := msg.MetaGet("operation")
						switch op {
						case "read":
							reads++
						case "update":
							updates++
						}
						name, _ := fields["name"].(string)
						latest[strings.Join(key, "|")] = name
					}
					return nil
				},
			})

			// Ask for the backfill now the slot exists.
			signalIncrementalSnapshot(t, db, fmt.Sprintf("test_slot_inc_%s", keyed.table), keyed.table)

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return reads >= 1
			}, 30*time.Second, 50*time.Millisecond, "did not observe any backfill rows before updating")

			// Update every row repeatedly while the backfill runs, so updates
			// land on chunks already read, currently buffered, and not yet
			// reached.
			const updatePasses = 3
			for pass := range updatePasses {
				_, err := db.Exec(`UPDATE `+keyed.table+` SET name = $1`, fmt.Sprintf("updated-%d", pass))
				require.NoError(t, err)
			}
			finalName := fmt.Sprintf("updated-%d", updatePasses-1)

			var totalRows int64
			require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM `+keyed.table).Scan(&totalRows))
			require.EqualValues(t, numPreExisting, totalRows)

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				if int64(len(latest)) < totalRows {
					return false
				}
				for _, name := range latest {
					if name != finalName {
						return false
					}
				}
				return true
			}, 90*time.Second, 100*time.Millisecond,
				"a row never settled on its updated value, so the snapshot and streaming decoders disagreed on the primary key")

			stop()

			mu.Lock()
			defer mu.Unlock()
			// One key per row: a decode mismatch would produce two.
			require.Len(t, latest, int(totalRows))
			require.NotZero(t, updates, "no updates streamed, so dedup was not exercised")
		})
	}

	t.Run("Concurrent Updates", func(t *testing.T) {
		// Whichever order the two occur in, the consumer must end on the updated
		// value: an update committing before its chunk is read is already in the
		// snapshot's result, and one committing after must evict the buffered
		// row.

		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)

		// Not a multiple of chunk_size, so the last chunk is a partial one.
		const numPreExisting = 605 // 12 full chunks of 50, then 5
		for range numPreExisting {
			_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ('orig', NOW())`)
			require.NoError(t, err)
		}

		var minID, maxID int64
		require.NoError(t, db.QueryRow(`SELECT MIN(id), MAX(id) FROM flights`).Scan(&minID, &maxID))

		template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_incremental_collision
    schema: public
    heartbeat_interval: 500ms
    tables:
      - flights
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 50
        checkpoint_cache: snap_cache
`, databaseURL)

		// Keep the last operation and value seen per id, plus the arrival
		// order, so a stale read landing after an update is detectable.
		type observation struct {
			operation string
			name      string
		}
		var (
			mu      sync.Mutex
			latest  = map[int64]observation{}
			updates int
			reads   int
		)

		stop := runIncSnapshotStream(t, incSnapshotStream{
			inputYAML: template,
			consume: func(_ context.Context, batch service.MessageBatch) error {
				mu.Lock()
				defer mu.Unlock()
				for _, msg := range batch {
					// The signal row streams like any other insert, and its
					// serial id collides with the ids under test.
					if table, _ := msg.MetaGet("table"); table == "rpcn_signal" {
						continue
					}
					data, err := msg.AsStructured()
					if err != nil {
						return err
					}
					fields, ok := data.(map[string]any)
					if !ok {
						return fmt.Errorf("unexpected payload shape %T", data)
					}
					id, err := fields["id"].(json.Number).Int64()
					if err != nil {
						return err
					}
					name, _ := fields["name"].(string)
					op, _ := msg.MetaGet("operation")
					switch op {
					case "read":
						reads++
					case "update":
						updates++
					}
					latest[id] = observation{operation: op, name: name}
				}
				return nil
			},
		})

		// Ask for the backfill now the slot exists.
		signalIncrementalSnapshot(t, db, "test_slot_incremental_collision", "flights")

		// Wait for the backfill to start, so the max-key bound is frozen and
		// the updates below genuinely race chunk reads.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return reads >= 1
		}, 30*time.Second, 50*time.Millisecond, "did not observe any backfill rows before updating")

		// Update every row repeatedly while the backfill is mid-flight. Each
		// pass walks the key range, so updates land on chunks already read,
		// currently buffered, and not yet reached. Several passes widen the
		// window in which an update can collide with a buffered chunk.
		const updatePasses = 3
		for pass := range updatePasses {
			name := fmt.Sprintf("updated-%d", pass)
			for id := minID; id <= maxID; id++ {
				_, err := db.Exec(`UPDATE flights SET name = $1 WHERE id = $2`, name, id)
				require.NoError(t, err)
			}
		}
		finalName := fmt.Sprintf("updated-%d", updatePasses-1)

		var totalRows int64
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM flights`).Scan(&totalRows))
		require.EqualValues(t, numPreExisting, totalRows)

		// Every row must be accounted for, and settle on the updated value.
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			if int64(len(latest)) < totalRows {
				return false
			}
			for _, obs := range latest {
				if obs.name != finalName {
					return false
				}
			}
			return true
		}, 90*time.Second, 100*time.Millisecond,
			"a row never settled on its updated value, so a stale snapshot read overwrote a streamed change")

		stop()

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, latest, int(totalRows))
		// Sanity check on the race itself: if every update had been folded
		// into the snapshot reads, no update would have streamed and the
		// assertion above would pass without exercising dedup at all.
		require.NotZero(t, updates, "no updates streamed as change events; the test did not exercise deduplication")
		for id, obs := range latest {
			assert.Equal(t, finalName, obs.name, "row %d settled on %q via %q", id, obs.name, obs.operation)
		}
	})

	for _, version := range []string{"17", "16", "15", "14", "13"} {
		// On a quiet table only the heartbeat produces the COMMIT that advances
		// the snapshot, and pgoutput decodes the heartbeat message on PostgreSQL
		// 15 and later only. Run on each version, because a change to the plugin
		// options or to the heartbeat can stop the snapshot without an error.
		t.Run("QuietTable/PG"+version, func(t *testing.T) {
			t.Parallel()

			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, version)
			require.NoError(t, err)

			// Inserted before the slot exists, so only the snapshot can
			// deliver them.
			const numPreExisting = 200
			for range numPreExisting {
				_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ('quiet', NOW())`)
				require.NoError(t, err)
			}

			template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_inc_quiet_pg%s
    schema: public
    heartbeat_interval: 200ms
    tables:
      - flights
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 20
        checkpoint_cache: snap_cache
`, databaseURL, version)

			var (
				mu   sync.Mutex
				rows []incrementalSnapshotRow
			)

			stop := runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: template,
				consume: func(_ context.Context, batch service.MessageBatch) error {
					mu.Lock()
					defer mu.Unlock()
					for _, msg := range batch {
						// The signal row streams like any other insert, and its
						// serial id collides with the ids under test.
						if table, _ := msg.MetaGet("table"); table == "rpcn_signal" {
							continue
						}
						data, err := msg.AsStructured()
						if err != nil {
							return err
						}
						id, err := data.(map[string]any)["id"].(json.Number).Int64()
						if err != nil {
							return err
						}
						op, _ := msg.MetaGet("operation")
						rows = append(rows, incrementalSnapshotRow{id: id, operation: op})
					}
					return nil
				},
			})

			// Ask for the backfill now the slot exists.
			signalIncrementalSnapshot(t, db, fmt.Sprintf("test_slot_inc_quiet_pg%s", version), "flights")

			// No writes from here, so only the heartbeat advances the
			// snapshot.
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(rows) >= numPreExisting
			}, 90*time.Second, 100*time.Millisecond,
				"the snapshot did not deliver each row of a quiet table on PostgreSQL "+version)

			stop()
		})
	}

	t.Run("Resume", func(t *testing.T) {
		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)

		const numRows = 1000
		for range numRows {
			_, err = db.Exec(`INSERT INTO flights (name, created_at) VALUES ('pre', NOW())`)
			require.NoError(t, err)
		}

		// A file cache, not memory, so the checkpoint survives across the two
		// independent stream instances below, like an actual restart.
		cacheDir := t.TempDir()
		template := fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: test_slot_incremental_resume
    schema: public
    heartbeat_interval: 100ms
    tables:
      - flights
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 20
        checkpoint_cache: snap_cache_resume
`, databaseURL)
		cacheTemplate := fmt.Sprintf(`
label: snap_cache_resume
file:
  directory: '%s'`, cacheDir)

		runPartial := func(minRows int) []incrementalSnapshotRow {
			var (
				mu   sync.Mutex
				rows []incrementalSnapshotRow
			)

			stop := runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: template,
				cacheYAML: cacheTemplate,
				consume: func(_ context.Context, batch service.MessageBatch) error {
					mu.Lock()
					defer mu.Unlock()
					for _, msg := range batch {
						// The signal row streams like any other insert, and its
						// serial id collides with the ids under test.
						if table, _ := msg.MetaGet("table"); table == "rpcn_signal" {
							continue
						}
						data, err := msg.AsStructured()
						if err != nil {
							return err
						}
						id, err := data.(map[string]any)["id"].(json.Number).Int64()
						if err != nil {
							return err
						}
						op, _ := msg.MetaGet("operation")
						rows = append(rows, incrementalSnapshotRow{id: id, operation: op})
					}
					return nil
				},
			})

			// Ask for the backfill now the slot exists.
			signalIncrementalSnapshot(t, db, "test_slot_incremental_resume", "flights")

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(rows) >= minRows
			}, 60*time.Second, 20*time.Millisecond, "did not observe the minimum number of rows before stopping")

			stop()

			mu.Lock()
			defer mu.Unlock()
			return append([]incrementalSnapshotRow(nil), rows...)
		}

		// Stop early, leaving the backfill (and its checkpoint) partway through.
		firstRun := runPartial(10)
		require.NotEmpty(t, firstRun)
		require.Less(t, len(firstRun), numRows, "first run should not have completed the entire backfill; the test can't exercise resume otherwise")

		// Reuses the same slot and cache directory: a correct resume picks up
		// from the checkpoint, so none of the first run's rows should reappear.
		secondRun := runPartial(numRows - len(firstRun))

		firstIDs := make(map[int64]struct{}, len(firstRun))
		for _, r := range firstRun {
			firstIDs[r.id] = struct{}{}
		}
		for _, r := range secondRun {
			_, seenBefore := firstIDs[r.id]
			assert.False(t, seenBefore, "row id %d observed in both the first and second run; resume should not re-deliver already-observed rows", r.id)
		}

		allCounts := make(map[int64]int, numRows)
		for _, r := range firstRun {
			allCounts[r.id]++
		}
		for _, r := range secondRun {
			allCounts[r.id]++
		}

		var totalRows int64
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM flights`).Scan(&totalRows))
		require.EqualValues(t, numRows, totalRows)

		assert.Len(t, allCounts, int(totalRows), "expected exactly %d distinct rows across both runs combined", totalRows)
		for id, count := range allCounts {
			assert.Equal(t, 1, count, "row id %d observed %d times across both runs, expected exactly once", id, count)
		}
	})
}

// TestIntegrationIncrementalSnapshotPartitionedTable covers a table whose
// changes do not stream under the name its backfilled rows are buffered
// under. PostgreSQL publishes a partitioned table's changes using its leaf
// partitions' identities unless the publication sets
// publish_via_partition_root, so the window buffer never sees them: before
// the guard, a row updated while its chunk was buffered was followed by the
// stale snapshot copy, silently reverting a committed write.
func TestIntegrationIncrementalSnapshotPartitionedTable(t *testing.T) {
	integration.CheckSkip(t)

	const (
		numRows = 40
		// One chunk covering every row, so the target is certainly buffered
		// when the update lands.
		chunkSize = 100
		targetID  = 25
	)

	type event struct {
		op     string
		id     int64
		tenant string
	}

	// run starts a backfill for table, lets the caller drive writes against a
	// buffered chunk, and returns what reached the consumer plus the logs.
	run := func(t *testing.T, table, slot string, ddl []string) ([]event, *pgtest.TestLogCapture) {
		t.Helper()

		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)
		for _, stmt := range ddl {
			_, err := db.Exec(stmt)
			require.NoError(t, err, stmt)
		}
		// Committed before the slot exists, so only the backfill can see them.
		for i := 1; i <= numRows; i++ {
			_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (id, tenant) VALUES ($1, 'pre')`, table), i)
			require.NoError(t, err)
		}

		var (
			mu     sync.Mutex
			events []event
		)
		logs := pgtest.NewTestLogCapture()
		// Heartbeats long enough that no commit intervenes between the signal
		// buffering a chunk and the writes below, which is what makes the
		// race deterministic rather than lucky.

		_ = runIncSnapshotStream(t, incSnapshotStream{
			inputYAML: fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: %s
    schema: public
    heartbeat_interval: 60s
    tables:
      - %s
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: %d
        heartbeat_interval: 60s
        checkpoint_cache: snap_cache
`, databaseURL, slot, table, chunkSize),
			consume: func(_ context.Context, batch service.MessageBatch) error {
				mu.Lock()
				defer mu.Unlock()
				for _, msg := range batch {
					if tbl, _ := msg.MetaGet("table"); tbl == "rpcn_signal" {
						continue
					}
					op, _ := msg.MetaGet("operation")
					data, err := msg.AsStructured()
					if err != nil {
						return err
					}
					row, ok := data.(map[string]any)
					if !ok {
						continue
					}
					num, ok := row["id"].(json.Number)
					if !ok {
						continue
					}
					id, err := num.Int64()
					if err != nil {
						return err
					}
					tenant, _ := row["tenant"].(string)
					events = append(events, event{op: op, id: id, tenant: tenant})
				}
				return nil
			},
			logs: logs,
		})

		signalIncrementalSnapshot(t, db, slot, table)

		// A buffered chunk emits nothing, so there is no message to wait on.
		// With the heartbeat at 60s nothing else commits meanwhile.
		time.Sleep(3 * time.Second)

		_, err = db.Exec(fmt.Sprintf(`UPDATE %s SET tenant = 'updated' WHERE id = $1`, table), targetID)
		require.NoError(t, err)
		// The update's own commit only opens the window; a later one closes
		// it. This insert is that commit -- id 999 is above the frozen max
		// key, so it is streamed rather than backfilled.
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (id, tenant) VALUES (999, 'closer')`, table))
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			var updates int
			for _, e := range events {
				if e.op == "update" {
					updates++
				}
			}
			return updates >= 1
		}, 60*time.Second, 100*time.Millisecond, "the update never streamed")

		// Give the drain a chance to deliver anything it still would.
		time.Sleep(3 * time.Second)

		mu.Lock()
		defer mu.Unlock()
		return append([]event(nil), events...), logs
	}

	t.Run("an ordinary table dedups the buffered row", func(t *testing.T) {
		events, _ := run(t, "plain_events", "dedup_plain_slot", []string{
			`CREATE TABLE plain_events (id bigint PRIMARY KEY, tenant text)`,
		})

		var seq []string
		for _, e := range events {
			if e.id == targetID {
				seq = append(seq, fmt.Sprintf("%s=%s", e.op, e.tenant))
			}
		}
		require.Equal(t, []string{"update=updated"}, seq,
			"the buffered row must be dropped, leaving the update as the row's only delivery")
	})

	t.Run("a partitioned parent is rejected rather than backfilled", func(t *testing.T) {
		events, logs := run(t, "part_events", "dedup_part_slot", []string{
			`CREATE TABLE part_events (id bigint, tenant text, PRIMARY KEY (id)) PARTITION BY RANGE (id)`,
			`CREATE TABLE part_events_p1 PARTITION OF part_events FOR VALUES FROM (1) TO (10000)`,
		})

		var reads int
		for _, e := range events {
			if e.op == "read" {
				reads++
			}
		}
		assert.Zero(t, reads, "no backfill may run for a table whose changes cannot be deduplicated")

		var rejected bool
		for _, m := range logs.Messages() {
			if strings.Contains(m, "publish_via_partition_root") {
				rejected = true
				break
			}
		}
		assert.True(t, rejected, "the rejection must name the publication option, got: %v", logs.Messages())

		// And replication itself carries on: a rejected signal must not stop
		// the stream.
		var streamed bool
		for _, e := range events {
			if e.op == "update" && e.id == targetID {
				streamed = true
			}
		}
		assert.True(t, streamed, "replication must continue after the rejected signal")
	})
}

// TestIntegrationIncrementalSnapshotWarnsAboutPartitionedTables: an operator
// should learn at startup that a configured table cannot be backfilled,
// rather than discovering it when their first signal is rejected.
func TestIntegrationIncrementalSnapshotWarnsAboutPartitionedTables(t *testing.T) {
	integration.CheckSkip(t)

	// viaRoot pre-creates the publication with the option set. The connector
	// only passes options on CREATE and leaves an existing publication's
	// parameters alone, so this is how a deployment ends up with it -- and
	// the only way to have it in place before setup runs.
	start := func(t *testing.T, slot string, viaRoot bool) *pgtest.TestLogCapture {
		t.Helper()

		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)
		for _, stmt := range []string{
			`CREATE TABLE part_orders (id bigint, tenant text, PRIMARY KEY (id)) PARTITION BY RANGE (id)`,
			`CREATE TABLE part_orders_p1 PARTITION OF part_orders FOR VALUES FROM (1) TO (1000)`,
		} {
			_, err := db.Exec(stmt)
			require.NoError(t, err, stmt)
		}

		if viaRoot {
			_, err := db.Exec(fmt.Sprintf(
				`CREATE PUBLICATION pglog_stream_%s FOR TABLE part_orders WITH (publish_via_partition_root = true)`, slot))
			require.NoError(t, err)
		}

		logs := pgtest.NewTestLogCapture()

		_ = runIncSnapshotStream(t, incSnapshotStream{
			inputYAML: fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: %s
    schema: public
    heartbeat_interval: 60s
    tables:
      - part_orders
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 100
        heartbeat_interval: 60s
        checkpoint_cache: snap_cache
`, databaseURL, slot),
			consume: func(context.Context, service.MessageBatch) error {
				return nil
			},
			logs: logs,
		})

		return logs
	}

	warned := func(logs *pgtest.TestLogCapture) bool {
		for _, m := range logs.Messages() {
			if strings.Contains(m, "cannot be backfilled") && strings.Contains(m, "part_orders") {
				return true
			}
		}
		return false
	}

	t.Run("warns when the publication does not republish via the root", func(t *testing.T) {
		logs := start(t, "warn_part_slot", false)

		// No signal is sent: the warning must come from startup alone.
		require.Eventually(t, func() bool { return warned(logs) },
			60*time.Second, 100*time.Millisecond,
			"expected a startup warning naming the partitioned table, got: %v", logs.Messages())
	})

	t.Run("stays quiet when the publication republishes via the root", func(t *testing.T) {
		logs := start(t, "warn_part_slot_viaroot", true)
		require.Eventually(t, func() bool {
			for _, m := range logs.Messages() {
				if strings.Contains(m, "Incremental snapshot") {
					return true
				}
			}
			return false
		}, 60*time.Second, 100*time.Millisecond, "the snapshot never started")
		assert.False(t, warned(logs),
			"a table the publication republishes via the root can be backfilled, got: %v", logs.Messages())
	})
}

func TestIntegrationIncrementalSnapshotWarnsAboutToastedColumns(t *testing.T) {
	integration.CheckSkip(t)

	for _, tc := range []struct {
		name         string
		slot         string
		fullIdentity bool
		noToastable  bool
		wantWarning  bool
	}{
		{name: "default replica identity warns", slot: "toast_warn_slot", wantWarning: true},
		{name: "replica identity full stays quiet", slot: "toast_full_slot", fullIdentity: true},
		// Nothing can be stored out of line, so there is nothing to lose
		// even though the identity cannot recover it.
		{name: "no toastable column stays quiet", slot: "toast_none_slot", noToastable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
			require.NoError(t, err)

			ddl := []string{
				`CREATE TABLE toast_t (id bigint PRIMARY KEY, name text, big_col text)`,
				// EXTERNAL disables compression, so a large value is
				// certainly stored out of line rather than compressed back
				// inline, which is what makes attstorage meaningful here.
				`ALTER TABLE toast_t ALTER COLUMN big_col SET STORAGE EXTERNAL`,
			}
			if tc.noToastable {
				// Fixed-width columns only: attstorage is 'p' for every one,
				// so no value can ever be stored out of line.
				ddl = []string{`CREATE TABLE toast_t (id bigint PRIMARY KEY, n integer)`}
			}
			if tc.fullIdentity {
				ddl = append(ddl, `ALTER TABLE toast_t REPLICA IDENTITY FULL`)
			}
			for _, stmt := range ddl {
				_, err := db.Exec(stmt)
				require.NoError(t, err, stmt)
			}
			insert := `INSERT INTO toast_t (id, name, big_col) VALUES (1, 'pre', repeat('abcdefgh', 1024))`
			if tc.noToastable {
				insert = `INSERT INTO toast_t (id, n) VALUES (1, 42)`
			}
			_, err = db.Exec(insert)
			require.NoError(t, err)

			logs := pgtest.NewTestLogCapture()

			_ = runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: %s
    schema: public
    heartbeat_interval: 60s
    tables:
      - toast_t
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 100
        heartbeat_interval: 60s
        checkpoint_cache: snap_cache
`, databaseURL, tc.slot),
				consume: func(context.Context, service.MessageBatch) error {
					return nil
				},
				logs: logs,
			})

			signalIncrementalSnapshot(t, db, tc.slot, "toast_t")

			warned := func() bool {
				for _, m := range logs.Messages() {
					if strings.Contains(m, "REPLICA IDENTITY FULL") && strings.Contains(m, "toast_t") {
						return true
					}
				}
				return false
			}
			queued := func() bool {
				for _, m := range logs.Messages() {
					if strings.Contains(m, "signal queued") {
						return true
					}
				}
				return false
			}

			// The warning accompanies the queueing, so wait for that either
			// way rather than for the warning itself.
			require.Eventually(t, queued, 60*time.Second, 100*time.Millisecond,
				"the signal was never queued, got: %v", logs.Messages())

			if tc.wantWarning {
				assert.True(t, warned(), "expected a warning naming the table and the remedy, got: %v", logs.Messages())
			} else {
				assert.False(t, warned(), "REPLICA IDENTITY FULL recovers the value, so there is nothing to warn about, got: %v", logs.Messages())
			}
		})
	}

	// Its own case rather than a row in the table above: it needs two
	// stream lifetimes and a cache that survives between them.
	t.Run("warns again on resume with no signal", func(t *testing.T) {
		databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
		require.NoError(t, err)
		for _, stmt := range []string{
			`CREATE TABLE toast_r (id bigint PRIMARY KEY, name text, big_col text)`,
			`ALTER TABLE toast_r ALTER COLUMN big_col SET STORAGE EXTERNAL`,
		} {
			_, err := db.Exec(stmt)
			require.NoError(t, err, stmt)
		}
		for i := 1; i <= 5; i++ {
			_, err := db.Exec(`INSERT INTO toast_r (id, name, big_col) VALUES ($1, 'pre', repeat('abcdefgh', 1024))`, i)
			require.NoError(t, err)
		}

		// A file cache so the checkpoint survives the restart; the memory cache
		// the other tests use is per-stream.
		cacheDir := t.TempDir()

		// Heartbeats long enough that no commit closes the first chunk's window,
		// so the backfill cannot finish before the restart and the checkpoint
		// still holds the table.
		run := func(t *testing.T, signal bool) *pgtest.TestLogCapture {
			t.Helper()

			logs := pgtest.NewTestLogCapture()

			stop := runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: toast_resume_slot
    schema: public
    heartbeat_interval: 60s
    tables:
      - toast_r
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 100
        heartbeat_interval: 60s
        checkpoint_cache: snap_cache
`, databaseURL),
				cacheYAML: fmt.Sprintf(`
label: snap_cache
file:
    directory: %s`, cacheDir),
				consume: func(context.Context, service.MessageBatch) error {
					return nil
				},
				logs: logs,
			})

			if signal {
				signalIncrementalSnapshot(t, db, "toast_resume_slot", "toast_r")
				// The signal's own commit checkpoints the queue before planning
				// a chunk, so wait for that to be written.
				require.Eventually(t, func() bool {
					for _, m := range logs.Messages() {
						if strings.Contains(m, "signal queued") {
							return true
						}
					}
					return false
				}, 60*time.Second, 100*time.Millisecond, "the signal was never queued, got: %v", logs.Messages())
				time.Sleep(2 * time.Second)
			} else {
				require.Eventually(t, func() bool {
					for _, m := range logs.Messages() {
						if strings.Contains(m, "loaded checkpoint") || strings.Contains(m, "Incremental snapshot") {
							return true
						}
					}
					return false
				}, 60*time.Second, 100*time.Millisecond, "the snapshot never started, got: %v", logs.Messages())
				time.Sleep(2 * time.Second)
			}

			stop()
			return logs
		}

		warned := func(logs *pgtest.TestLogCapture) bool {
			for _, m := range logs.Messages() {
				if strings.Contains(m, "REPLICA IDENTITY FULL") && strings.Contains(m, "toast_r") {
					return true
				}
			}
			return false
		}

		first := run(t, true)
		require.True(t, warned(first), "the signalled run must warn, got: %v", first.Messages())

		// No signal on the second run: the queue comes back from the checkpoint.
		second := run(t, false)
		for _, m := range second.Messages() {
			if strings.Contains(m, "signal queued") {
				require.Fail(t, "the second run must not process a signal", "got: %v", second.Messages())
			}
		}
		assert.True(t, warned(second),
			"a resumed backfill must warn again, or nothing reports it after a restart, got: %v", second.Messages())
	})
}

func TestIntegrationIncrementalSnapshotPKChangingUpdate(t *testing.T) {
	integration.CheckSkip(t)

	for _, tc := range []struct {
		name         string
		slot         string
		fullIdentity bool
	}{
		// The old tuple is sent for a key change under either identity:
		// key columns only by default, the whole row with FULL.
		{name: "default replica identity", slot: "pkmove_slot"},
		{name: "replica identity full", slot: "pkmove_full_slot", fullIdentity: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
			require.NoError(t, err)
			_, err = db.Exec(`CREATE TABLE pkmove (id bigint PRIMARY KEY, name text)`)
			require.NoError(t, err)
			if tc.fullIdentity {
				_, err = db.Exec(`ALTER TABLE pkmove REPLICA IDENTITY FULL`)
				require.NoError(t, err)
			}
			const rows = 5
			for i := 1; i <= rows; i++ {
				_, err := db.Exec(`INSERT INTO pkmove (id, name) VALUES ($1, 'pre')`, i)
				require.NoError(t, err)
			}

			type event struct {
				op string
				id int64
			}
			var (
				mu      sync.Mutex
				emitted []event
			)

			// One chunk covering every row, heartbeats long enough that no
			// commit closes its window before the update below.

			_ = runIncSnapshotStream(t, incSnapshotStream{
				inputYAML: fmt.Sprintf(`
postgres_cdc:
    dsn: %s
    slot_name: %s
    schema: public
    heartbeat_interval: 60s
    tables:
      - pkmove
    signal_table_name: rpcn_signal
    incremental_snapshot:
        enabled: true
        chunk_size: 100
        heartbeat_interval: 60s
        checkpoint_cache: snap_cache
`, databaseURL, tc.slot),
				consume: func(_ context.Context, batch service.MessageBatch) error {
					mu.Lock()
					defer mu.Unlock()
					for _, msg := range batch {
						if tbl, _ := msg.MetaGet("table"); tbl != "pkmove" {
							continue
						}
						op, _ := msg.MetaGet("operation")
						data, err := msg.AsStructured()
						if err != nil {
							return err
						}
						row, ok := data.(map[string]any)
						if !ok {
							continue
						}
						num, ok := row["id"].(json.Number)
						if !ok {
							continue
						}
						id, err := num.Int64()
						if err != nil {
							return err
						}
						emitted = append(emitted, event{op: op, id: id})
					}
					return nil
				},
			})

			signalIncrementalSnapshot(t, db, tc.slot, "pkmove")
			// A buffered chunk emits nothing, so there is no message to wait
			// on; with a 60s heartbeat nothing else commits meanwhile.
			time.Sleep(3 * time.Second)

			// Move id 3 above the frozen max key: the new key was never
			// buffered, the old one was.
			_, err = db.Exec(`UPDATE pkmove SET id = 100 WHERE id = 3`)
			require.NoError(t, err)
			// The update's commit only opens the window; this one closes it.
			_, err = db.Exec(`INSERT INTO pkmove (id, name) VALUES (999, 'closer')`)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				var reads int
				for _, e := range emitted {
					if e.op == "read" {
						reads++
					}
				}
				// Every row but the vacated one, or all of them if the
				// eviction is missing.
				return reads >= rows-1
			}, 60*time.Second, 100*time.Millisecond, "the backfill did not drain")
			time.Sleep(2 * time.Second)

			mu.Lock()
			defer mu.Unlock()

			var readIDs []int64
			for _, e := range emitted {
				if e.op == "read" {
					readIDs = append(readIDs, e.id)
				}
			}
			assert.NotContains(t, readIDs, int64(3),
				"the key the update vacated must not be released as a read; it would resurrect a row the source no longer has")
			assert.ElementsMatch(t, []int64{1, 2, 4, 5}, readIDs,
				"every untouched row must still be backfilled")
		})
	}
}
