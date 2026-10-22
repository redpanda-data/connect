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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pgtest"
)

// TestIntegrationExternallyManagedPublicationWithoutOwnership runs the input
// with manage_publication: false as a replication role that owns no table,
// may only read the replicated ones, and cannot create anything in the
// database. That is the setup manage_publication exists for, so every
// statement the input issues on the way -- slot creation, the snapshot, the
// signal table, heartbeats and an incremental snapshot -- must work with
// those privileges alone.
func TestIntegrationExternallyManagedPublicationWithoutOwnership(t *testing.T) {
	integration.CheckSkip(t)

	databaseURL, db, err := ResourceWithPostgreSQLVersion(t, "16")
	require.NoError(t, err)

	const (
		slot        = "unowned_slot"
		publication = "pglog_stream_" + slot
		preRows     = 5
	)
	for _, stmt := range []string{
		`CREATE TABLE owned_events (id bigint PRIMARY KEY, note text)`,
		`CREATE TABLE other_events (id bigint PRIMARY KEY, note text)`,
		fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE owned_events, other_events, rpcn_signal`, publication),
		`CREATE ROLE cdc_reader WITH LOGIN REPLICATION PASSWORD 'reader'`,
		`GRANT SELECT ON owned_events, other_events, rpcn_signal TO cdc_reader`,
		`REVOKE CREATE ON DATABASE dbname FROM PUBLIC`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
	} {
		db.MustExec(t, stmt)
	}
	for i := 1; i <= preRows; i++ {
		db.MustExec(t, `INSERT INTO owned_events (id, note) VALUES ($1, 'pre')`, i)
	}

	// The role must really lack the privileges the managed mode needs.
	var owns, canCreate bool
	require.NoError(t, db.QueryRow(`SELECT pg_has_role('cdc_reader', (SELECT relowner FROM pg_class WHERE oid = 'owned_events'::regclass), 'USAGE')`).Scan(&owns))
	require.NoError(t, db.QueryRow(`SELECT has_database_privilege('cdc_reader', 'dbname', 'CREATE')`).Scan(&canCreate))
	require.False(t, owns)
	require.False(t, canCreate)

	members := func() []string {
		t.Helper()
		rows, err := db.Query(`
			SELECT c.relname
			FROM   pg_publication_rel pr
			JOIN   pg_class c ON c.oid = pr.prrelid
			WHERE  pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = $1)
			ORDER BY c.relname`, publication)
		require.NoError(t, err)
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			names = append(names, name)
		}
		require.NoError(t, rows.Err())
		return names
	}
	before := members()
	require.Equal(t, []string{"other_events", "owned_events", "rpcn_signal"}, before)

	readerURL := strings.Replace(databaseURL, "user=user_name password=l]YLSc|4[i56%{gY", "user=cdc_reader password=reader", 1)
	require.NotEqual(t, databaseURL, readerURL)

	type event struct {
		op, table, note string
	}
	var (
		mu     sync.Mutex
		events []event
	)
	count := func(op, table string) int {
		mu.Lock()
		defer mu.Unlock()
		var n int
		for _, e := range events {
			if e.op == op && e.table == table {
				n++
			}
		}
		return n
	}

	consume := func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			op, _ := msg.MetaGet("operation")
			table, _ := msg.MetaGet("table")
			data, err := msg.AsStructured()
			if err != nil {
				continue
			}
			row, _ := data.(map[string]any)
			note, _ := row["note"].(string)
			events = append(events, event{op: op, table: table, note: note})
		}
		return nil
	}
	const common = `
postgres_cdc:
    dsn: %s
    slot_name: %s
    schema: public
    manage_publication: false
    pg_wal_monitor_interval: 1s
    heartbeat_interval: 1s
    tables:
      - owned_events
      - other_events
    signal_table_name: rpcn_signal
`

	// The first run takes the initial snapshot, which creates the slot and
	// reads every table in an exported snapshot.
	logs := pgtest.NewTestLogCapture()
	stop := runIncSnapshotStream(t, incSnapshotStream{
		inputYAML: fmt.Sprintf(common+"    stream_snapshot: true\n", readerURL, slot),
		logs:      logs,
		consume:   consume,
	})
	require.Eventually(t, func() bool { return count("read", "owned_events") >= preRows },
		time.Minute, 100*time.Millisecond, "the initial snapshot never arrived: %v", logs.Messages())

	for i := 1; i <= 3; i++ {
		db.MustExec(t, `INSERT INTO owned_events (id, note) VALUES ($1, 'live')`, 100+i)
	}
	require.Eventually(t, func() bool { return count("insert", "owned_events") >= 3 },
		time.Minute, 100*time.Millisecond, "streamed inserts never arrived: %v", logs.Messages())
	stop()
	assert.Equal(t, before, members())

	// The second run reconnects to the slot with the incremental snapshot
	// enabled, which reads the table again through the same role.
	runLogs := logs.Messages()
	logs = pgtest.NewTestLogCapture()
	_ = runIncSnapshotStream(t, incSnapshotStream{
		inputYAML: fmt.Sprintf(common+`    incremental_snapshot:
        enabled: true
        chunk_size: 2
        heartbeat_interval: 1s
        checkpoint_cache: snap_cache
`, readerURL, slot),
		logs:    logs,
		consume: consume,
	})
	readsBefore := count("read", "owned_events")
	signalIncrementalSnapshot(t, db, slot, "owned_events")
	require.Eventually(t, func() bool { return count("read", "owned_events") >= readsBefore+preRows+3 },
		time.Minute, 100*time.Millisecond, "the incremental snapshot never completed: %v", logs.Messages())

	// A table dropped from the publication mid-run is reported, rather than
	// silently going quiet until the next reconnect.
	db.MustExec(t, fmt.Sprintf(`ALTER PUBLICATION %s DROP TABLE other_events`, publication))
	require.Eventually(t, func() bool {
		for _, m := range logs.Messages() {
			if strings.Contains(m, "no longer includes table(s)") && strings.Contains(m, `"public"."other_events"`) {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond, "the dropped table was never reported: %v", logs.Messages())
	db.MustExec(t, fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE other_events`, publication))

	// The input never altered the publication: only the test did, and it put
	// back what it took out.
	assert.Equal(t, before, members())

	for _, m := range append(runLogs, logs.Messages()...) {
		assert.NotContains(t, m, "permission denied", "every statement must work without ownership")
	}
}
