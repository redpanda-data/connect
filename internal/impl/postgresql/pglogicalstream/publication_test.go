// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	incsnapshot "github.com/redpanda-data/connect/v4/internal/impl/postgresql/incrementalsnapshot"
	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestPublicationSyncPlan(t *testing.T) {
	a := TableFQN{Schema: `"public"`, Table: `"a"`}
	b := TableFQN{Schema: `"public"`, Table: `"b"`}
	c := TableFQN{Schema: `"public"`, Table: `"c"`}

	tests := []struct {
		name      string
		pub       publicationInfo
		pubTables []TableFQN
		tables    []TableFQN
		wantAdd   []TableFQN
		wantDrop  []TableFQN
		wantErr   bool
	}{
		{
			name: "FOR ALL TABLES publication without a table list is left alone",
			pub:  publicationInfo{exists: true, allTables: true},
		},
		{
			// A FOR ALL TABLES publication rejects DROP TABLE and ADD TABLE,
			// and keeping it would stream every table instead of the
			// configured ones, so a table list must fail fast.
			name:      "FOR ALL TABLES publication with a table list is an error",
			pub:       publicationInfo{exists: true, allTables: true},
			pubTables: []TableFQN{a, b, c},
			tables:    []TableFQN{a},
			wantErr:   true,
		},
		{
			// A FOR TABLE publication with every table dropped lists nothing,
			// which must not be mistaken for FOR ALL TABLES.
			name:    "empty FOR TABLE publication gets the configured tables",
			pub:     publicationInfo{exists: true},
			tables:  []TableFQN{a, b},
			wantAdd: []TableFQN{a, b},
		},
		{
			name:      "adds missing and drops unlisted",
			pub:       publicationInfo{exists: true},
			pubTables: []TableFQN{a, c},
			tables:    []TableFQN{a, b},
			wantAdd:   []TableFQN{b},
			wantDrop:  []TableFQN{c},
		},
		{
			name:      "matching publication needs nothing",
			pub:       publicationInfo{exists: true},
			pubTables: []TableFQN{a, b},
			tables:    []TableFQN{b, a},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			add, drop, err := publicationSyncPlan("pub", test.pub, test.pubTables, test.tables)
			if test.wantErr {
				require.ErrorIs(t, err, errAllTablesPublication)
				assert.ErrorContains(t, err, "self_manage_publication")
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.wantAdd, add)
			assert.Equal(t, test.wantDrop, drop)
		})
	}
}

func TestMissingFromPublicationError(t *testing.T) {
	require.NoError(t, missingFromPublicationError("pglog_stream_slot", nil))

	err := missingFromPublicationError("pglog_stream_slot", []TableFQN{
		{Schema: `"public"`, Table: `"orders"`},
		{Schema: `"public"`, Table: `"Users"`},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `publication pglog_stream_slot does not include table(s) "public"."orders", "public"."Users"`)
	assert.Contains(t, err.Error(), "ALTER PUBLICATION pglog_stream_slot ADD TABLE", "the error must say how to fix it")
}

func TestPublicationCoversTableQuery(t *testing.T) {
	q, err := publicationCoversTableQuery("pglog_stream_slot", `"public"."it's"`)
	require.NoError(t, err)
	assert.Contains(t, q, "'pglog_stream_slot'")
	// Quotes are escaped, and the table is resolved without casting, so a
	// missing table reports false instead of failing the query.
	assert.Contains(t, q, `to_regclass( '"public"."it''s"' )`)
	// A partitioned table is published under its partitions unless the
	// publication sets publish_via_partition_root.
	assert.Contains(t, q, "pg_partition_tree")
	assert.Contains(t, q, "pg_partition_ancestors")
	// Those functions return no rows for a plain table, so the table itself
	// must be matched directly.
	assert.Contains(t, q, `SELECT to_regclass( '"public"."it''s"' )::oid`)
	// Every leaf partition must be published, so a publication listing only
	// some partitions does not cover the parent.
	assert.Contains(t, q, "leaf.isleaf")
}

func TestCheckExternalPublicationVersion(t *testing.T) {
	for _, v := range []int{10, 11} {
		err := checkExternalPublicationVersion(v)
		require.ErrorIs(t, err, errExternalPublicationVersion, "PostgreSQL %d lacks pg_partition_tree", v)
		assert.Contains(t, err.Error(), fmt.Sprintf("PostgreSQL %d", v))
	}
	for _, v := range []int{12, 16} {
		require.NoError(t, checkExternalPublicationVersion(v))
	}
}

func TestPublicationMissingOperations(t *testing.T) {
	assert.Empty(t, publicationInfo{exists: true, insert: true, update: true, delete: true}.missingOperations())
	assert.Equal(t, []string{"update", "delete"}, publicationInfo{exists: true, insert: true}.missingOperations())
}

func TestPublishedAsItselfQuery(t *testing.T) {
	q, err := publishedAsItselfQuery("pglog_stream_slot", "public", "it's", `"public"."it's"`)
	require.NoError(t, err)
	// Matched on the exact name, since pg_publication_tables lists each table
	// under the name its changes stream with.
	assert.Contains(t, q, "pt.schemaname =  'public' ")
	assert.Contains(t, q, "pt.tablename =  'it''s' ")
	// Read through to_jsonb, so the query runs before PostgreSQL 15 too.
	assert.Contains(t, q, "to_jsonb(pt) ->> 'rowfilter'")
	assert.Contains(t, q, "to_jsonb(pt) -> 'attnames'")
	assert.NotContains(t, q, "pt.rowfilter")
}

func TestPublicationFiltersTableQuery(t *testing.T) {
	q, err := publicationFiltersTableQuery("pglog_stream_slot", `"public"."events"`)
	require.NoError(t, err)
	assert.Contains(t, q, "to_jsonb(pt) ->> 'rowfilter'")
	// A filter on a partition or an ancestor filters the table too.
	assert.Contains(t, q, "pg_partition_tree")
	assert.Contains(t, q, "pg_partition_ancestors")
}

func TestSnapshotSignalChecksExternallyManagedPublication(t *testing.T) {
	signalTable := incrementalsnapshot.TableID{Schema: "public", Table: "rpcn_signal"}
	events := incrementalsnapshot.TableID{Schema: "public", Table: "events"}

	newStream := func(externallyManaged bool, d *fakeQueryDriver) *Stream {
		name := fmt.Sprintf("fake_pglog_pub_%d", fakeQueryDriverSeq.Add(1))
		d.columns = []string{"attname"}
		d.rows = [][]driver.Value{{"id"}}
		sql.Register(name, d)
		db, err := sql.Open(name, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		return &Stream{
			slotName:                     "test_slot",
			signalTable:                  &signalTable,
			snapshotSchema:               "public",
			externallyManagedPublication: externallyManaged,
			logger:                       service.MockResources().Logger(),
			incSnapshot: incrementalSnapshot{
				coordinator: &incsnapshot.Coordinator{},
				conn:        db,
				pkCache:     map[string][]string{},
			},
		}
	}

	t.Run("a table missing from an externally managed publication is rejected", func(t *testing.T) {
		s := newStream(true, &fakeQueryDriver{notPublishedAsItself: true})

		err := s.checkBackfillable(t.Context(), events)
		require.ErrorIs(t, err, errSignalRejected, "rejected and logged, not a stream restart")
		assert.Contains(t, err.Error(), "publication pglog_stream_test_slot does not stream table public.events")
	})

	t.Run("a table in an externally managed publication is accepted", func(t *testing.T) {
		s := newStream(true, &fakeQueryDriver{})
		require.NoError(t, s.checkBackfillable(t.Context(), events))
	})

	t.Run("a table outside the configured tables is still rejected", func(t *testing.T) {
		s := newStream(true, &fakeQueryDriver{})
		s.incSnapshot.replicated = map[incrementalsnapshot.TableID]struct{}{
			{Schema: "public", Table: "orders"}: {},
		}
		require.ErrorIs(t, s.checkBackfillable(t.Context(), events), errSignalRejected,
			"the publication including it is not enough: the input's tables must too")
	})

	t.Run("an externally managed publication is read on every signal", func(t *testing.T) {
		d := &fakeQueryDriver{}
		s := newStream(true, d)
		require.NoError(t, s.checkBackfillable(t.Context(), events))

		// The key columns are cached by now, but the publication can change
		// while the stream runs.
		d.notPublishedAsItself = true
		require.ErrorIs(t, s.checkBackfillable(t.Context(), events), errSignalRejected)
	})

	t.Run("a connector-managed publication is checked once with the key", func(t *testing.T) {
		d := &fakeQueryDriver{}
		s := newStream(false, d)
		require.NoError(t, s.checkBackfillable(t.Context(), events))

		// The connector made the publication, so it cannot change under it.
		d.notPublishedAsItself = true
		require.NoError(t, s.checkBackfillable(t.Context(), events))
	})

	t.Run("a resumed table no longer published is dropped", func(t *testing.T) {
		s := newStream(true, &fakeQueryDriver{notPublishedAsItself: true})
		_, err := incrementalSnapshotDeps{stream: s}.ResolvePrimaryKey(t.Context(), events)
		require.ErrorIs(t, err, incrementalsnapshot.ErrTableUnusable,
			"the coordinator drops an unusable table from the queue")
	})

	t.Run("a chunk after the table leaves the publication aborts the backfill", func(t *testing.T) {
		d := &fakeQueryDriver{}
		s := newStream(true, d)
		deps := incrementalSnapshotDeps{stream: s}
		require.NoError(t, s.checkBackfillable(t.Context(), events))

		// Queued, then the table is dropped from the publication.
		d.notPublishedAsItself = true
		_, err := deps.FetchChunk(t.Context(), events, []string{"id"}, nil, incrementalsnapshot.PrimaryKey{int64(100)}, 10)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "aborting the backfill of table public.events")
		assert.NotErrorIs(t, err, incrementalsnapshot.ErrRetryable,
			"a retry would emit rows whose concurrent writes can no longer be deduplicated")
	})
}

func TestMonitorWatchesPublication(t *testing.T) {
	name := fmt.Sprintf("fake_pglog_monitor_%d", fakeQueryDriverSeq.Add(1))
	d := &fakeQueryDriver{}
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	m := &Monitor{dbConn: db, logger: service.MockResources().Logger()}

	// Nothing watched, so nothing is queried.
	var queries int
	d.queries = &queries
	m.checkPublication(t.Context())
	assert.Zero(t, queries)

	events := TableFQN{Schema: `"public"`, Table: `"events"`}
	orders := TableFQN{Schema: `"public"`, Table: `"orders"`}
	m.WatchPublication("pglog_stream_slot", []TableFQN{events, orders})

	m.checkPublication(t.Context())
	assert.Zero(t, m.Report().TablesMissingFromPublication)

	d.notInPublication = true
	m.checkPublication(t.Context())
	assert.Equal(t, int64(2), m.Report().TablesMissingFromPublication)
	assert.Equal(t, `"public"."events", "public"."orders"`, m.lastMissingFromPublication)

	d.notInPublication = false
	m.checkPublication(t.Context())
	assert.Zero(t, m.Report().TablesMissingFromPublication)
}
