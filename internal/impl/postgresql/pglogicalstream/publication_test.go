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
	}{
		{
			// pg_publication_tables lists every table for a FOR ALL TABLES
			// publication, which rejects DROP TABLE and ADD TABLE, so the
			// list must not be reconciled against the configured tables.
			name:      "FOR ALL TABLES publication is left alone",
			pub:       publicationInfo{exists: true, allTables: true},
			pubTables: []TableFQN{a, b, c},
			tables:    []TableFQN{a},
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
			add, drop := publicationSyncPlan(test.pub, test.pubTables, test.tables)
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
}

func TestSnapshotSignalChecksExternallyManagedPublication(t *testing.T) {
	signalTable := incrementalsnapshot.TableID{Schema: "public", Table: "rpcn_signal"}
	events := incrementalsnapshot.TableID{Schema: "public", Table: "events"}

	newStream := func(externallyManaged, notInPublication bool, queries *int) *Stream {
		name := fmt.Sprintf("fake_pglog_pub_%d", fakeQueryDriverSeq.Add(1))
		sql.Register(name, &fakeQueryDriver{
			columns:          []string{"attname"},
			rows:             [][]driver.Value{{"id"}},
			notInPublication: notInPublication,
			queries:          queries,
		})
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
		s := newStream(true, true, nil)

		err := s.checkBackfillable(t.Context(), events)
		require.ErrorIs(t, err, errSignalRejected, "rejected and logged, not a stream restart")
		assert.Contains(t, err.Error(), "not in publication pglog_stream_test_slot")
	})

	t.Run("a table in an externally managed publication is accepted", func(t *testing.T) {
		s := newStream(true, false, nil)
		require.NoError(t, s.checkBackfillable(t.Context(), events))
	})

	t.Run("a table outside the configured tables is still rejected", func(t *testing.T) {
		s := newStream(true, false, nil)
		s.incSnapshot.replicated = map[incrementalsnapshot.TableID]struct{}{
			{Schema: "public", Table: "orders"}: {},
		}
		require.ErrorIs(t, s.checkBackfillable(t.Context(), events), errSignalRejected,
			"the publication including it is not enough: the input's tables must too")
	})

	t.Run("a connector-managed publication is not queried", func(t *testing.T) {
		var queries int
		require.NoError(t, newStream(false, true, &queries).checkInPublication(t.Context(), events))
		assert.Zero(t, queries, "the connector made the publication list exactly its tables")
	})
}
