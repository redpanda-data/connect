// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.wantAdd, add)
			assert.Equal(t, test.wantDrop, drop)
		})
	}
}

func TestPublishedAsItselfQuery(t *testing.T) {
	q, err := publishedAsItselfQuery(15, "pglog_stream_slot", "public", "it's", `"public"."it's"`)
	require.NoError(t, err)
	// Matched on the exact name, since pg_publication_tables lists each table
	// under the name its changes stream with.
	assert.Contains(t, q, "pt.schemaname =  'public' ")
	assert.Contains(t, q, "pt.tablename =  'it''s' ")
	// Read through to_jsonb, so the query runs before PostgreSQL 15 too.
	assert.Contains(t, q, "to_jsonb(pt) ->> 'rowfilter'")
	assert.Contains(t, q, "to_jsonb(pt) -> 'attnames'")
	assert.NotContains(t, q, "pt.rowfilter")
	assert.Contains(t, q, "attgenerated")
}

func TestPublishedAsItselfQueryBeforeColumnLists(t *testing.T) {
	// pg_attribute.attgenerated only exists from PostgreSQL 12, and a query
	// naming it fails to parse on 10 and 11 even when the branch never runs.
	for _, v := range []int{10, 11, 12, 14} {
		q, err := publishedAsItselfQuery(v, "pglog_stream_slot", "public", "events", `"public"."events"`)
		require.NoError(t, err)
		assert.NotContains(t, q, "attgenerated", "PostgreSQL %d", v)
		assert.NotContains(t, q, "pg_attribute", "PostgreSQL %d", v)
		assert.Contains(t, q, "to_jsonb(pt) ->> 'rowfilter'", "PostgreSQL %d", v)
	}
}
