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
		wantErr   error
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
			wantErr:   errAllTablesPublication,
		},
		{
			// A FOR TABLES IN SCHEMA publication streams every table in the
			// schema, which pg_publication_rel does not list.
			name:      "schema publication with a table list is an error",
			pub:       publicationInfo{exists: true, schemas: []string{"public"}},
			pubTables: []TableFQN{a},
			tables:    []TableFQN{a},
			wantErr:   errSchemaPublication,
		},
		{
			name: "schema publication without a table list is left alone",
			pub:  publicationInfo{exists: true, schemas: []string{"public"}},
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
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.wantAdd, add)
			assert.Equal(t, test.wantDrop, drop)
		})
	}
}

func TestPublicationSyncPlanErrorsGiveSafeFix(t *testing.T) {
	a := TableFQN{Schema: `"public"`, Table: `"a"`}
	b := TableFQN{Schema: `"public"`, Table: `"b"`}

	// Dropping the publication and creating it in separate transactions
	// leaves changes that commit in between decoded with no publication, so
	// the fix recreates it in one transaction with its own options.
	_, _, err := publicationSyncPlan("pub", publicationInfo{exists: true, allTables: true, withClause: "publish = 'insert, update', publish_via_partition_root = true"}, nil, []TableFQN{a, b})
	require.ErrorIs(t, err, errAllTablesPublication)
	assert.Contains(t, err.Error(), `BEGIN; DROP PUBLICATION pub; CREATE PUBLICATION pub FOR TABLE "public"."a", "public"."b" WITH (publish = 'insert, update', publish_via_partition_root = true); COMMIT;`)

	// Dropping the schemas before adding the tables leaves changes to the
	// tables that commit in between decoded with the tables in no
	// publication, so the fix swaps them in one statement.
	_, _, err = publicationSyncPlan("pub", publicationInfo{exists: true, schemas: []string{"public", "Sales"}}, nil, []TableFQN{a, b})
	require.ErrorIs(t, err, errSchemaPublication)
	assert.Contains(t, err.Error(), `ALTER PUBLICATION pub SET TABLE "public"."a", "public"."b";`)
	assert.NotContains(t, err.Error(), "DROP")
}

func TestUnpublishedTableQueries(t *testing.T) {
	q, err := unpublishedTableQuery("pglog_stream_slot", `"public"."it's"`)
	require.NoError(t, err)
	// Read through to_jsonb, so the query runs before PostgreSQL 13 too.
	assert.Contains(t, q, "to_jsonb(p) ->> 'pubviaroot'")
	assert.Contains(t, q, `to_regclass( '"public"."it''s"' )`)

	for _, build := range []func(string, string) (string, error){publishedAncestorQuery, publishedPartitionsQuery} {
		q, err := build("pglog_stream_slot", `"public"."it's"`)
		require.NoError(t, err)
		assert.Contains(t, q, "pt.pubname =  'pglog_stream_slot' ")
		assert.Contains(t, q, "format('%I.%I', pt.schemaname, pt.tablename)")
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
