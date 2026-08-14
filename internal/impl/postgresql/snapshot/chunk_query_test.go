// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestBuildChunkQueryFirstChunkSingleColumnPK(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	query, args, err := buildChunkQuery(table, []string{"id"}, nil, incrementalsnapshot.PrimaryKey{100}, 500)
	require.NoError(t, err)

	assert.Equal(
		t,
		`SELECT * FROM "public"."orders" WHERE (ROW("id") <= ROW($1)) ORDER BY "id" ASC LIMIT 500`,
		query,
	)
	assert.Equal(t, []any{100}, args)
}

func TestBuildChunkQuerySubsequentChunkCompositePK(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "line_items"}

	query, args, err := buildChunkQuery(
		table,
		[]string{"order_id", "line_no"},
		incrementalsnapshot.PrimaryKey{5, 2},
		incrementalsnapshot.PrimaryKey{50, 9},
		250,
	)
	require.NoError(t, err)

	assert.Equal(
		t,
		`SELECT * FROM "public"."line_items" WHERE (ROW("order_id", "line_no") > ROW($1, $2) AND ROW("order_id", "line_no") <= ROW($3, $4)) ORDER BY "order_id" ASC, "line_no" ASC LIMIT 250`,
		query,
	)
	assert.Equal(t, []any{5, 2, 50, 9}, args)
}

func TestBuildChunkQueryNilUpperIsError(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	_, _, err := buildChunkQuery(table, []string{"id"}, nil, nil, 500)
	require.Error(t, err)
}

func TestBuildChunkQueryNoPKColumnsIsError(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	_, _, err := buildChunkQuery(table, nil, nil, incrementalsnapshot.PrimaryKey{1}, 500)
	require.Error(t, err)
}

func TestBuildMaxKeyQuery(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	query, err := buildMaxKeyQuery(table, []string{"id"})
	require.NoError(t, err)
	assert.Equal(t, `SELECT "id" FROM "public"."orders" ORDER BY "id" DESC LIMIT 1`, query)
}

func TestBuildMaxKeyQueryCompositePK(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "line_items"}

	query, err := buildMaxKeyQuery(table, []string{"order_id", "line_no"})
	require.NoError(t, err)
	assert.Equal(
		t,
		`SELECT "order_id", "line_no" FROM "public"."line_items" ORDER BY "order_id" DESC, "line_no" DESC LIMIT 1`,
		query,
	)
}

func TestBuildMaxKeyQueryNoPKColumnsIsError(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	_, err := buildMaxKeyQuery(table, nil)
	require.Error(t, err)
}
