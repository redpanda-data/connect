// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

func TestBuildChunkQuery(t *testing.T) {
	orders := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}
	lineItems := incrementalsnapshot.TableID{Schema: "public", Table: "line_items"}

	t.Run("first chunk of a single-column key", func(t *testing.T) {
		query, args, err := BuildChunkQuery(orders, []string{"id"}, nil, incrementalsnapshot.PrimaryKey{100}, 500)
		require.NoError(t, err)

		assert.Equal(
			t,
			`SELECT * FROM "public"."orders" WHERE (ROW("id") <= ROW($1)) ORDER BY "id" ASC LIMIT 500`,
			query,
		)
		assert.Equal(t, []any{100}, args)
	})

	t.Run("later chunk of a composite key", func(t *testing.T) {
		query, args, err := BuildChunkQuery(
			lineItems,
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
	})

	t.Run("nil upper bound is an error", func(t *testing.T) {
		_, _, err := BuildChunkQuery(orders, []string{"id"}, nil, nil, 500)
		require.Error(t, err)
	})

	t.Run("no key columns is an error", func(t *testing.T) {
		_, _, err := BuildChunkQuery(orders, nil, nil, incrementalsnapshot.PrimaryKey{1}, 500)
		require.Error(t, err)
	})
}

func TestBuildMaxKeyQuery(t *testing.T) {
	orders := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}
	lineItems := incrementalsnapshot.TableID{Schema: "public", Table: "line_items"}

	t.Run("single-column key", func(t *testing.T) {
		query, err := BuildMaxKeyQuery(orders, []string{"id"})
		require.NoError(t, err)
		assert.Equal(t, `SELECT "id" FROM "public"."orders" ORDER BY "id" DESC LIMIT 1`, query)
	})

	t.Run("composite key", func(t *testing.T) {
		query, err := BuildMaxKeyQuery(lineItems, []string{"order_id", "line_no"})
		require.NoError(t, err)
		assert.Equal(
			t,
			`SELECT "order_id", "line_no" FROM "public"."line_items" ORDER BY "order_id" DESC, "line_no" DESC LIMIT 1`,
			query,
		)
	})

	t.Run("no key columns is an error", func(t *testing.T) {
		_, err := BuildMaxKeyQuery(orders, nil)
		require.Error(t, err)
	})
}
