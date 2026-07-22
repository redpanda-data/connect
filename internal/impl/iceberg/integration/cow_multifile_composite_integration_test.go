// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWMultiFileAndCompositeIntegration bundles three lower-risk copy-on-write
// e2e checks that share one infra:
//
//   - multifile: seed several separate data files, then a mutating batch that
//     touches keys in only one file; the untouched files' rows must survive
//     unchanged (a whole-file rewrite must be scoped to the matched files).
//   - composite: a two-column merge key round trip, exercising buildCOWFilter's
//     OR-of-per-tuple-ANDs filter shape rather than the single-column IN.
//   - collapse: two upserts to the same key in one batch must yield a single
//     (latest-wins) row, exercising the same-batch per-key collapse under the
//     rewrite path.
//
// Each asserts the final state via DuckDB plus the copy-on-write invariant
// (zero delete manifests + overwrite operation).
func TestCOWMultiFileAndCompositeIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	newRouter := func(t *testing.T, ns, tbl string, idFields ...string) *icebergimpl.Router {
		operation, err := service.NewInterpolatedString(`${! meta("op") }`)
		require.NoError(t, err)
		return infra.NewRouter(t, ns, tbl, WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: idFields,
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))
	}

	t.Run("multifile", func(t *testing.T) {
		const ns, tbl = "cow_multifile_ns", "cow_multifile_test"
		infra.CreateNamespace(t, ns)
		client := infra.NewCatalogClient(t, ns)
		_, err := client.CreateTable(ctx, tbl, iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.StringType{}, Required: false},
		))
		require.NoError(t, err)

		router := newRouter(t, ns, tbl, "id")

		// Three separate seed batches -> three separate data files. ids are
		// zero-padded so their string min/max cleanly separates the files.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "01", "value": "one"}),
			opStructMsg("insert", map[string]any{"id": "02", "value": "two"}),
			opStructMsg("insert", map[string]any{"id": "03", "value": "three"}),
		})
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "04", "value": "four"}),
			opStructMsg("insert", map[string]any{"id": "05", "value": "five"}),
			opStructMsg("insert", map[string]any{"id": "06", "value": "six"}),
		})
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "07", "value": "seven"}),
			opStructMsg("insert", map[string]any{"id": "08", "value": "eight"}),
			opStructMsg("insert", map[string]any{"id": "09", "value": "nine"}),
		})

		// Mutate keys only in the middle file: upsert id=05, delete id=06. The
		// first and third files must be left entirely intact.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("upsert", map[string]any{"id": "05", "value": "five-updated"}),
			opStructMsg("delete", map[string]any{"id": "06"}),
		})

		type row struct {
			ID    string `json:"id"`
			Value string `json:"value"`
		}
		rows := querySQL[row](t, ctx, infra,
			fmt.Sprintf(`SELECT id, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
		require.Len(t, rows, 8, "id=06 deleted, id=05 updated in place, all others intact")
		assert.Equal(t, []row{
			{"01", "one"},
			{"02", "two"},
			{"03", "three"},
			{"04", "four"},
			{"05", "five-updated"},
			{"07", "seven"},
			{"08", "eight"},
			{"09", "nine"},
		}, rows, "untouched files' rows must survive the scoped rewrite unchanged")

		assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
	})

	t.Run("composite", func(t *testing.T) {
		const ns, tbl = "cow_composite_ns", "cow_composite_test"
		infra.CreateNamespace(t, ns)
		client := infra.NewCatalogClient(t, ns)
		_, err := client.CreateTable(ctx, tbl, iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "tenant", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 3, Name: "val", Type: iceberg.StringType{}, Required: false},
		))
		require.NoError(t, err)

		router := newRouter(t, ns, tbl, "tenant", "id")

		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"tenant": "t1", "id": "x", "val": "a"}),
			opStructMsg("insert", map[string]any{"tenant": "t1", "id": "y", "val": "b"}),
			opStructMsg("insert", map[string]any{"tenant": "t2", "id": "x", "val": "c"}),
		})
		// delete (t1,y); upsert (t2,x). (t1,x) untouched. The composite filter must
		// distinguish (t1,x) from (t2,x) — an AND-of-INs would match the cross
		// product and wrongly touch (t1,x).
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("delete", map[string]any{"tenant": "t1", "id": "y"}),
			opStructMsg("upsert", map[string]any{"tenant": "t2", "id": "x", "val": "c2"}),
		})

		type row struct {
			Tenant string `json:"tenant"`
			ID     string `json:"id"`
			Val    string `json:"val"`
		}
		rows := querySQL[row](t, ctx, infra,
			fmt.Sprintf(`SELECT tenant, id, val FROM iceberg_cat."%s"."%s" ORDER BY tenant, id;`, ns, tbl))
		require.Len(t, rows, 2)
		assert.Equal(t, row{"t1", "x", "a"}, rows[0], "(t1,x) must be untouched")
		assert.Equal(t, row{"t2", "x", "c2"}, rows[1], "(t2,x) must be upserted, not confused with (t1,x)")

		assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
	})

	t.Run("collapse", func(t *testing.T) {
		const ns, tbl = "cow_collapse_ns", "cow_collapse_test"
		infra.CreateNamespace(t, ns)
		client := infra.NewCatalogClient(t, ns)
		_, err := client.CreateTable(ctx, tbl, iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.StringType{}, Required: false},
		))
		require.NoError(t, err)

		router := newRouter(t, ns, tbl, "id")

		// Seed k and g so the mutating batch genuinely rewrites existing rows and
		// commits as an overwrite (an overwrite whose filter matches no existing
		// data is optimised into a plain append by iceberg-go).
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "k", "val": "seed"}),
			opStructMsg("insert", map[string]any{"id": "g", "val": "seed"}),
		})

		// Two upserts of "k" plus an upsert-then-delete of "g", all in one batch.
		// The per-key collapse must leave one row for k (latest wins) and none for g.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("upsert", map[string]any{"id": "k", "val": "v1"}),
			opStructMsg("upsert", map[string]any{"id": "k", "val": "v2"}),
			opStructMsg("upsert", map[string]any{"id": "g", "val": "g1"}),
			opStructMsg("delete", map[string]any{"id": "g"}),
		})

		type row struct {
			ID  string `json:"id"`
			Val string `json:"val"`
		}
		rows := querySQL[row](t, ctx, infra,
			fmt.Sprintf(`SELECT id, val FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
		require.Len(t, rows, 1, "k must appear once (two same-batch upserts collapse); g must be deleted")
		assert.Equal(t, row{"k", "v2"}, rows[0], "the later upsert of k must win")

		assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
	})
}
