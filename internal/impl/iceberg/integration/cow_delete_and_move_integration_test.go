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
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWDeleteOnlyBatchIntegration drives a mutating batch that is ALL deletes
// through copy-on-write, exercising commitOverwrite's txn.Delete branch
// (input.NewReader == nil) end-to-end against a real catalog.
//
// A delete-only copy-on-write mutation rewrites the surviving rows of every
// touched data file and commits a data-only snapshot; iceberg-go stamps this as
// an OpDelete snapshot (not OpOverwrite — only txn.Overwrite yields the latter),
// so this test asserts OpDelete while still requiring the copy-on-write
// invariant of zero delete-content manifests. DuckDB confirms the surviving row.
func TestCOWDeleteOnlyBatchIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_delonly_ns", "cow_delonly_test"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	_, err := client.CreateTable(ctx, tbl, iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.StringType{}, Required: false},
	))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Seed three rows in one data file.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "value": "one"}),
		opStructMsg("insert", map[string]any{"id": "2", "value": "two"}),
		opStructMsg("insert", map[string]any{"id": "3", "value": "three"}),
	})

	// One batch of ONLY deletes: remove id=2 and id=3. No rows to write, so
	// writeCOW takes the delete-only branch (txn.Delete over the filter).
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("delete", map[string]any{"id": "2"}),
		opStructMsg("delete", map[string]any{"id": "3"}),
	})

	type row struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
	require.Len(t, rows, 1, "only id=1 must survive an all-deletes batch")
	assert.Equal(t, row{"1", "one"}, rows[0])

	// Copy-on-write delete-only invariant: zero delete manifests, OpDelete op.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpDelete)
}

// TestCOWCrossPartitionKeyMoveIntegration verifies that a copy-on-write upsert
// which changes an existing key's partition value MOVES the row across
// partitions: the old-partition copy must be gone and the row must land in the
// new partition. This is distinct from inserting a brand-new key, and is only
// tractable under copy-on-write because the merge key (id) is not the partition
// column (region) — the rewrite deletes the old row by filter across all
// partitions and re-appends it routed by its new partition value.
func TestCOWCrossPartitionKeyMoveIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_move_ns", "cow_move_test"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.StringType{}, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	_, err := client.CreateTable(ctx, tbl, sc, catalog.WithPartitionSpec(&spec))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"}, // merge key is NOT the partition column
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Seed: id=1 in us, id=2 in eu.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "region": "us", "value": "one"}),
		opStructMsg("insert", map[string]any{"id": "2", "region": "eu", "value": "two"}),
	})

	// Upsert id=1 changing its region us -> eu: the SAME key moves partitions.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "1", "region": "eu", "value": "one-moved"}),
	})

	// Full state: id=1 now in eu, id=2 still in eu, and NO id=1 left in us.
	type row struct {
		ID     string `json:"id"`
		Region string `json:"region"`
		Value  string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, region, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
	require.Len(t, rows, 2, "id=1 must move (not duplicate); id=2 untouched")
	assert.Equal(t, row{"1", "eu", "one-moved"}, rows[0], "id=1 must now live in eu with the new value")
	assert.Equal(t, row{"2", "eu", "two"}, rows[1])

	// The old-partition copy must be gone: no id=1 remains in us.
	usID1 := querySQL[countResult](t, ctx, infra,
		fmt.Sprintf(`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s" WHERE region = 'us';`, ns, tbl))
	require.Len(t, usID1, 1)
	assert.Equal(t, 0, usID1[0].Count, "the old us-partition copy of id=1 must be gone after the move")

	// Copy-on-write invariant.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
}
