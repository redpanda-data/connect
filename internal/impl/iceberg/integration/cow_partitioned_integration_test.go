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

// TestCOWPartitionedRowOperationsIntegration drives an insert -> upsert/delete
// round trip through the iceberg output configured with merge_strategy:
// copy-on-write against a PARTITIONED table (partition by region, identity
// transform), keyed on id — a column that is NOT the partition column.
//
// This is the case merge-on-read cannot serve: equality deletes are
// partition-scoped, so merge-on-read requires every partition source column to
// be an identifier field. Copy-on-write has no such constraint — it rewrites
// whole data files by filter (which matches across all partitions) and appends
// the new rows routed to their partitions by value.
//
// The single mutating batch spans multiple partitions and asserts:
//   - correct final per-partition state via DuckDB (id=1 untouched in us, id=2
//     updated in eu, id=3 deleted from eu, id=5 inserted into apac),
//   - ZERO delete files (the copy-on-write invariant), and
//   - the batch committed as an overwrite (a whole-file rewrite, not a delete
//     append).
func TestCOWPartitionedRowOperationsIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_part_ns", "cow_part_test"
	infra.CreateNamespace(t, ns)

	// Create a partitioned table up front. All columns are flat strings so the
	// copy-on-write path (validated for flat primitives) and its string merge key
	// are both satisfied. Note the schema carries NO identifier-field-ids: under
	// copy-on-write, identifier_fields are the connector-side merge key only.
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
			Operation: operation,
			// Merge key is id only — deliberately NOT the partition column, to prove
			// copy-on-write is free of the merge-on-read partition-subset constraint.
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Seed four rows spread across three partitions (us, eu, apac).
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "region": "us", "value": "one"}),
		opStructMsg("insert", map[string]any{"id": "2", "region": "eu", "value": "two"}),
		opStructMsg("insert", map[string]any{"id": "3", "region": "eu", "value": "three"}),
		opStructMsg("insert", map[string]any{"id": "4", "region": "apac", "value": "four"}),
	})

	// One mutating batch touching keys in multiple partitions:
	//   - upsert id=2 (eu): new value, same partition
	//   - delete id=3 (eu)
	//   - upsert id=5 (apac): a brand-new key in a different partition
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "2", "region": "eu", "value": "two-updated"}),
		opStructMsg("delete", map[string]any{"id": "3", "region": "eu"}),
		opStructMsg("upsert", map[string]any{"id": "5", "region": "apac", "value": "five"}),
	})

	// (a) Final state via DuckDB. Select the partition + key columns per the
	// DuckDB Iceberg projection quirk (a projection that omits them can misread).
	type row struct {
		ID     string `json:"id"`
		Region string `json:"region"`
		Value  string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, region, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))

	require.Len(t, rows, 4, "expected id=1,2,4,5 (id=3 deleted, id=2 not duplicated)")
	assert.Equal(t, row{"1", "us", "one"}, rows[0], "id=1 must be untouched in us")
	assert.Equal(t, row{"2", "eu", "two-updated"}, rows[1], "id=2 must be upserted in place in eu")
	assert.Equal(t, row{"4", "apac", "four"}, rows[2], "id=4 must be untouched in apac")
	assert.Equal(t, row{"5", "apac", "five"}, rows[3], "id=5 must be inserted into apac")

	// (b) Zero delete files. Load the committed table through the REST catalog and
	// inspect its snapshot manifests directly.
	loaded, err := client.LoadTable(ctx, tbl)
	require.NoError(t, err)

	dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
	assert.Positive(t, dataManifests, "expected at least one data manifest to inspect")
	assert.Zero(t, deleteManifests, "copy-on-write must leave zero delete manifests on a partitioned table")

	// (c) The mutating batch must have landed as an overwrite, not a delete-file
	// append — proving it was materialised the copy-on-write way.
	require.NotNil(t, loaded.CurrentSnapshot())
	assert.Equal(t, table.OpOverwrite, loaded.CurrentSnapshot().Summary.Operation,
		"the upsert+delete batch must commit as an overwrite under copy-on-write")
}
