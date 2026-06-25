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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// opStructMsg builds a message with a structured body and an `op` metadata value
// driving the row_operation interpolation.
func opStructMsg(op string, body map[string]any) *service.Message {
	m := service.NewMessage(nil)
	m.SetStructured(body)
	m.MetaSetMut("op", op)
	return m
}

// keyRoundTrip pre-creates a table with a single typed identifier column `k`
// plus a string `val` column, then exercises insert -> delete -> upsert keyed on
// `k` and asserts the final state contains exactly the upserted row. A wrong key
// encoding (delete/upsert silently matching nothing) shows up as a surviving or
// duplicated row, i.e. count != 1.
func keyRoundTrip(t *testing.T, infra *testInfrastructure, ns, tbl string, keyType iceberg.Type, k1, k2 any) {
	t.Helper()
	ctx := context.Background()

	client := infra.NewCatalogClient(t, ns)
	_, err := client.CreateTable(ctx, tbl, iceberg.NewSchemaWithIdentifiers(
		1, []int{1},
		iceberg.NestedField{ID: 1, Name: "k", Type: keyType, Required: true},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.StringType{}, Required: false},
	))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"k"},
		}))

	// Seed two rows in their own snapshot.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"k": k1, "val": "a"}),
		opStructMsg("insert", map[string]any{"k": k2, "val": "b"}),
	})
	// Delete k2 and upsert k1 in a later snapshot — both must match the seeded
	// rows for the final state to be correct.
	produceMessages(t, ctx, router, service.MessageBatch{opStructMsg("delete", map[string]any{"k": k2})})
	produceMessages(t, ctx, router, service.MessageBatch{opStructMsg("upsert", map[string]any{"k": k1, "val": "a2"})})

	type valRow struct {
		Val string `json:"val"`
	}
	// DuckDB requires the equality-delete key column (k) to be in the projection
	// to apply the deletes, so select it even though we only assert on val.
	rows := querySQL[valRow](t, ctx, infra,
		fmt.Sprintf(`SELECT k, val FROM iceberg_cat."%s"."%s" ORDER BY val;`, ns, tbl))
	require.Lenf(t, rows, 1, "expected exactly one surviving row (delete+upsert both matched); got %d", len(rows))
	assert.Equal(t, "a2", rows[0].Val, "surviving row must be the upserted value")
}

func TestRowOperationKeyTypesIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)
	const ns = "row_op_keytypes"
	infra.CreateNamespace(t, ns)

	ts1 := time.UnixMilli(1700000000000).UTC()
	ts2 := time.UnixMilli(1700000111000).UTC()

	cases := []struct {
		name    string
		tbl     string
		keyType iceberg.Type
		k1, k2  any
	}{
		{"string", "k_string", iceberg.StringType{}, "alpha", "beta"},
		{"int64-big", "k_int64", iceberg.Int64Type{}, int64(9007199254740993), int64(9007199254740994)},
		{"uuid", "k_uuid", iceberg.UUIDType{}, "f47ac10b-58cc-4372-a567-0e02b2c3d479", "1b4e28ba-2fa1-11d2-883f-0016d3cca427"},
		{"decimal-float", "k_decimal_f", iceberg.DecimalTypeOf(12, 2), 123.45, 234.56},
		{"decimal-string", "k_decimal_s", iceberg.DecimalTypeOf(12, 2), "123.45", "234.56"},
		{"timestamp-time", "k_ts_time", iceberg.TimestampType{}, ts1, ts2},
		// Note: a bare numeric timestamp key is deliberately rejected at write
		// time (its unit is ambiguous and cannot be matched against the insert
		// path) — covered by the deleteKeyJSONValue unit test, not here.
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keyRoundTrip(t, infra, ns, tc.tbl, tc.keyType, tc.k1, tc.k2)
		})
	}
}

// TestRowOperationCompositeKeyIntegration verifies a multi-column identifier.
func TestRowOperationCompositeKeyIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)
	const ns, tbl = "row_op_composite", "ck"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	_, err := client.CreateTable(ctx, tbl, iceberg.NewSchemaWithIdentifiers(
		1, []int{1, 2},
		iceberg.NestedField{ID: 1, Name: "tenant", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 3, Name: "val", Type: iceberg.StringType{}, Required: false},
	))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"tenant", "id"},
		}))

	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"tenant": "t1", "id": "x", "val": "a"}),
		opStructMsg("insert", map[string]any{"tenant": "t1", "id": "y", "val": "b"}),
		opStructMsg("insert", map[string]any{"tenant": "t2", "id": "x", "val": "c"}),
	})
	// Delete only (t1,y); upsert (t2,x). (t1,x) is left untouched. The composite
	// key must distinguish (t1,x) from (t2,x).
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
}

// TestRowOperationPartitionedIntegration verifies upsert/delete on a partitioned
// table: the equality-delete records must carry the partition source column so
// deletes route to the correct partition and match the intended rows.
func TestRowOperationPartitionedIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)
	const ns, tbl = "row_op_partitioned", "p"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	// The partition source (region) must be part of the identifier so equality
	// deletes route to the correct partition.
	sc := iceberg.NewSchemaWithIdentifiers(1, []int{1, 2},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 3, Name: "val", Type: iceberg.StringType{}, Required: false},
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
			IdentifierFields: []string{"id", "region"},
		}))

	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "region": "us", "val": "a"}),
		opStructMsg("insert", map[string]any{"id": "2", "region": "eu", "val": "b"}),
	})
	// Delete id=2 (in the eu partition) and upsert id=1 (in the us partition).
	// The delete/upsert records must include region so they route correctly.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("delete", map[string]any{"id": "2", "region": "eu"}),
		opStructMsg("upsert", map[string]any{"id": "1", "region": "us", "val": "a2"}),
	})

	type row struct {
		ID  string `json:"id"`
		Val string `json:"val"`
	}
	// DuckDB needs all equality-delete key columns (id, region) in the projection
	// to apply the deletes.
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, region, val FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
	require.Len(t, rows, 1, "id=2 (eu) must be deleted and id=1 (us) must not be duplicated")
	assert.Equal(t, row{"1", "a2"}, rows[0])
}

// TestRowOperationBatchCollapseIntegration verifies the per-key collapse: two
// upserts of the same key in one batch must leave a single (latest) row.
func TestRowOperationBatchCollapseIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)
	const ns, tbl = "row_op_collapse", "c"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	_, err := client.CreateTable(ctx, tbl, iceberg.NewSchemaWithIdentifiers(
		1, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.StringType{}, Required: false},
	))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
		}))

	// Two upserts of "k" plus an upsert-then-delete of "g", all in one batch.
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
	require.Len(t, rows, 1, "k must appear once (no duplicate from two same-batch upserts); g must be deleted")
	assert.Equal(t, row{"k", "v2"}, rows[0], "the later upsert of k must win")
}
