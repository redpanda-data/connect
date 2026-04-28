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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestNumericPrecisionIntegration validates that typed Go values (as produced by
// Avro deserialization) land with the correct Iceberg/Parquet column types and
// that data round-trips without precision loss. This exercises the fix for the
// customer escalation where financial fields defaulted to DOUBLE.
func TestNumericPrecisionIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	t.Run("TypedIntegersMapToCorrectColumnTypes", func(t *testing.T) {
		const ns = "numeric_types_ns"
		const tbl = "numeric_types_table"

		router := infra.NewRouter(t, ns, tbl,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{
			"amount_cents": int64(1234567890),
			"count":        int32(42),
			"label":        "payment",
			"rate":         float64(0.035),
			"enabled":      true,
		})

		produceMessages(t, ctx, router, service.MessageBatch{msg})

		cols := querySQL[ColumnInfo](t, ctx, infra,
			fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}

		assert.Equal(t, "BIGINT", colTypes["amount_cents"], "int64 should map to BIGINT (Iceberg long)")
		assert.Equal(t, "INTEGER", colTypes["count"], "int32 should map to INTEGER (Iceberg int)")
		assert.Equal(t, "VARCHAR", colTypes["label"], "string should map to VARCHAR")
		assert.Equal(t, "DOUBLE", colTypes["rate"], "float64 should map to DOUBLE")
		assert.Equal(t, "BOOLEAN", colTypes["enabled"], "bool should map to BOOLEAN")
	})

	t.Run("Int64ValuesPreservePrecision", func(t *testing.T) {
		const ns = "precision_ns"
		const tbl = "precision_table"

		router := infra.NewRouter(t, ns, tbl,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		// Use values that would lose precision if stored as float64.
		// float64 has a 53-bit mantissa (~15.9 decimal digits).
		// These 18-digit values are exact as int64 but not as float64.
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{
			"id":           int64(1),
			"large_amount": int64(999999999999999999), // 18 digits
			"negative":     int64(-123456789012345678),
		})

		produceMessages(t, ctx, router, service.MessageBatch{msg})

		type row struct {
			ID          int64 `json:"id"`
			LargeAmount int64 `json:"large_amount"`
			Negative    int64 `json:"negative"`
		}

		rows := querySQL[row](t, ctx, infra,
			fmt.Sprintf(`SELECT id, large_amount, negative FROM iceberg_cat."%s"."%s";`, ns, tbl))
		require.Len(t, rows, 1)
		assert.Equal(t, int64(999999999999999999), rows[0].LargeAmount,
			"large int64 should round-trip without precision loss")
		assert.Equal(t, int64(-123456789012345678), rows[0].Negative,
			"negative int64 should round-trip without precision loss")
	})

	t.Run("NestedStructs", func(t *testing.T) {
		const ns = "nested_ns"
		const tbl = "nested_table"

		router := infra.NewRouter(t, ns, tbl,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{
			"transfer_id": "TXN-001",
			"source": map[string]any{
				"account_id":  "ACC-123",
				"bank_code":   "SWIFT-XYZ",
				"holder_name": "Alice",
			},
			"destination": map[string]any{
				"account_id":  "ACC-456",
				"bank_code":   "SWIFT-ABC",
				"holder_name": "Bob",
			},
			"amount_cents": int64(5000000),
			"currency":     "USD",
		})

		produceMessages(t, ctx, router, service.MessageBatch{msg})

		// Verify column types
		cols := querySQL[ColumnInfo](t, ctx, infra,
			fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}

		assert.Equal(t, "VARCHAR", colTypes["transfer_id"])
		assert.Equal(t, "BIGINT", colTypes["amount_cents"], "int64 amount_cents should be BIGINT")
		assert.Equal(t, "VARCHAR", colTypes["currency"])
		assert.Contains(t, colTypes["source"], "STRUCT", "nested record should be STRUCT")
		assert.Contains(t, colTypes["destination"], "STRUCT", "nested record should be STRUCT")

		// Verify data round-trips through nested structs
		type result struct {
			TransferID  string `json:"transfer_id"`
			AmountCents int64  `json:"amount_cents"`
			Currency    string `json:"currency"`
		}

		rows := querySQL[result](t, ctx, infra,
			fmt.Sprintf(`SELECT transfer_id, amount_cents, currency FROM iceberg_cat."%s"."%s";`, ns, tbl))
		require.Len(t, rows, 1)
		assert.Equal(t, "TXN-001", rows[0].TransferID)
		assert.Equal(t, int64(5000000), rows[0].AmountCents)
		assert.Equal(t, "USD", rows[0].Currency)
	})

	t.Run("NestedStructsWithNullableFields", func(t *testing.T) {
		// Auto-create table, then write messages where some nested fields are absent.
		const ns = "nullable_nested_ns"
		const tbl = "nullable_nested_table"

		router := infra.NewRouter(t, ns, tbl,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		// First message: fee present, reference present
		msg1 := service.NewMessage(nil)
		msg1.SetStructuredMut(map[string]any{
			"transfer_id":  "TXN-001",
			"amount_cents": int64(5000000),
			"fee": map[string]any{
				"amount_cents": int64(250),
				"type":         "wire",
			},
			"reference": "INV-2026-001",
		})

		// Second message: fee absent, reference absent
		msg2 := service.NewMessage(nil)
		msg2.SetStructuredMut(map[string]any{
			"transfer_id":  "TXN-002",
			"amount_cents": int64(3000000),
		})

		produceMessages(t, ctx, router, service.MessageBatch{msg1, msg2})

		// Verify row count
		rows := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."%s";`, ns, tbl))
		require.Len(t, rows, 1)
		assert.Equal(t, 2, rows[0].Count)

		// Verify column types
		cols := querySQL[ColumnInfo](t, ctx, infra,
			fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}

		assert.Equal(t, "BIGINT", colTypes["amount_cents"])
		assert.Equal(t, "VARCHAR", colTypes["reference"])
		assert.Contains(t, colTypes["fee"], "STRUCT", "fee should be STRUCT")
	})

	t.Run("SchemaEvolution_NewIntegerColumn", func(t *testing.T) {
		const ns = "evo_int_ns"
		const tbl = "evo_int_table"

		router := infra.NewRouter(t, ns, tbl,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		// First batch: creates table with {id, name}
		msg1 := service.NewMessage(nil)
		msg1.SetStructuredMut(map[string]any{
			"id":   int64(1),
			"name": "alice",
		})
		produceMessages(t, ctx, router, service.MessageBatch{msg1})

		// Second batch: adds amount_cents column (int64) via schema evolution
		msg2 := service.NewMessage(nil)
		msg2.SetStructuredMut(map[string]any{
			"id":           int64(2),
			"name":         "bob",
			"amount_cents": int64(9999999),
		})
		produceMessages(t, ctx, router, service.MessageBatch{msg2})

		cols := querySQL[ColumnInfo](t, ctx, infra,
			fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}

		assert.Equal(t, "BIGINT", colTypes["id"], "int64 id should be BIGINT")
		assert.Equal(t, "BIGINT", colTypes["amount_cents"],
			"schema-evolved int64 column should be BIGINT, not DOUBLE")

		// Verify data
		type result struct {
			ID          int64  `json:"id"`
			Name        string `json:"name"`
			AmountCents *int64 `json:"amount_cents"`
		}
		dataRows := querySQL[result](t, ctx, infra,
			fmt.Sprintf(`SELECT id, name, amount_cents FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
		require.Len(t, dataRows, 2)

		// First row: amount_cents is null (column didn't exist yet)
		assert.Equal(t, int64(1), dataRows[0].ID)
		assert.Nil(t, dataRows[0].AmountCents)

		// Second row: amount_cents is present
		assert.Equal(t, int64(2), dataRows[1].ID)
		require.NotNil(t, dataRows[1].AmountCents)
		assert.Equal(t, int64(9999999), *dataRows[1].AmountCents)
	})
}
