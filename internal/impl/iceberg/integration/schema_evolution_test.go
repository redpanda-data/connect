// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

func TestSchemaEvolutionIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	t.Run("AutoCreateNamespaceAndTable", func(t *testing.T) {
		router := infra.NewRouter(t, "auto_create_ns", "auto_create_table",
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		produce(t, ctx, router,
			`{"id": 1, "name": "alice", "active": true}`,
			`{"id": 2, "name": "bob", "active": false}`,
		)

		// Verify namespace and table were auto-created
		client := infra.NewCatalogClient(t, "auto_create_ns")
		exists, err := client.CheckNamespaceExists(ctx)
		require.NoError(t, err)
		assert.True(t, exists, "namespace should exist")

		tbl, err := client.LoadTable(ctx, "auto_create_table")
		require.NoError(t, err)
		assert.Len(t, tbl.Schema().Fields(), 3)

		// Verify schema via DuckDB
		cols := querySQL[ColumnInfo](t, ctx, infra,
			`DESCRIBE iceberg_cat."auto_create_ns"."auto_create_table";`)
		require.Len(t, cols, 3)

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}
		assert.Equal(t, "DOUBLE", colTypes["id"])
		assert.Equal(t, "VARCHAR", colTypes["name"])
		assert.Equal(t, "BOOLEAN", colTypes["active"])
	})

	t.Run("SchemaEvolution_AddNewColumn", func(t *testing.T) {
		infra.CreateNamespace(t, "schema_evo_ns")
		router := infra.NewRouter(t, "schema_evo_ns", "schema_evo_table",
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		// First batch creates the table with {id, name}
		produce(t, ctx, router, `{"id": 1, "name": "alice"}`)

		cols := querySQL[ColumnInfo](t, ctx, infra,
			`DESCRIBE iceberg_cat."schema_evo_ns"."schema_evo_table";`)
		require.Len(t, cols, 2)

		// Second batch adds "email" column
		produce(t, ctx, router, `{"id": 2, "name": "bob", "email": "bob@example.com"}`)

		cols = querySQL[ColumnInfo](t, ctx, infra,
			`DESCRIBE iceberg_cat."schema_evo_ns"."schema_evo_table";`)
		require.Len(t, cols, 3)

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}
		assert.Equal(t, "VARCHAR", colTypes["email"])
	})

	t.Run("AutoCreateTable_WithPartitionSpec", func(t *testing.T) {
		infra.CreateNamespace(t, "partition_spec_ns")

		partitionSpecStr, err := service.NewInterpolatedString("(bucket(16, value))")
		require.NoError(t, err)

		router := infra.NewRouter(t, "partition_spec_ns", "partition_spec_table",
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{
				Enabled:       true,
				PartitionSpec: partitionSpecStr,
			}))

		produce(t, ctx, router, `{"id": 1, "value": "test"}`)

		client := infra.NewCatalogClient(t, "partition_spec_ns")
		tbl, err := client.LoadTable(ctx, "partition_spec_table")
		require.NoError(t, err)
		assert.False(t, tbl.Spec().IsUnpartitioned())
		spec := tbl.Spec()
		assert.Equal(t, 1, spec.NumFields())

		cols := querySQL[ColumnInfo](t, ctx, infra,
			`DESCRIBE iceberg_cat."partition_spec_ns"."partition_spec_table";`)
		require.Len(t, cols, 2)

		colTypes := make(map[string]string)
		for _, col := range cols {
			colTypes[col.ColumnName] = col.ColumnType
		}
		assert.Equal(t, "DOUBLE", colTypes["id"])
		assert.Equal(t, "VARCHAR", colTypes["value"])
	})

	t.Run("SchemaEvolutionDisabled_FailsOnMissingTable", func(t *testing.T) {
		router := infra.NewRouter(t, "disabled_evo_ns", "disabled_evo_table")

		batch := service.MessageBatch{service.NewMessage([]byte(`{"id": 1}`))}
		err := router.Route(ctx, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "disabled_evo_ns.disabled_evo_table")
	})

	t.Run("SchemaEvolution_NullInRequiredColumn", func(t *testing.T) {
		const ns = "null_req_ns"
		const tblName = "null_req_table"
		infra.CreateNamespace(t, ns)

		// Create table with a required column via catalog
		client := infra.NewCatalogClient(t, ns)
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Float64Type{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: false},
		)
		_, err := client.CreateTable(ctx, tblName, schema)
		require.NoError(t, err)

		// Verify "id" starts as required via iceberg catalog
		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		idField, ok := tbl.Schema().FindFieldByName("id")
		require.True(t, ok)
		assert.True(t, idField.Required, "id should start as required")

		// Write a record with null for the required "id" column.
		// The router should catch RequiredFieldNullError, make "id" optional, and retry.
		router := infra.NewRouter(t, ns, tblName,
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		produce(t, ctx, router, `{"id": null, "name": "alice"}`)

		// Verify "id" is now optional via iceberg catalog
		tbl, err = client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		idField, ok = tbl.Schema().FindFieldByName("id")
		require.True(t, ok)
		assert.False(t, idField.Required, "id should now be optional after schema evolution")

		// Verify the data was written
		rows := querySQL[countResult](t, ctx, infra,
			`SELECT COUNT(*) as count FROM iceberg_cat."`+ns+`"."`+tblName+`";`)
		assert.Equal(t, 1, rows[0].Count)
	})

	t.Run("RowCount", func(t *testing.T) {
		router := infra.NewRouter(t, "auto_create_ns", "auto_create_table",
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		// Write to the same table created in AutoCreateNamespaceAndTable
		produce(t, ctx, router,
			`{"id": 3, "name": "charlie", "active": true}`,
		)

		rows := querySQL[countResult](t, ctx, infra,
			`SELECT COUNT(*) as count FROM iceberg_cat."auto_create_ns"."auto_create_table";`)
		require.GreaterOrEqual(t, rows[0].Count, 3)
	})
}
