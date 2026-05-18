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

	// CaseInsensitiveColumnMatching covers a customer scenario where the iceberg
	// table schema uses lowercase column names (the iceberg convention) but the
	// inbound messages have uppercase keys. The router must route the upper-case
	// values into the existing lowercase columns rather than treating them as
	// new columns and triggering schema evolution — adding a case-only duplicate
	// would either corrupt the schema or be rejected by case-insensitive
	// catalogs.
	t.Run("CaseInsensitiveColumnMatching", func(t *testing.T) {
		const ns = "case_match_ns"
		const tblName = "case_match_table"
		infra.CreateNamespace(t, ns)

		// Pre-create the table with lowercase column names.
		client := infra.NewCatalogClient(t, ns)
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: false},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: false},
		)
		_, err := client.CreateTable(ctx, tblName, schema)
		require.NoError(t, err)

		// Send messages with upper-case keys (the customer's payload shape).
		// case_sensitive_columns=false matches iceberg's recommended convention.
		router := infra.NewRouter(t, ns, tblName,
			WithCaseSensitive(false),
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))
		produce(t, ctx, router,
			`{"ID": 1, "NAME": "alice"}`,
			`{"ID": 2, "NAME": "bob"}`,
		)

		// Schema must not have evolved: still two columns, both lowercase.
		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		fields := tbl.Schema().Fields()
		require.Len(t, fields, 2, "schema must not have evolved for case-only mismatches")
		assert.Equal(t, "id", fields[0].Name)
		assert.Equal(t, "name", fields[1].Name)

		// Data must land in the lowercase columns.
		cols := querySQL[ColumnInfo](t, ctx, infra,
			`DESCRIBE iceberg_cat."`+ns+`"."`+tblName+`";`)
		require.Len(t, cols, 2)

		rows := querySQL[countResult](t, ctx, infra,
			`SELECT COUNT(*) as count FROM iceberg_cat."`+ns+`"."`+tblName+`" WHERE name IN ('alice', 'bob');`)
		assert.Equal(t, 2, rows[0].Count)
	})

	// CaseInsensitiveCreateTimeDuplicate verifies the create-time guard: when
	// case_sensitive_columns=false and the first message contains two keys
	// differing only in case, table creation is rejected before we hand the
	// schema to iceberg-go.
	t.Run("CaseInsensitiveCreateTimeDuplicate", func(t *testing.T) {
		const ns = "case_dup_create_ns"
		const tblName = "case_dup_create_table"
		infra.CreateNamespace(t, ns)

		router := infra.NewRouter(t, ns, tblName,
			WithCaseSensitive(false),
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		batch := service.MessageBatch{service.NewMessage([]byte(`{"id": 1, "ID": 2}`))}
		err := router.Route(ctx, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ambiguous columns")

		// Table must not have been created.
		client := infra.NewCatalogClient(t, ns)
		exists, err := client.CheckTableExists(ctx, tblName)
		require.NoError(t, err)
		assert.False(t, exists, "table should not exist after rejected create")
	})

	// CaseInsensitiveNewFieldDedupAcrossBatch covers the sink-level dedup
	// fix end-to-end: when two messages in a single batch each introduce a
	// new field that differs from the other only in case, schema evolution
	// must add the column exactly once. Without dedup folding, the second
	// add would race the first and be rejected by case-insensitive
	// UpdateSchema, breaking the entire batch.
	t.Run("CaseInsensitiveNewFieldDedupAcrossBatch", func(t *testing.T) {
		const ns = "case_dedup_ns"
		const tblName = "case_dedup_table"
		infra.CreateNamespace(t, ns)

		client := infra.NewCatalogClient(t, ns)
		base := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: false},
		)
		_, err := client.CreateTable(ctx, tblName, base)
		require.NoError(t, err)

		router := infra.NewRouter(t, ns, tblName,
			WithCaseSensitive(false),
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}))

		produce(t, ctx, router,
			`{"id": 1, "EMAIL": "a@x.z"}`,
			`{"id": 2, "email": "b@x.z"}`,
		)

		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		// Expect base id + exactly one email column added; whichever case
		// arrived first wins, but there must not be both.
		assert.Len(t, tbl.Schema().Fields(), 2)

		rows := querySQL[countResult](t, ctx, infra,
			`SELECT COUNT(*) as count FROM iceberg_cat."`+ns+`"."`+tblName+`";`)
		assert.Equal(t, 2, rows[0].Count)
	})

	// CaseInsensitivePartitionSpecCreate verifies the partition-spec parser
	// resolves column references case-insensitively when the flag is off.
	// The first message creates the table with lowercase columns, while the
	// configured partition spec references the column in uppercase — the
	// parse must succeed against the schema produced from the message.
	t.Run("CaseInsensitivePartitionSpecCreate", func(t *testing.T) {
		const ns = "case_partition_ns"
		const tblName = "case_partition_table"
		infra.CreateNamespace(t, ns)

		// Partition spec references VALUE in upper case; the table schema
		// will be inferred from the message whose key is lowercase value.
		partitionSpecStr, err := service.NewInterpolatedString("(bucket(16, VALUE))")
		require.NoError(t, err)

		router := infra.NewRouter(t, ns, tblName,
			WithCaseSensitive(false),
			WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{
				Enabled:       true,
				PartitionSpec: partitionSpecStr,
			}))

		produce(t, ctx, router, `{"id": 1, "value": "test"}`)

		client := infra.NewCatalogClient(t, ns)
		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		spec := tbl.Spec()
		assert.False(t, spec.IsUnpartitioned())
		assert.Equal(t, 1, spec.NumFields())
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
