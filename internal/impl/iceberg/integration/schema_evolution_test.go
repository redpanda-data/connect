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
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

func TestSchemaEvolutionIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()

	// Start test infrastructure
	infra := startTestInfrastructure(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, infra.Terminate(context.Background()))
	})

	// Create warehouse bucket
	infra.CreateBucket(t, "warehouse")

	t.Run("AutoCreateNamespaceAndTable", func(t *testing.T) {
		// Use a new namespace that doesn't exist yet
		namespaceName := "auto_create_ns"
		tableName := "auto_create_table"

		// Create router with schema evolution enabled
		namespaceStr, err := service.NewInterpolatedString(namespaceName)
		require.NoError(t, err)
		tableStr, err := service.NewInterpolatedString(tableName)
		require.NoError(t, err)

		logger := service.MockResources().Logger()

		schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
			Enabled:       true,
			PartitionSpec: nil, // No partitioning
		}

		catalogCfg := catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
			AdditionalProps: iceberg.Properties{
				io.S3AccessKeyID:            "admin",
				io.S3SecretAccessKey:        "password",
				io.S3EndpointURL:            infra.MinioEndpoint,
				io.S3ForceVirtualAddressing: "false",
				io.S3Region:                 "us-east-1",
			},
		}

		router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, logger)
		defer router.Close()

		// Create a batch with test messages
		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"id": 1, "name": "alice", "active": true}`)),
			service.NewMessage([]byte(`{"id": 2, "name": "bob", "active": false}`)),
		}

		// Route should succeed - creating namespace and table automatically
		err = router.Route(ctx, batch)
		require.NoError(t, err)

		// Verify namespace was created
		client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		exists, err := client.CheckNamespaceExists(ctx)
		require.NoError(t, err)
		assert.True(t, exists, "namespace should exist")

		// Verify table was created
		exists, err = client.CheckTableExists(ctx, tableName)
		require.NoError(t, err)
		assert.True(t, exists, "table should exist")

		// Verify table has correct schema
		tbl, err := client.LoadTable(ctx, tableName)
		require.NoError(t, err)

		schema := tbl.Schema()
		t.Logf("Created table schema: %v", schema)

		// Should have 3 fields: id, name, active
		assert.Len(t, schema.Fields(), 3)

		fieldNames := make(map[string]bool)
		for _, f := range schema.Fields() {
			fieldNames[f.Name] = true
		}
		assert.True(t, fieldNames["id"], "schema should have 'id' field")
		assert.True(t, fieldNames["name"], "schema should have 'name' field")
		assert.True(t, fieldNames["active"], "schema should have 'active' field")

		// Verify schema via DuckDB DESCRIBE
		columns, err := infra.DescribeIcebergTable(ctx, "rest", namespaceName, tableName)
		require.NoError(t, err)
		t.Logf("DuckDB DESCRIBE result: %v", columns)

		// Build map of column names to types
		columnTypes := make(map[string]string)
		for _, col := range columns {
			columnTypes[col.ColumnName] = col.ColumnType
		}

		assert.Len(t, columns, 3, "DuckDB should see 3 columns")
		assert.Contains(t, columnTypes, "id", "DuckDB should see 'id' column")
		assert.Contains(t, columnTypes, "name", "DuckDB should see 'name' column")
		assert.Contains(t, columnTypes, "active", "DuckDB should see 'active' column")
		assert.Equal(t, "DOUBLE", columnTypes["id"], "id should be DOUBLE")
		assert.Equal(t, "VARCHAR", columnTypes["name"], "name should be VARCHAR")
		assert.Equal(t, "BOOLEAN", columnTypes["active"], "active should be BOOLEAN")
	})

	t.Run("SchemaEvolution_AddNewColumn", func(t *testing.T) {
		// Create namespace first
		namespaceName := "schema_evo_ns"
		tableName := "schema_evo_table"
		infra.CreateNamespace(t, namespaceName)

		// Create router with schema evolution enabled
		namespaceStr, err := service.NewInterpolatedString(namespaceName)
		require.NoError(t, err)
		tableStr, err := service.NewInterpolatedString(tableName)
		require.NoError(t, err)

		logger := service.MockResources().Logger()

		schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
			Enabled:       true,
			PartitionSpec: nil,
		}

		catalogCfg := catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
			AdditionalProps: iceberg.Properties{
				io.S3AccessKeyID:            "admin",
				io.S3SecretAccessKey:        "password",
				io.S3EndpointURL:            infra.MinioEndpoint,
				io.S3ForceVirtualAddressing: "false",
				io.S3Region:                 "us-east-1",
			},
		}

		router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, logger)
		defer router.Close()

		// First batch - creates table with initial schema
		batch1 := service.MessageBatch{
			service.NewMessage([]byte(`{"id": 1, "name": "alice"}`)),
		}
		err = router.Route(ctx, batch1)
		require.NoError(t, err)

		// Verify initial schema
		client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		tbl, err := client.LoadTable(ctx, tableName)
		require.NoError(t, err)
		initialFieldCount := len(tbl.Schema().Fields())
		t.Logf("Initial schema has %d fields", initialFieldCount)

		// Verify initial schema via DuckDB
		initialColumns, err := infra.DescribeIcebergTable(ctx, "rest", namespaceName, tableName)
		require.NoError(t, err)
		t.Logf("DuckDB initial schema: %v", initialColumns)
		assert.Len(t, initialColumns, 2, "DuckDB should see 2 columns initially")

		initialColumnNames := make(map[string]bool)
		for _, col := range initialColumns {
			initialColumnNames[col.ColumnName] = true
		}
		assert.True(t, initialColumnNames["id"], "initial schema should have 'id'")
		assert.True(t, initialColumnNames["name"], "initial schema should have 'name'")
		assert.False(t, initialColumnNames["email"], "initial schema should NOT have 'email'")

		// Second batch - has a new column "email"
		batch2 := service.MessageBatch{
			service.NewMessage([]byte(`{"id": 2, "name": "bob", "email": "bob@example.com"}`)),
		}
		err = router.Route(ctx, batch2)
		require.NoError(t, err)

		// Verify schema was evolved
		tbl, err = client.LoadTable(ctx, tableName)
		require.NoError(t, err)
		evolvedFieldCount := len(tbl.Schema().Fields())
		t.Logf("Evolved schema has %d fields", evolvedFieldCount)

		assert.Greater(t, evolvedFieldCount, initialFieldCount, "schema should have more fields after evolution")

		// Check for email field
		hasEmail := false
		for _, f := range tbl.Schema().Fields() {
			if f.Name == "email" {
				hasEmail = true
				break
			}
		}
		assert.True(t, hasEmail, "schema should have 'email' field after evolution")

		// Verify evolved schema via DuckDB
		evolvedColumns, err := infra.DescribeIcebergTable(ctx, "rest", namespaceName, tableName)
		require.NoError(t, err)
		t.Logf("DuckDB evolved schema: %v", evolvedColumns)
		assert.Len(t, evolvedColumns, 3, "DuckDB should see 3 columns after evolution")

		evolvedColumnTypes := make(map[string]string)
		for _, col := range evolvedColumns {
			evolvedColumnTypes[col.ColumnName] = col.ColumnType
		}
		assert.Contains(t, evolvedColumnTypes, "id", "evolved schema should have 'id'")
		assert.Contains(t, evolvedColumnTypes, "name", "evolved schema should have 'name'")
		assert.Contains(t, evolvedColumnTypes, "email", "evolved schema should have 'email'")
		assert.Equal(t, "VARCHAR", evolvedColumnTypes["email"], "email should be VARCHAR")
	})

	t.Run("AutoCreateTable_WithPartitionSpec", func(t *testing.T) {
		// Create namespace first
		namespaceName := "partition_spec_ns"
		tableName := "partition_spec_table"
		infra.CreateNamespace(t, namespaceName)

		// Create router with schema evolution and partition spec
		namespaceStr, err := service.NewInterpolatedString(namespaceName)
		require.NoError(t, err)
		tableStr, err := service.NewInterpolatedString(tableName)
		require.NoError(t, err)

		// Note: bucket transform requires a non-double type, so we use the 'value' string field
		partitionSpecStr, err := service.NewInterpolatedString("(bucket(16, value))")
		require.NoError(t, err)

		logger := service.MockResources().Logger()

		schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
			Enabled:       true,
			PartitionSpec: partitionSpecStr,
		}

		catalogCfg := catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
			AdditionalProps: iceberg.Properties{
				io.S3AccessKeyID:            "admin",
				io.S3SecretAccessKey:        "password",
				io.S3EndpointURL:            infra.MinioEndpoint,
				io.S3ForceVirtualAddressing: "false",
				io.S3Region:                 "us-east-1",
			},
		}

		router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, logger)
		defer router.Close()

		// Create batch with test messages
		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"id": 1, "value": "test"}`)),
		}

		err = router.Route(ctx, batch)
		require.NoError(t, err)

		// Verify table was created with partition spec
		client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		tbl, err := client.LoadTable(ctx, tableName)
		require.NoError(t, err)

		spec := tbl.Spec()
		t.Logf("Partition spec: %v", spec)
		assert.False(t, spec.IsUnpartitioned(), "table should be partitioned")
		assert.Equal(t, 1, spec.NumFields(), "should have 1 partition field")

		// Verify schema via DuckDB
		columns, err := infra.DescribeIcebergTable(ctx, "rest", namespaceName, tableName)
		require.NoError(t, err)
		t.Logf("DuckDB DESCRIBE result: %v", columns)

		columnTypes := make(map[string]string)
		for _, col := range columns {
			columnTypes[col.ColumnName] = col.ColumnType
		}

		assert.Len(t, columns, 2, "DuckDB should see 2 columns")
		assert.Equal(t, "DOUBLE", columnTypes["id"], "id should be DOUBLE")
		assert.Equal(t, "VARCHAR", columnTypes["value"], "value should be VARCHAR")
	})

	t.Run("SchemaEvolutionDisabled_FailsOnMissingTable", func(t *testing.T) {
		// Use a namespace that doesn't exist
		namespaceName := "disabled_evo_ns"
		tableName := "disabled_evo_table"

		// Create router with schema evolution DISABLED
		namespaceStr, err := service.NewInterpolatedString(namespaceName)
		require.NoError(t, err)
		tableStr, err := service.NewInterpolatedString(tableName)
		require.NoError(t, err)

		logger := service.MockResources().Logger()

		schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
			Enabled: false, // Disabled
		}

		catalogCfg := catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}

		router := icebergimpl.NewRouter(catalogCfg, namespaceStr, tableStr, schemaEvoCfg, logger)
		defer router.Close()

		// Create batch
		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"id": 1}`)),
		}

		// Should fail because namespace/table don't exist and evolution is disabled
		err = router.Route(ctx, batch)
		require.Error(t, err)
		t.Logf("Expected error: %v", err)
	})
}
