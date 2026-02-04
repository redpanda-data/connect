// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

func TestCatalogxIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()

	// Start test infrastructure (MinIO + Iceberg REST catalog + DuckDB)
	infra := startTestInfrastructure(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, infra.Terminate(context.Background()))
	})

	// Create warehouse bucket in MinIO (must match CATALOG_WAREHOUSE in iceberg-rest-fixture)
	infra.CreateBucket(t, "warehouse")

	// Create namespace via REST API
	namespaceName := "catalogx_test"
	infra.CreateNamespace(t, namespaceName)

	t.Run("NewCatalogClient", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			require.NotNil(t, client)
			require.NoError(t, client.Close())
		})

		t.Run("WithWarehouse", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:       infra.RestURL,
				AuthType:  "none",
				Warehouse: "s3://warehouse/",
			}, []string{namespaceName})
			require.NoError(t, err)
			require.NotNil(t, client)
			require.NoError(t, client.Close())
		})

		t.Run("InvalidAuthType", func(t *testing.T) {
			_, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "invalid_auth_type",
			}, []string{namespaceName})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported auth type")
		})
	})

	t.Run("CreateTable", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		// Create table with simple schema
		tableName := "test_create_table"
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}, Required: false},
		)

		tbl, err := client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)
		require.NotNil(t, tbl)

		// Verify table exists via DuckDB
		tables, err := infra.ListIcebergTables(ctx, "demo", namespaceName)
		require.NoError(t, err)
		assert.Contains(t, tables, tableName)
	})

	t.Run("LoadTable", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		// Create a table first
		tableName := "test_load_table"
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "col1", Type: iceberg.Int64Type{}, Required: true},
		)

		_, err = client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)

		// Load the table
		tbl, err := client.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.NotNil(t, tbl)

		// Verify schema matches
		loadedSchema := tbl.Schema()
		assert.Len(t, loadedSchema.Fields(), 1)
		assert.Equal(t, "col1", loadedSchema.Fields()[0].Name)

		// Test loading non-existent table returns error
		_, err = client.LoadTable(ctx, "non_existent_table")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to load table")
	})

	t.Run("UpdateSchema", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		// Create table with single column
		tableName := "test_update_schema"
		initialSchema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "col1", Type: iceberg.Int32Type{}, Required: true},
		)

		tbl, err := client.CreateTable(ctx, tableName, initialSchema)
		require.NoError(t, err)

		// Update schema by adding a new column using the callback API
		_, err = client.UpdateSchema(ctx, tbl, func(us *table.UpdateSchema) {
			us.AddColumn([]string{"col2"}, iceberg.StringType{}, "", false, nil)
		})
		require.NoError(t, err)

		// Load table and verify schema has 2 fields
		tbl, err = client.LoadTable(ctx, tableName)
		require.NoError(t, err)

		updatedSchema := tbl.Schema()
		assert.Len(t, updatedSchema.Fields(), 2)

		// Verify field names
		fieldNames := make([]string, len(updatedSchema.Fields()))
		for i, f := range updatedSchema.Fields() {
			fieldNames[i] = f.Name
		}
		assert.Contains(t, fieldNames, "col1")
		assert.Contains(t, fieldNames, "col2")
	})

	t.Run("AppendDataFiles", func(t *testing.T) {
		// Configure S3 properties for MinIO access
		// The iceberg-go library needs these to read Parquet file stats
		s3Props := iceberg.Properties{
			io.S3AccessKeyID:            "admin",
			io.S3SecretAccessKey:        "password",
			io.S3EndpointURL:            infra.MinioEndpoint,
			io.S3ForceVirtualAddressing: "false", // Use path-style for MinIO
			io.S3Region:                 "us-east-1",
		}

		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:             infra.RestURL,
			AuthType:        "none",
			AdditionalProps: s3Props,
		}, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		// Create table with schema matching test data
		tableName := "test_append_data"
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.StringType{}, Required: false},
		)

		tbl, err := client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)

		// Create test Parquet data
		parquetData := createTestParquet(t, []testRow{
			{ID: 1, Value: "one"},
			{ID: 2, Value: "two"},
			{ID: 3, Value: "three"},
		})

		// Upload Parquet file to MinIO
		fileKey := namespaceName + "/" + tableName + "/data/test-data.parquet"
		s3URI := uploadToMinIO(t, infra.MinioEndpoint, "warehouse", fileKey, parquetData)
		t.Logf("Uploaded Parquet file: %s", s3URI)

		// Append data files to table
		updatedTbl, err := client.AppendDataFiles(ctx, tbl, []string{s3URI})
		require.NoError(t, err)
		require.NotNil(t, updatedTbl)

		// Verify table was updated by checking snapshots
		// AppendDataFiles creates a new snapshot with the data files
		require.NotNil(t, updatedTbl.CurrentSnapshot(), "Table should have a snapshot after appending data files")
		t.Logf("Successfully appended data files, snapshot: %d", updatedTbl.CurrentSnapshot().SnapshotID)
	})

	t.Run("Close", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)

		err = client.Close()
		require.NoError(t, err)
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		t.Run("ErrNoSuchTable", func(t *testing.T) {
			// Test that catalog.ErrNoSuchTable is properly propagated through catalogx wrapper
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			defer client.Close()

			// Try to load a non-existent table
			_, err = client.LoadTable(ctx, "nonexistent_table_xyz")
			require.Error(t, err)

			// Verify the error chain preserves catalog.ErrNoSuchTable
			assert.True(t, errors.Is(err, catalog.ErrNoSuchTable),
				"expected error to wrap catalog.ErrNoSuchTable, got: %v", err)
		})

		t.Run("ErrNoSuchNamespace", func(t *testing.T) {
			// Test that catalog.ErrNoSuchNamespace is properly propagated through catalogx wrapper
			// Use a namespace that doesn't exist
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{"nonexistent_namespace_xyz"})
			require.NoError(t, err)
			defer client.Close()

			// Try to create a table in a non-existent namespace
			schema := iceberg.NewSchema(
				0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			)
			_, err = client.CreateTable(ctx, "test_table", schema)
			require.Error(t, err)

			// Verify the error chain preserves catalog.ErrNoSuchNamespace
			assert.True(t, errors.Is(err, catalog.ErrNoSuchNamespace),
				"expected error to wrap catalog.ErrNoSuchNamespace, got: %v", err)
		})
	})

	t.Run("NamespaceOperations", func(t *testing.T) {
		t.Run("CheckNamespaceExists", func(t *testing.T) {
			// Test existing namespace
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			defer client.Close()

			exists, err := client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.True(t, exists, "namespace should exist")

			// Test non-existing namespace
			clientNonExistent, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{"nonexistent_namespace_check"})
			require.NoError(t, err)
			defer clientNonExistent.Close()

			exists, err = clientNonExistent.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.False(t, exists, "namespace should not exist")
		})

		t.Run("CreateNamespace", func(t *testing.T) {
			newNamespace := "test_create_namespace"

			// Create client for new namespace
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{newNamespace})
			require.NoError(t, err)
			defer client.Close()

			// Namespace should not exist initially
			exists, err := client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.False(t, exists)

			// Create the namespace
			err = client.CreateNamespace(ctx, nil)
			require.NoError(t, err)

			// Namespace should now exist
			exists, err = client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.True(t, exists)

			// Creating again should be idempotent (no error)
			err = client.CreateNamespace(ctx, nil)
			require.NoError(t, err)
		})

		t.Run("CheckTableExists", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			defer client.Close()

			// First, create a table
			tableName := "test_check_exists"
			schema := iceberg.NewSchema(
				0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			)
			_, err = client.CreateTable(ctx, tableName, schema)
			require.NoError(t, err)

			// Table should exist
			exists, err := client.CheckTableExists(ctx, tableName)
			require.NoError(t, err)
			assert.True(t, exists, "table should exist")

			// Non-existent table should return false
			exists, err = client.CheckTableExists(ctx, "nonexistent_table_check")
			require.NoError(t, err)
			assert.False(t, exists, "table should not exist")
		})
	})
}

// testRow is a test data structure for Parquet generation.
type testRow struct {
	ID    int32  `parquet:"id"`
	Value string `parquet:"value"`
}

// createTestParquet creates a Parquet file from test rows.
func createTestParquet(t *testing.T, rows []testRow) []byte {
	t.Helper()

	buf := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[testRow](buf)

	_, err := writer.Write(rows)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// uploadToMinIO uploads data to MinIO and returns the S3 URI.
func uploadToMinIO(t *testing.T, endpoint, bucket, key string, data []byte) string {
	t.Helper()

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("admin", "password", ""),
		),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	})
	require.NoError(t, err)

	return "s3://" + bucket + "/" + key
}
