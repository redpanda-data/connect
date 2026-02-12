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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
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
	infra := setupTestInfra(t, ctx)

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
		type tableNameResult struct {
			TableName string `json:"table_name"`
		}
		tables := querySQL[tableNameResult](t, ctx, infra,
			fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_catalog = 'iceberg_cat';`, namespaceName))
		var names []string
		for _, row := range tables {
			names = append(names, row.TableName)
		}
		assert.Contains(t, names, tableName)
	})

	t.Run("LoadTable", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		tableName := "test_load_table"
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "col1", Type: iceberg.Int64Type{}, Required: true},
		)

		_, err = client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)

		tbl, err := client.LoadTable(ctx, tableName)
		require.NoError(t, err)
		require.NotNil(t, tbl)

		loadedSchema := tbl.Schema()
		assert.Len(t, loadedSchema.Fields(), 1)
		assert.Equal(t, "col1", loadedSchema.Fields()[0].Name)

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

		tableName := "test_update_schema"
		initialSchema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "col1", Type: iceberg.Int32Type{}, Required: true},
		)

		tbl, err := client.CreateTable(ctx, tableName, initialSchema)
		require.NoError(t, err)

		_, err = client.UpdateSchema(ctx, tbl, func(us *table.UpdateSchema) {
			us.AddColumn([]string{"col2"}, iceberg.StringType{}, "", false, nil)
		})
		require.NoError(t, err)

		tbl, err = client.LoadTable(ctx, tableName)
		require.NoError(t, err)

		updatedSchema := tbl.Schema()
		assert.Len(t, updatedSchema.Fields(), 2)

		fieldNames := make([]string, len(updatedSchema.Fields()))
		for i, f := range updatedSchema.Fields() {
			fieldNames[i] = f.Name
		}
		assert.Contains(t, fieldNames, "col1")
		assert.Contains(t, fieldNames, "col2")
	})

	t.Run("AppendDataFiles", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(infra.CatalogConfig(), []string{namespaceName})
		require.NoError(t, err)
		defer client.Close()

		tableName := "test_append_data"
		schema := iceberg.NewSchema(
			0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.StringType{}, Required: false},
		)

		tbl, err := client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)

		parquetData := createTestParquet(t, []testRow{
			{ID: 1, Value: "one"},
			{ID: 2, Value: "two"},
			{ID: 3, Value: "three"},
		})

		fileKey := namespaceName + "/" + tableName + "/data/test-data.parquet"
		s3URI := uploadToMinIO(t, infra.MinioEndpoint, "warehouse", fileKey, parquetData)

		updatedTbl, err := client.AppendDataFiles(ctx, tbl, []string{s3URI})
		require.NoError(t, err)
		require.NotNil(t, updatedTbl)
		require.NotNil(t, updatedTbl.CurrentSnapshot())
	})

	t.Run("Close", func(t *testing.T) {
		client, err := catalogx.NewCatalogClient(catalogx.Config{
			URL:      infra.RestURL,
			AuthType: "none",
		}, []string{namespaceName})
		require.NoError(t, err)
		require.NoError(t, client.Close())
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		t.Run("ErrNoSuchTable", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			defer client.Close()

			_, err = client.LoadTable(ctx, "nonexistent_table_xyz")
			require.Error(t, err)
			assert.ErrorIs(t, err, catalog.ErrNoSuchTable)
		})

		t.Run("ErrNoSuchNamespace", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{"nonexistent_namespace_xyz"})
			require.NoError(t, err)
			defer client.Close()

			schema := iceberg.NewSchema(
				0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			)
			_, err = client.CreateTable(ctx, "test_table", schema)
			require.Error(t, err)
			assert.ErrorIs(t, err, catalog.ErrNoSuchNamespace)
		})
	})

	t.Run("NamespaceOperations", func(t *testing.T) {
		t.Run("CheckNamespaceExists", func(t *testing.T) {
			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{namespaceName})
			require.NoError(t, err)
			defer client.Close()

			exists, err := client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.True(t, exists)

			clientNonExistent, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{"nonexistent_namespace_check"})
			require.NoError(t, err)
			defer clientNonExistent.Close()

			exists, err = clientNonExistent.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.False(t, exists)
		})

		t.Run("CreateNamespace", func(t *testing.T) {
			newNamespace := "test_create_namespace"

			client, err := catalogx.NewCatalogClient(catalogx.Config{
				URL:      infra.RestURL,
				AuthType: "none",
			}, []string{newNamespace})
			require.NoError(t, err)
			defer client.Close()

			exists, err := client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.False(t, exists)

			err = client.CreateNamespace(ctx, nil)
			require.NoError(t, err)

			exists, err = client.CheckNamespaceExists(ctx)
			require.NoError(t, err)
			assert.True(t, exists)

			// Idempotent
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

			tableName := "test_check_exists"
			schema := iceberg.NewSchema(
				0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int32Type{}, Required: true},
			)
			_, err = client.CreateTable(ctx, tableName, schema)
			require.NoError(t, err)

			exists, err := client.CheckTableExists(ctx, tableName)
			require.NoError(t, err)
			assert.True(t, exists)

			exists, err = client.CheckTableExists(ctx, "nonexistent_table_check")
			require.NoError(t, err)
			assert.False(t, exists)
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
