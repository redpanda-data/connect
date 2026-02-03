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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	iberg "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

func TestConnectorIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	infra := startTestInfrastructure(t, ctx)
	t.Cleanup(func() { require.NoError(t, infra.Terminate(context.Background())) })

	// Create bucket and namespace for all connector tests
	infra.CreateBucket(t, "warehouse")
	const namespace = "connector_test"
	infra.CreateNamespace(t, namespace)

	t.Run("Writer", func(t *testing.T) {
		testWriterIntegration(t, ctx, infra, namespace)
	})

	t.Run("Router", func(t *testing.T) {
		testRouterIntegration(t, ctx, infra, namespace)
	})

	t.Run("RouterMultipleTables", func(t *testing.T) {
		testRouterMultipleTablesIntegration(t, ctx, infra, namespace)
	})

	t.Run("ListValues", func(t *testing.T) {
		testListValuesIntegration(t, ctx, infra, namespace)
	})

	t.Run("NestedStruct", func(t *testing.T) {
		testNestedStructIntegration(t, ctx, infra, namespace)
	})
}

// testWriterIntegration tests the writer component directly.
func testWriterIntegration(t *testing.T, ctx context.Context, infra *testInfrastructure, namespace string) {
	t.Helper()

	// Create a test table
	tableName := "writer_test"
	schema := iceberg.NewSchemaWithIdentifiers(
		1, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	// Create catalog client
	catalogCfg := catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			"s3.access-key-id":     "admin",
			"s3.secret-access-key": "password",
			"s3.endpoint":          infra.MinioEndpoint,
			"s3.path-style-access": "true",
			"s3.region":            "us-east-1",
		},
	}

	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Create the table
	_, err = client.CreateTable(ctx, tableName, schema)
	require.NoError(t, err)
	t.Logf("Created table: %s.%s", namespace, tableName)

	// Load the table twice - writer and committer need separate references
	writerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	committerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	// Create committer and writer with separate table references
	logger := service.MockResources().Logger()
	comm, err := iberg.NewCommitter(committerTbl, logger)
	require.NoError(t, err)

	writer := iberg.NewWriter(writerTbl, comm, logger)
	defer writer.Close()

	// Create test messages
	batch := createTestBatch(t, []map[string]any{
		{"id": int64(1), "name": "Alice", "value": 1.5},
		{"id": int64(2), "name": "Bob", "value": 2.5},
		{"id": int64(3), "name": "Charlie", "value": 3.5},
	})

	// Write the batch
	err = writer.Write(ctx, batch)
	require.NoError(t, err)

	// Wait for commit to complete
	time.Sleep(500 * time.Millisecond)

	// Verify data was written
	count, err := infra.CountIcebergRows(ctx, "rest", namespace, tableName)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "expected 3 rows")

	t.Logf("Writer test passed: wrote %d rows to %s.%s", count, namespace, tableName)
}

// testRouterIntegration tests the router with a static table name.
func testRouterIntegration(t *testing.T, ctx context.Context, infra *testInfrastructure, namespace string) {
	t.Helper()

	// Create a test table
	tableName := "router_test"
	schema := iceberg.NewSchemaWithIdentifiers(
		1, nil,
		iceberg.NestedField{ID: 1, Name: "event_type", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Create catalog client
	catalogCfg := catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			"s3.access-key-id":     "admin",
			"s3.secret-access-key": "password",
			"s3.endpoint":          infra.MinioEndpoint,
			"s3.path-style-access": "true",
			"s3.region":            "us-east-1",
		},
	}

	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Create the table
	_, err = client.CreateTable(ctx, tableName, schema)
	require.NoError(t, err)
	t.Logf("Created table: %s.%s", namespace, tableName)

	// Create interpolated strings (static values for this test)
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	tableStr, err := service.NewInterpolatedString(tableName)
	require.NoError(t, err)

	// Create router
	logger := service.MockResources().Logger()
	router := iberg.NewRouter(catalogCfg, namespaceStr, tableStr, logger)
	defer router.Close()

	// Create test messages
	batch := createTestBatch(t, []map[string]any{
		{"event_type": "click", "payload": "button_1"},
		{"event_type": "view", "payload": "page_home"},
		{"event_type": "click", "payload": "button_2"},
	})

	// Route the batch
	err = router.Route(ctx, batch)
	require.NoError(t, err)

	// Wait for commit to complete
	time.Sleep(500 * time.Millisecond)

	// Verify data was written
	count, err := infra.CountIcebergRows(ctx, "rest", namespace, tableName)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "expected 3 rows")

	t.Logf("Router test passed: routed %d messages to %s.%s", count, namespace, tableName)
}

// testRouterMultipleTablesIntegration tests the router with dynamic table names.
func testRouterMultipleTablesIntegration(t *testing.T, ctx context.Context, infra *testInfrastructure, namespace string) {
	t.Helper()

	// Create two test tables with the same schema
	schema := iceberg.NewSchemaWithIdentifiers(
		1, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	catalogCfg := catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			"s3.access-key-id":     "admin",
			"s3.secret-access-key": "password",
			"s3.endpoint":          infra.MinioEndpoint,
			"s3.path-style-access": "true",
			"s3.region":            "us-east-1",
		},
	}

	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Create tables for different types
	tables := []string{"events_clicks", "events_views"}
	for _, tableName := range tables {
		_, err = client.CreateTable(ctx, tableName, schema)
		require.NoError(t, err)
		t.Logf("Created table: %s.%s", namespace, tableName)
	}

	// Create interpolated strings with dynamic table name based on metadata
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	// Table name will be interpolated from message metadata: "events_${!meta("event_type")}"
	tableStr, err := service.NewInterpolatedString(`events_${!meta("event_type")}`)
	require.NoError(t, err)

	// Create router
	logger := service.MockResources().Logger()
	router := iberg.NewRouter(catalogCfg, namespaceStr, tableStr, logger)
	defer router.Close()

	// Create test messages with metadata indicating which table they should go to
	batch := service.MessageBatch{
		createMessageWithMeta(t, map[string]any{"id": int64(1), "data": "click_1"}, "event_type", "clicks"),
		createMessageWithMeta(t, map[string]any{"id": int64(2), "data": "view_1"}, "event_type", "views"),
		createMessageWithMeta(t, map[string]any{"id": int64(3), "data": "click_2"}, "event_type", "clicks"),
		createMessageWithMeta(t, map[string]any{"id": int64(4), "data": "view_2"}, "event_type", "views"),
		createMessageWithMeta(t, map[string]any{"id": int64(5), "data": "click_3"}, "event_type", "clicks"),
	}

	// Route the batch (should split into two tables)
	err = router.Route(ctx, batch)
	require.NoError(t, err)

	// Wait for commits to complete
	time.Sleep(500 * time.Millisecond)

	// Verify data was written to correct tables
	clickCount, err := infra.CountIcebergRows(ctx, "rest", namespace, "events_clicks")
	require.NoError(t, err)
	assert.Equal(t, 3, clickCount, "expected 3 rows in events_clicks")

	viewCount, err := infra.CountIcebergRows(ctx, "rest", namespace, "events_views")
	require.NoError(t, err)
	assert.Equal(t, 2, viewCount, "expected 2 rows in events_views")

	t.Logf("Router multiple tables test passed: %d clicks, %d views", clickCount, viewCount)
}

// testListValuesIntegration tests writing records with list fields.
func testListValuesIntegration(t *testing.T, ctx context.Context, infra *testInfrastructure, namespace string) {
	t.Helper()

	tableName := "list_test"
	schema := iceberg.NewSchemaWithIdentifiers(
		1, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID:   2,
			Name: "tags",
			Type: &iceberg.ListType{
				ElementID:       3,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			},
			Required: false,
		},
		iceberg.NestedField{
			ID:   4,
			Name: "scores",
			Type: &iceberg.ListType{
				ElementID:       5,
				Element:         iceberg.PrimitiveTypes.Int64,
				ElementRequired: false,
			},
			Required: false,
		},
	)

	catalogCfg := catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			"s3.access-key-id":     "admin",
			"s3.secret-access-key": "password",
			"s3.endpoint":          infra.MinioEndpoint,
			"s3.path-style-access": "true",
			"s3.region":            "us-east-1",
		},
	}

	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	_, err = client.CreateTable(ctx, tableName, schema)
	require.NoError(t, err)
	t.Logf("Created table: %s.%s", namespace, tableName)

	writerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	committerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	logger := service.MockResources().Logger()
	comm, err := iberg.NewCommitter(committerTbl, logger)
	require.NoError(t, err)

	writer := iberg.NewWriter(writerTbl, comm, logger)
	defer writer.Close()

	// Create test messages with list values
	batch := createTestBatch(t, []map[string]any{
		{"id": int64(1), "tags": []any{"red", "blue", "green"}, "scores": []any{int64(100), int64(200)}},
		{"id": int64(2), "tags": []any{"yellow"}, "scores": []any{int64(50), int64(75), int64(100)}},
		{"id": int64(3), "tags": []any{}, "scores": nil}, // empty list and null list
	})

	err = writer.Write(ctx, batch)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	count, err := infra.CountIcebergRows(ctx, "rest", namespace, tableName)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "expected 3 rows")

	t.Logf("List values test passed: wrote %d rows with list fields to %s.%s", count, namespace, tableName)
}

// testNestedStructIntegration tests writing records with nested struct fields.
func testNestedStructIntegration(t *testing.T, ctx context.Context, infra *testInfrastructure, namespace string) {
	t.Helper()

	tableName := "nested_test"
	schema := iceberg.NewSchemaWithIdentifiers(
		1, nil,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{
			ID:   2,
			Name: "user",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
					{ID: 4, Name: "email", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 5, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
				},
			},
			Required: false,
		},
		iceberg.NestedField{
			ID:   6,
			Name: "address",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 7, Name: "street", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 8, Name: "city", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 9, Name: "location", Type: &iceberg.StructType{
						FieldList: []iceberg.NestedField{
							{ID: 10, Name: "lat", Type: iceberg.PrimitiveTypes.Float64, Required: false},
							{ID: 11, Name: "lng", Type: iceberg.PrimitiveTypes.Float64, Required: false},
						},
					}, Required: false},
				},
			},
			Required: false,
		},
	)

	catalogCfg := catalogx.Config{
		URL:      infra.RestURL,
		AuthType: "none",
		AdditionalProps: iceberg.Properties{
			"s3.access-key-id":     "admin",
			"s3.secret-access-key": "password",
			"s3.endpoint":          infra.MinioEndpoint,
			"s3.path-style-access": "true",
			"s3.region":            "us-east-1",
		},
	}

	client, err := catalogx.NewCatalogClient(catalogCfg, []string{namespace})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	_, err = client.CreateTable(ctx, tableName, schema)
	require.NoError(t, err)
	t.Logf("Created table: %s.%s", namespace, tableName)

	writerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	committerTbl, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)

	logger := service.MockResources().Logger()
	comm, err := iberg.NewCommitter(committerTbl, logger)
	require.NoError(t, err)

	writer := iberg.NewWriter(writerTbl, comm, logger)
	defer writer.Close()

	// Create test messages with nested struct values
	batch := createTestBatch(t, []map[string]any{
		{
			"id": int64(1),
			"user": map[string]any{
				"name":  "Alice",
				"email": "alice@example.com",
				"age":   int32(30),
			},
			"address": map[string]any{
				"street": "123 Main St",
				"city":   "Seattle",
				"location": map[string]any{
					"lat": 47.6062,
					"lng": -122.3321,
				},
			},
		},
		{
			"id": int64(2),
			"user": map[string]any{
				"name":  "Bob",
				"email": nil, // null optional field
				"age":   int32(25),
			},
			"address": nil, // null nested struct
		},
		{
			"id": int64(3),
			"user": map[string]any{
				"name":  "Charlie",
				"email": "charlie@example.com",
				"age":   nil,
			},
			"address": map[string]any{
				"street": "456 Oak Ave",
				"city":   "Portland",
				"location": nil, // null deeply nested struct
			},
		},
	})

	err = writer.Write(ctx, batch)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	count, err := infra.CountIcebergRows(ctx, "rest", namespace, tableName)
	require.NoError(t, err)
	assert.Equal(t, 3, count, "expected 3 rows")

	t.Logf("Nested struct test passed: wrote %d rows with nested structs to %s.%s", count, namespace, tableName)
}

// createTestBatch creates a message batch from test data.
func createTestBatch(t *testing.T, data []map[string]any) service.MessageBatch {
	t.Helper()
	batch := make(service.MessageBatch, len(data))
	for i, d := range data {
		msg := service.NewMessage(nil)
		msg.SetStructured(d)
		batch[i] = msg
	}
	return batch
}

// createMessageWithMeta creates a message with structured data and metadata.
func createMessageWithMeta(t *testing.T, data map[string]any, metaKey, metaValue string) *service.Message {
	t.Helper()
	msg := service.NewMessage(nil)
	msg.SetStructured(data)
	msg.MetaSetMut(metaKey, metaValue)
	return msg
}
