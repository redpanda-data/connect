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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestConnectorIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const namespace = "connector_test"
	infra.CreateNamespace(t, namespace)

	t.Run("Router", func(t *testing.T) {
		client := infra.NewCatalogClient(t, namespace)
		_, err := client.CreateTable(ctx, "router_test", iceberg.NewSchemaWithIdentifiers(
			1, nil,
			iceberg.NestedField{ID: 1, Name: "event_type", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
		))
		require.NoError(t, err)

		router := infra.NewRouter(t, namespace, "router_test")
		produce(t, ctx, router,
			`{"event_type":"click","payload":"button_1"}`,
			`{"event_type":"view","payload":"page_home"}`,
			`{"event_type":"click","payload":"button_2"}`,
		)

		rows := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."router_test";`, namespace))
		require.Equal(t, 3, rows[0].Count)
	})

	t.Run("RouterMultipleTables", func(t *testing.T) {
		client := infra.NewCatalogClient(t, namespace)
		schema := iceberg.NewSchemaWithIdentifiers(
			1, nil,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
		)
		for _, name := range []string{"events_clicks", "events_views"} {
			_, err := client.CreateTable(ctx, name, schema)
			require.NoError(t, err)
		}

		router := infra.NewRouter(t, namespace, `events_${!meta("event_type")}`)
		produceMessages(t, ctx, router, service.MessageBatch{
			createMessageWithMeta(t, map[string]any{"id": int64(1), "data": "click_1"}, "event_type", "clicks"),
			createMessageWithMeta(t, map[string]any{"id": int64(2), "data": "view_1"}, "event_type", "views"),
			createMessageWithMeta(t, map[string]any{"id": int64(3), "data": "click_2"}, "event_type", "clicks"),
			createMessageWithMeta(t, map[string]any{"id": int64(4), "data": "view_2"}, "event_type", "views"),
			createMessageWithMeta(t, map[string]any{"id": int64(5), "data": "click_3"}, "event_type", "clicks"),
		})

		clicks := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."events_clicks";`, namespace))
		require.Equal(t, 3, clicks[0].Count)

		views := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."events_views";`, namespace))
		require.Equal(t, 2, views[0].Count)
	})

	t.Run("ListValues", func(t *testing.T) {
		client := infra.NewCatalogClient(t, namespace)
		_, err := client.CreateTable(ctx, "list_test", iceberg.NewSchemaWithIdentifiers(
			1, nil,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{
				ID: 2, Name: "tags",
				Type:     &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String, ElementRequired: false},
				Required: false,
			},
			iceberg.NestedField{
				ID: 4, Name: "scores",
				Type:     &iceberg.ListType{ElementID: 5, Element: iceberg.PrimitiveTypes.Int64, ElementRequired: false},
				Required: false,
			},
		))
		require.NoError(t, err)

		router := infra.NewRouter(t, namespace, "list_test")
		produce(t, ctx, router,
			`{"id":1,"tags":["red","blue","green"],"scores":[100,200]}`,
			`{"id":2,"tags":["yellow"],"scores":[50,75,100]}`,
			`{"id":3,"tags":[],"scores":null}`,
		)

		rows := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."list_test";`, namespace))
		require.Equal(t, 3, rows[0].Count)
	})

	t.Run("NestedStruct", func(t *testing.T) {
		client := infra.NewCatalogClient(t, namespace)
		_, err := client.CreateTable(ctx, "nested_test", iceberg.NewSchemaWithIdentifiers(
			1, nil,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{
				ID: 2, Name: "user",
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
				ID: 6, Name: "address",
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
		))
		require.NoError(t, err)

		router := infra.NewRouter(t, namespace, "nested_test")
		produce(t, ctx, router,
			`{"id":1,"user":{"name":"Alice","email":"alice@example.com","age":30},"address":{"street":"123 Main St","city":"Seattle","location":{"lat":47.6062,"lng":-122.3321}}}`,
			`{"id":2,"user":{"name":"Bob","email":null,"age":25},"address":null}`,
			`{"id":3,"user":{"name":"Charlie","email":"charlie@example.com","age":null},"address":{"street":"456 Oak Ave","city":"Portland","location":null}}`,
		)

		rows := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."nested_test";`, namespace))
		require.Equal(t, 3, rows[0].Count)
	})

	t.Run("PartitionedTable", func(t *testing.T) {
		client := infra.NewCatalogClient(t, namespace)
		schema := iceberg.NewSchemaWithIdentifiers(
			1, nil,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "category", Type: iceberg.PrimitiveTypes.String, Required: true},
			iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.PrimitiveTypes.Float64, Required: false},
			iceberg.NestedField{ID: 4, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
		)
		partitionSpec := iceberg.NewPartitionSpec(
			iceberg.PartitionField{SourceID: 2, FieldID: 1000, Name: "category", Transform: iceberg.IdentityTransform{}},
			iceberg.PartitionField{SourceID: 4, FieldID: 1001, Name: "ts_day", Transform: iceberg.DayTransform{}},
		)
		_, err := client.CreateTable(ctx, "partitioned_test", schema, catalog.WithPartitionSpec(&partitionSpec))
		require.NoError(t, err)

		router := infra.NewRouter(t, namespace, "partitioned_test")
		// Timestamps as microseconds since epoch: 2024-01-15 and 2024-01-16 12:00:00 UTC
		produce(t, ctx, router,
			`{"id":1,"category":"electronics","value":100.0,"ts":1705320000000000}`,
			`{"id":2,"category":"electronics","value":200.0,"ts":1705320000000000}`,
			`{"id":3,"category":"clothing","value":50.0,"ts":1705320000000000}`,
			`{"id":4,"category":"electronics","value":150.0,"ts":1705406400000000}`,
			`{"id":5,"category":"clothing","value":75.0,"ts":1705406400000000}`,
			`{"id":6,"category":"food","value":25.0,"ts":1705406400000000}`,
		)

		tbl := fmt.Sprintf(`iceberg_cat."%s"."partitioned_test"`, namespace)

		total := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM %s;`, tbl))
		require.Equal(t, 6, total[0].Count)

		electronics := querySQL[map[string]any](t, ctx, infra,
			fmt.Sprintf(`SELECT * FROM %s WHERE category = 'electronics';`, tbl))
		require.Len(t, electronics, 3)

		clothing := querySQL[map[string]any](t, ctx, infra,
			fmt.Sprintf(`SELECT * FROM %s WHERE category = 'clothing';`, tbl))
		require.Len(t, clothing, 2)

		food := querySQL[map[string]any](t, ctx, infra,
			fmt.Sprintf(`SELECT * FROM %s WHERE category = 'food';`, tbl))
		require.Len(t, food, 1)

		// Verify data files (one per partition: 5 partitions = 5 files)
		metadata := querySQL[map[string]any](t, ctx, infra,
			fmt.Sprintf(`SELECT * FROM iceberg_metadata('%s');`, tbl))
		require.Len(t, metadata, 5, "expected 5 data files (one per partition)")

		snapshots := querySQL[map[string]any](t, ctx, infra,
			fmt.Sprintf(`SELECT * FROM iceberg_snapshots('%s');`, tbl))
		require.NotEmpty(t, snapshots)
	})
}
