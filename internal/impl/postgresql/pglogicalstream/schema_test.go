// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bschema "github.com/redpanda-data/benthos/v4/public/schema"
)

func TestPgTypeNameToCommonType(t *testing.T) {
	tests := []struct {
		typeName string
		expected bschema.CommonType
	}{
		{typeName: "bool", expected: bschema.Boolean},
		{typeName: "boolean", expected: bschema.Boolean},
		{typeName: "int2", expected: bschema.Int32},
		{typeName: "smallint", expected: bschema.Int32},
		{typeName: "int4", expected: bschema.Int32},
		{typeName: "integer", expected: bschema.Int32},
		{typeName: "int8", expected: bschema.Int64},
		{typeName: "bigint", expected: bschema.Int64},
		{typeName: "float4", expected: bschema.Float32},
		{typeName: "real", expected: bschema.Float32},
		{typeName: "float8", expected: bschema.Float64},
		{typeName: "numeric", expected: bschema.String},
		{typeName: "decimal", expected: bschema.String},
		{typeName: "text", expected: bschema.String},
		{typeName: "varchar", expected: bschema.String},
		{typeName: "character varying", expected: bschema.String},
		{typeName: "bpchar", expected: bschema.String},
		{typeName: "bytea", expected: bschema.ByteArray},
		{typeName: "date", expected: bschema.String},
		{typeName: "time", expected: bschema.String},
		{typeName: "timetz", expected: bschema.String},
		{typeName: "timestamp", expected: bschema.Timestamp},
		{typeName: "timestamptz", expected: bschema.Timestamp},
		{typeName: "timestamp without time zone", expected: bschema.Timestamp},
		{typeName: "timestamp with time zone", expected: bschema.Timestamp},
		{typeName: "json", expected: bschema.Any},
		{typeName: "jsonb", expected: bschema.Any},
		{typeName: "uuid", expected: bschema.String},
		// Case-insensitive (database/sql returns uppercase)
		{typeName: "BOOL", expected: bschema.Boolean},
		{typeName: "INT4", expected: bschema.Int32},
		{typeName: "INT8", expected: bschema.Int64},
		{typeName: "FLOAT4", expected: bschema.Float32},
		{typeName: "FLOAT8", expected: bschema.Float64},
		{typeName: "TEXT", expected: bschema.String},
		{typeName: "VARCHAR", expected: bschema.String},
		{typeName: "TIMESTAMP", expected: bschema.Timestamp},
		{typeName: "JSONB", expected: bschema.Any},
		{typeName: "UUID", expected: bschema.String},
		// Unknown types fall back to any
		{typeName: "unknown_type", expected: bschema.Any},
		{typeName: "INET", expected: bschema.Any},
		{typeName: "_INT4", expected: bschema.Any},
		{typeName: "_TEXT", expected: bschema.Any},
		{typeName: "", expected: bschema.Any},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := pgTypeNameToCommonType(tt.typeName)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRelationMessageToSchema(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := &RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "orders",
		Columns: []*RelationMessageColumn{
			{Name: "is_active", DataType: 16},    // bool
			{Name: "quantity", DataType: 23},     // int4
			{Name: "user_id", DataType: 20},      // int8
			{Name: "price", DataType: 700},       // float4
			{Name: "discount", DataType: 701},    // float8
			{Name: "description", DataType: 25},  // text
			{Name: "payload", DataType: 17},      // bytea
			{Name: "created_at", DataType: 1114}, // timestamp
			{Name: "amount", DataType: 1700},     // numeric -> string
		},
	}

	result := relationMessageToSchema(rel, typeMap)
	require.NotNil(t, result)

	parsed, err := bschema.ParseFromAny(result)
	require.NoError(t, err)

	assert.Equal(t, "orders", parsed.Name)
	assert.Equal(t, bschema.Object, parsed.Type)
	assert.False(t, parsed.Optional)
	require.Len(t, parsed.Children, 9)

	childByName := make(map[string]bschema.Common)
	for _, child := range parsed.Children {
		childByName[child.Name] = child
	}

	assert.Equal(t, bschema.Boolean, childByName["is_active"].Type)
	assert.Equal(t, bschema.Int32, childByName["quantity"].Type)
	assert.Equal(t, bschema.Int64, childByName["user_id"].Type)
	assert.Equal(t, bschema.Float32, childByName["price"].Type)
	assert.Equal(t, bschema.Float64, childByName["discount"].Type)
	assert.Equal(t, bschema.String, childByName["description"].Type)
	assert.Equal(t, bschema.ByteArray, childByName["payload"].Type)
	assert.Equal(t, bschema.Timestamp, childByName["created_at"].Type)
	assert.Equal(t, bschema.String, childByName["amount"].Type)

	// All columns are optional
	for _, child := range parsed.Children {
		assert.True(t, child.Optional, "column %s should be optional", child.Name)
	}
}

func TestRelationMessageToSchemaRoundtrip(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := &RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "events",
		Columns: []*RelationMessageColumn{
			{Name: "id", DataType: 20},            // int8
			{Name: "name", DataType: 25},          // text
			{Name: "occurred_at", DataType: 1114}, // timestamp
			{Name: "active", DataType: 16},        // bool
		},
	}

	result := relationMessageToSchema(rel, typeMap)
	require.NotNil(t, result)

	parsed, err := bschema.ParseFromAny(result)
	require.NoError(t, err)

	assert.Equal(t, "events", parsed.Name)
	assert.Equal(t, bschema.Object, parsed.Type)
	assert.False(t, parsed.Optional)
	require.Len(t, parsed.Children, 4)

	assert.Equal(t, "id", parsed.Children[0].Name)
	assert.Equal(t, bschema.Int64, parsed.Children[0].Type)

	assert.Equal(t, "name", parsed.Children[1].Name)
	assert.Equal(t, bschema.String, parsed.Children[1].Type)

	assert.Equal(t, "occurred_at", parsed.Children[2].Name)
	assert.Equal(t, bschema.Timestamp, parsed.Children[2].Type)

	assert.Equal(t, "active", parsed.Children[3].Name)
	assert.Equal(t, bschema.Boolean, parsed.Children[3].Type)
}

func TestRelationMessageToSchemaTimetz(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := &RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "appointments",
		Columns: []*RelationMessageColumn{
			{Name: "id", DataType: 23},                    // int4
			{Name: "appt_time", DataType: pgtype.TimetzOID}, // timetz — OID 1266, not in pgtype default map
		},
	}

	result := relationMessageToSchema(rel, typeMap)
	require.NotNil(t, result)

	parsed, err := bschema.ParseFromAny(result)
	require.NoError(t, err)

	require.Len(t, parsed.Children, 2)
	childByName := make(map[string]bschema.Common)
	for _, child := range parsed.Children {
		childByName[child.Name] = child
	}
	assert.Equal(t, bschema.Int32, childByName["id"].Type)
	assert.Equal(t, bschema.String, childByName["appt_time"].Type, "timetz should map to String via OID fallback")
}

func TestRelationMessageToSchemaUnknownOID(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := &RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "widgets",
		Columns: []*RelationMessageColumn{
			{Name: "id", DataType: 23},         // int4 — known OID
			{Name: "mystery", DataType: 99999}, // unknown OID — should fall back to string
		},
	}

	result := relationMessageToSchema(rel, typeMap)
	require.NotNil(t, result)

	parsed, err := bschema.ParseFromAny(result)
	require.NoError(t, err)

	assert.Equal(t, "widgets", parsed.Name)
	require.Len(t, parsed.Children, 2)

	childByName := make(map[string]bschema.Common)
	for _, child := range parsed.Children {
		childByName[child.Name] = child
	}

	assert.Equal(t, bschema.Int32, childByName["id"].Type)
	assert.Equal(t, bschema.Any, childByName["mystery"].Type)
}

func TestRelationMessageToSchemaEmptyTable(t *testing.T) {
	typeMap := pgtype.NewMap()

	rel := &RelationMessage{
		RelationID:   5,
		Namespace:    "public",
		RelationName: "empty_table",
		Columns:      []*RelationMessageColumn{},
	}

	result := relationMessageToSchema(rel, typeMap)
	require.NotNil(t, result)

	parsed, err := bschema.ParseFromAny(result)
	require.NoError(t, err)

	assert.Equal(t, "empty_table", parsed.Name)
	assert.Equal(t, bschema.Object, parsed.Type)
	assert.Empty(t, parsed.Children)
}
