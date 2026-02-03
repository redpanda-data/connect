/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildParquetSchema_SimpleFlat(t *testing.T) {
	// Schema: { id: int64, name: string }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 2 leaf columns
	require.Len(t, fieldToCol, 2)

	// Verify field ID to column index mapping
	// Field IDs should map to column indices
	assert.Contains(t, fieldToCol, 1)
	assert.Contains(t, fieldToCol, 2)

	// Column indices should be 0 and 1
	colIndices := make(map[int]bool)
	for _, colIdx := range fieldToCol {
		colIndices[colIdx] = true
	}
	assert.True(t, colIndices[0])
	assert.True(t, colIndices[1])
}

func TestBuildParquetSchema_NestedStruct(t *testing.T) {
	// Schema: { user: struct<name: string, age: int32> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "user",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
				},
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 2 leaf columns (name and age, not the struct itself)
	require.Len(t, fieldToCol, 2)

	// Field IDs 2 and 3 should be mapped
	assert.Contains(t, fieldToCol, 2)
	assert.Contains(t, fieldToCol, 3)

	// Field ID 1 (struct) should not be in the mapping (not a leaf)
	assert.NotContains(t, fieldToCol, 1)

	// Verify we can look up columns in the parquet schema
	col2, ok := pqSchema.Lookup("user", "name")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[2], col2.ColumnIndex)

	col3, ok := pqSchema.Lookup("user", "age")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[3], col3.ColumnIndex)
}

func TestBuildParquetSchema_List(t *testing.T) {
	// Schema: { tags: list<string> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "tags",
			Type: &iceberg.ListType{
				ElementID:       2,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: false,
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 1 leaf column (the list element)
	require.Len(t, fieldToCol, 1)

	// Field ID 2 (list element) should be mapped
	assert.Contains(t, fieldToCol, 2)

	// Field ID 1 (list) should not be in the mapping (not a leaf)
	assert.NotContains(t, fieldToCol, 1)

	// Verify parquet schema lookup (list uses "list"/"element" path)
	col, ok := pqSchema.Lookup("tags", "list", "element")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[2], col.ColumnIndex)
}

func TestBuildParquetSchema_Map(t *testing.T) {
	// Schema: { props: map<string, int64> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "props",
			Type: &iceberg.MapType{
				KeyID:         2,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       3,
				ValueType:     iceberg.PrimitiveTypes.Int64,
				ValueRequired: false,
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 2 leaf columns (key and value)
	require.Len(t, fieldToCol, 2)

	// Field IDs 2 (key) and 3 (value) should be mapped
	assert.Contains(t, fieldToCol, 2)
	assert.Contains(t, fieldToCol, 3)

	// Field ID 1 (map) should not be in the mapping (not a leaf)
	assert.NotContains(t, fieldToCol, 1)

	// Verify parquet schema lookup (map uses "key_value"/"key" and "key_value"/"value" paths)
	keyCol, ok := pqSchema.Lookup("props", "key_value", "key")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[2], keyCol.ColumnIndex)

	valCol, ok := pqSchema.Lookup("props", "key_value", "value")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[3], valCol.ColumnIndex)
}

func TestBuildParquetSchema_ListOfStructs(t *testing.T) {
	// Schema: { events: list<struct<type: string, ts: int64>> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "events",
			Type: &iceberg.ListType{
				ElementID: 2,
				Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 3, Name: "type", Type: iceberg.PrimitiveTypes.String, Required: false},
						{ID: 4, Name: "ts", Type: iceberg.PrimitiveTypes.Int64, Required: false},
					},
				},
				ElementRequired: false,
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 2 leaf columns (type and ts)
	require.Len(t, fieldToCol, 2)

	// Field IDs 3 and 4 should be mapped
	assert.Contains(t, fieldToCol, 3)
	assert.Contains(t, fieldToCol, 4)

	// Non-leaf fields should not be in mapping
	assert.NotContains(t, fieldToCol, 1)
	assert.NotContains(t, fieldToCol, 2)

	// Verify parquet schema lookup
	typeCol, ok := pqSchema.Lookup("events", "list", "element", "type")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[3], typeCol.ColumnIndex)

	tsCol, ok := pqSchema.Lookup("events", "list", "element", "ts")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[4], tsCol.ColumnIndex)
}

func TestBuildParquetSchema_DeeplyNested(t *testing.T) {
	// Schema: { a: struct<b: struct<c: int32>> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "a",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{
						ID:   2,
						Name: "b",
						Type: &iceberg.StructType{
							FieldList: []iceberg.NestedField{
								{ID: 3, Name: "c", Type: iceberg.PrimitiveTypes.Int32, Required: false},
							},
						},
						Required: false,
					},
				},
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 1 leaf column
	require.Len(t, fieldToCol, 1)

	// Only field ID 3 should be mapped
	assert.Contains(t, fieldToCol, 3)
	assert.NotContains(t, fieldToCol, 1)
	assert.NotContains(t, fieldToCol, 2)

	// Verify parquet schema lookup
	col, ok := pqSchema.Lookup("a", "b", "c")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[3], col.ColumnIndex)
}

func TestBuildParquetSchema_NestedListsInStruct(t *testing.T) {
	// Schema: { outer: struct<items: list<string>, values: list<int64>> }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "outer",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{
						ID:   2,
						Name: "items",
						Type: &iceberg.ListType{
							ElementID:       3,
							Element:         iceberg.PrimitiveTypes.String,
							ElementRequired: false,
						},
						Required: false,
					},
					{
						ID:   4,
						Name: "values",
						Type: &iceberg.ListType{
							ElementID:       5,
							Element:         iceberg.PrimitiveTypes.Int64,
							ElementRequired: false,
						},
						Required: false,
					},
				},
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 2 leaf columns (items element and values element)
	require.Len(t, fieldToCol, 2)

	// Field IDs 3 and 5 should be mapped
	assert.Contains(t, fieldToCol, 3)
	assert.Contains(t, fieldToCol, 5)

	// Verify parquet schema lookup
	itemsCol, ok := pqSchema.Lookup("outer", "items", "list", "element")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[3], itemsCol.ColumnIndex)

	valuesCol, ok := pqSchema.Lookup("outer", "values", "list", "element")
	require.True(t, ok)
	assert.Equal(t, fieldToCol[5], valuesCol.ColumnIndex)
}

func TestBuildParquetSchema_ComplexMixed(t *testing.T) {
	// Address book example schema
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:       1,
			Name:     "owner",
			Type:     iceberg.PrimitiveTypes.String,
			Required: true,
		},
		iceberg.NestedField{
			ID:   2,
			Name: "ownerPhoneNumbers",
			Type: &iceberg.ListType{
				ElementID:       3,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: true,
			},
			Required: false,
		},
		iceberg.NestedField{
			ID:   4,
			Name: "contacts",
			Type: &iceberg.ListType{
				ElementID: 5,
				Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 6, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: true},
						{ID: 7, Name: "phoneNumber", Type: iceberg.PrimitiveTypes.String, Required: false},
					},
				},
				ElementRequired: true,
			},
			Required: false,
		},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 4 leaf columns: owner, ownerPhoneNumbers element, contacts.name, contacts.phoneNumber
	require.Len(t, fieldToCol, 4)

	// Leaf field IDs
	assert.Contains(t, fieldToCol, 1) // owner
	assert.Contains(t, fieldToCol, 3) // ownerPhoneNumbers element
	assert.Contains(t, fieldToCol, 6) // contacts.name
	assert.Contains(t, fieldToCol, 7) // contacts.phoneNumber

	// Non-leaf IDs should not be present
	assert.NotContains(t, fieldToCol, 2) // ownerPhoneNumbers list
	assert.NotContains(t, fieldToCol, 4) // contacts list
	assert.NotContains(t, fieldToCol, 5) // contacts element struct

	// Verify column indices are unique and sequential
	colIndices := make([]int, 0, 4)
	for _, idx := range fieldToCol {
		colIndices = append(colIndices, idx)
	}
	// Sort not needed for uniqueness check
	seen := make(map[int]bool)
	for _, idx := range colIndices {
		assert.False(t, seen[idx], "duplicate column index %d", idx)
		seen[idx] = true
	}
}

func TestBuildParquetSchema_AllPrimitiveTypes(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "bool_col", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 2, Name: "int32_col", Type: iceberg.PrimitiveTypes.Int32, Required: false},
		iceberg.NestedField{ID: 3, Name: "int64_col", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 4, Name: "float32_col", Type: iceberg.PrimitiveTypes.Float32, Required: false},
		iceberg.NestedField{ID: 5, Name: "float64_col", Type: iceberg.PrimitiveTypes.Float64, Required: false},
		iceberg.NestedField{ID: 6, Name: "string_col", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 7, Name: "binary_col", Type: iceberg.PrimitiveTypes.Binary, Required: false},
		iceberg.NestedField{ID: 8, Name: "date_col", Type: iceberg.PrimitiveTypes.Date, Required: false},
		iceberg.NestedField{ID: 9, Name: "time_col", Type: iceberg.PrimitiveTypes.Time, Required: false},
		iceberg.NestedField{ID: 10, Name: "timestamp_col", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
		iceberg.NestedField{ID: 11, Name: "timestamptz_col", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
		iceberg.NestedField{ID: 12, Name: "uuid_col", Type: iceberg.PrimitiveTypes.UUID, Required: false},
	)

	pqSchema, fieldToCol, err := BuildParquetSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, pqSchema)

	// Should have 12 leaf columns
	require.Len(t, fieldToCol, 12)

	// All field IDs should be mapped
	for i := 1; i <= 12; i++ {
		assert.Contains(t, fieldToCol, i)
	}
}

func TestSchemaLeaves_SimpleStruct(t *testing.T) {
	st := iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		},
	}

	var leaves []schemaLeaf
	for leaf := range schemaLeaves(&st, -1, nil) {
		leaves = append(leaves, leaf)
	}

	require.Len(t, leaves, 2)

	// First leaf: id
	assert.Equal(t, 1, leaves[0].FieldID)
	assert.Equal(t, []string{"id"}, leaves[0].Path)

	// Second leaf: name
	assert.Equal(t, 2, leaves[1].FieldID)
	assert.Equal(t, []string{"name"}, leaves[1].Path)
}

func TestSchemaLeaves_NestedStruct(t *testing.T) {
	st := iceberg.StructType{
		FieldList: []iceberg.NestedField{
			{
				ID:   1,
				Name: "user",
				Type: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
						{ID: 3, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: false},
					},
				},
				Required: false,
			},
		},
	}

	var leaves []schemaLeaf
	for leaf := range schemaLeaves(&st, -1, nil) {
		leaves = append(leaves, leaf)
	}

	require.Len(t, leaves, 2)

	// First leaf: user.name
	assert.Equal(t, 2, leaves[0].FieldID)
	assert.Equal(t, []string{"user", "name"}, leaves[0].Path)

	// Second leaf: user.age
	assert.Equal(t, 3, leaves[1].FieldID)
	assert.Equal(t, []string{"user", "age"}, leaves[1].Path)
}

func TestSchemaLeaves_List(t *testing.T) {
	lt := iceberg.ListType{
		ElementID:       2,
		Element:         iceberg.PrimitiveTypes.String,
		ElementRequired: false,
	}

	var leaves []schemaLeaf
	for leaf := range schemaLeaves(&lt, 1, []string{"tags"}) {
		leaves = append(leaves, leaf)
	}

	require.Len(t, leaves, 1)

	// List element with parquet path convention
	assert.Equal(t, 2, leaves[0].FieldID)
	assert.Equal(t, []string{"tags", "list", "element"}, leaves[0].Path)
}

func TestSchemaLeaves_Map(t *testing.T) {
	mt := iceberg.MapType{
		KeyID:         2,
		KeyType:       iceberg.PrimitiveTypes.String,
		ValueID:       3,
		ValueType:     iceberg.PrimitiveTypes.Int64,
		ValueRequired: false,
	}

	var leaves []schemaLeaf
	for leaf := range schemaLeaves(&mt, 1, []string{"props"}) {
		leaves = append(leaves, leaf)
	}

	require.Len(t, leaves, 2)

	// Key
	assert.Equal(t, 2, leaves[0].FieldID)
	assert.Equal(t, []string{"props", "key_value", "key"}, leaves[0].Path)

	// Value
	assert.Equal(t, 3, leaves[1].FieldID)
	assert.Equal(t, []string{"props", "key_value", "value"}, leaves[1].Path)
}

func TestSchemaLeaves_ListOfStructs(t *testing.T) {
	lt := iceberg.ListType{
		ElementID: 2,
		Element: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "type", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 4, Name: "ts", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			},
		},
		ElementRequired: false,
	}

	var leaves []schemaLeaf
	for leaf := range schemaLeaves(&lt, 1, []string{"events"}) {
		leaves = append(leaves, leaf)
	}

	require.Len(t, leaves, 2)

	// type field
	assert.Equal(t, 3, leaves[0].FieldID)
	assert.Equal(t, []string{"events", "list", "element", "type"}, leaves[0].Path)

	// ts field
	assert.Equal(t, 4, leaves[1].FieldID)
	assert.Equal(t, []string{"events", "list", "element", "ts"}, leaves[1].Path)
}
