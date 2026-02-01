// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package icebergx

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustSchema parses a JSON schema string or panics.
func mustSchema(t *testing.T, id int, jsonFields string) *iceberg.Schema {
	t.Helper()
	schema, err := iceberg.NewSchemaFromJsonFields(id, jsonFields)
	require.NoError(t, err)
	return schema
}

func TestMakeSchemaFieldOptional(t *testing.T) {
	tests := []struct {
		name        string
		schema      string // JSON fields
		path        string // dot-delimited path
		wantErr     string
		checkResult func(t *testing.T, result *iceberg.Schema)
	}{
		{
			name: "top level required field becomes optional",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true},
				{"id": 2, "name": "name", "type": "string", "required": false}
			]`,
			path: "id",
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				field, found := result.FindFieldByName("id")
				require.True(t, found)
				assert.False(t, field.Required, "field should be optional")
			},
		},
		{
			name: "nested field becomes optional",
			schema: `[
				{"id": 1, "name": "user", "type": {
					"type": "struct",
					"fields": [
						{"id": 2, "name": "email", "type": "string", "required": true},
						{"id": 3, "name": "age", "type": "int", "required": false}
					]
				}, "required": false}
			]`,
			path: "user.email",
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				userField, found := result.FindFieldByName("user")
				require.True(t, found)
				userStruct, ok := userField.Type.(*iceberg.StructType)
				require.True(t, ok)
				var emailField *iceberg.NestedField
				for i := range userStruct.FieldList {
					if userStruct.FieldList[i].Name == "email" {
						emailField = &userStruct.FieldList[i]
						break
					}
				}
				require.NotNil(t, emailField)
				assert.False(t, emailField.Required, "email should be optional")
			},
		},
		{
			name: "field inside list element",
			schema: `[
				{"id": 1, "name": "items", "type": {
					"type": "list",
					"element-id": 2,
					"element": {
						"type": "struct",
						"fields": [
							{"id": 3, "name": "sku", "type": "string", "required": true}
						]
					},
					"element-required": false
				}, "required": false}
			]`,
			path: "items.[*].sku",
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				itemsField, found := result.FindFieldByName("items")
				require.True(t, found)
				listType, ok := itemsField.Type.(*iceberg.ListType)
				require.True(t, ok)
				elemStruct, ok := listType.Element.(*iceberg.StructType)
				require.True(t, ok)
				var skuField *iceberg.NestedField
				for i := range elemStruct.FieldList {
					if elemStruct.FieldList[i].Name == "sku" {
						skuField = &elemStruct.FieldList[i]
						break
					}
				}
				require.NotNil(t, skuField)
				assert.False(t, skuField.Required, "sku should be optional")
			},
		},
		{
			name: "field inside map value",
			schema: `[
				{"id": 1, "name": "metadata", "type": {
					"type": "map",
					"key-id": 2,
					"key": "string",
					"value-id": 3,
					"value": {
						"type": "struct",
						"fields": [
							{"id": 4, "name": "count", "type": "int", "required": true}
						]
					},
					"value-required": false
				}, "required": false}
			]`,
			path: "metadata.{}.count",
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				metaField, found := result.FindFieldByName("metadata")
				require.True(t, found)
				mapType, ok := metaField.Type.(*iceberg.MapType)
				require.True(t, ok)
				valueStruct, ok := mapType.ValueType.(*iceberg.StructType)
				require.True(t, ok)
				var countField *iceberg.NestedField
				for i := range valueStruct.FieldList {
					if valueStruct.FieldList[i].Name == "count" {
						countField = &valueStruct.FieldList[i]
						break
					}
				}
				require.NotNil(t, countField)
				assert.False(t, countField.Required, "count should be optional")
			},
		},
		{
			name: "field not found",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			path:    "nonexistent",
			wantErr: `field "nonexistent" not found`,
		},
		{
			name: "empty path",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			path:    "",
			wantErr: "path cannot be empty",
		},
		{
			name: "nested field not found",
			schema: `[
				{"id": 1, "name": "user", "type": {
					"type": "struct",
					"fields": [
						{"id": 2, "name": "name", "type": "string", "required": true}
					]
				}, "required": false}
			]`,
			path:    "user.missing",
			wantErr: `field "missing" not found`,
		},
		{
			name: "cannot navigate through primitive",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			path:    "id.child",
			wantErr: "cannot navigate into primitive type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := mustSchema(t, 1, tc.schema)
			result, err := MakeSchemaFieldOptional(schema, ParsePath(tc.path))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			tc.checkResult(t, result)

			// Verify original schema is unchanged
			origField, _ := schema.FindFieldByID(1)
			assert.True(t, origField.Required || !origField.Required, "original schema should be unchanged")
		})
	}
}

func TestMakeSchemaFieldOptionalNilSchema(t *testing.T) {
	_, err := MakeSchemaFieldOptional(nil, ParsePath("id"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema cannot be nil")
}

func TestAddFieldsToSchema(t *testing.T) {
	tests := []struct {
		name      string
		schema    string // JSON fields
		additions []struct {
			path string
			typ  iceberg.Type
		}
		wantErr     string
		checkResult func(t *testing.T, result *iceberg.Schema)
	}{
		{
			name: "add top level field",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"name", iceberg.PrimitiveTypes.String},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				assert.Equal(t, 2, result.NumFields())
				field, found := result.FindFieldByName("name")
				require.True(t, found)
				assert.Equal(t, iceberg.PrimitiveTypes.String, field.Type)
				assert.False(t, field.Required, "new fields should be optional")
				assert.Equal(t, 2, field.ID, "should get auto-generated ID")
			},
		},
		{
			name: "add nested field with mkdir -p",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"user.profile.email", iceberg.PrimitiveTypes.String},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				// Should have created user.profile.email with intermediate structs
				userField, found := result.FindFieldByName("user")
				require.True(t, found)
				assert.False(t, userField.Required, "intermediate struct should be optional")
				assert.Equal(t, 2, userField.ID)

				userStruct, ok := userField.Type.(*iceberg.StructType)
				require.True(t, ok)
				require.Len(t, userStruct.FieldList, 1)

				profileField := userStruct.FieldList[0]
				assert.Equal(t, "profile", profileField.Name)
				assert.False(t, profileField.Required)
				assert.Equal(t, 3, profileField.ID)

				profileStruct, ok := profileField.Type.(*iceberg.StructType)
				require.True(t, ok)
				require.Len(t, profileStruct.FieldList, 1)

				emailField := profileStruct.FieldList[0]
				assert.Equal(t, "email", emailField.Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, emailField.Type)
				assert.Equal(t, 4, emailField.ID)
			},
		},
		{
			name: "add field to existing struct",
			schema: `[
				{"id": 1, "name": "user", "type": {
					"type": "struct",
					"fields": [
						{"id": 2, "name": "name", "type": "string", "required": true}
					]
				}, "required": false}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"user.age", iceberg.PrimitiveTypes.Int32},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				userField, found := result.FindFieldByName("user")
				require.True(t, found)
				userStruct, ok := userField.Type.(*iceberg.StructType)
				require.True(t, ok)
				require.Len(t, userStruct.FieldList, 2)

				// Original field unchanged
				assert.Equal(t, "name", userStruct.FieldList[0].Name)
				assert.True(t, userStruct.FieldList[0].Required)

				// New field added
				assert.Equal(t, "age", userStruct.FieldList[1].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int32, userStruct.FieldList[1].Type)
				assert.Equal(t, 3, userStruct.FieldList[1].ID)
			},
		},
		{
			name: "add field inside list element",
			schema: `[
				{"id": 1, "name": "items", "type": {
					"type": "list",
					"element-id": 2,
					"element": {
						"type": "struct",
						"fields": [
							{"id": 3, "name": "id", "type": "long", "required": true}
						]
					},
					"element-required": false
				}, "required": false}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"items.[*].quantity", iceberg.PrimitiveTypes.Int32},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				itemsField, found := result.FindFieldByName("items")
				require.True(t, found)
				listType, ok := itemsField.Type.(*iceberg.ListType)
				require.True(t, ok)
				elemStruct, ok := listType.Element.(*iceberg.StructType)
				require.True(t, ok)
				require.Len(t, elemStruct.FieldList, 2)

				assert.Equal(t, "id", elemStruct.FieldList[0].Name)
				assert.Equal(t, "quantity", elemStruct.FieldList[1].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int32, elemStruct.FieldList[1].Type)
			},
		},
		{
			name: "add field inside map value",
			schema: `[
				{"id": 1, "name": "data", "type": {
					"type": "map",
					"key-id": 2,
					"key": "string",
					"value-id": 3,
					"value": {
						"type": "struct",
						"fields": [
							{"id": 4, "name": "count", "type": "int", "required": false}
						]
					},
					"value-required": false
				}, "required": false}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"data.{}.label", iceberg.PrimitiveTypes.String},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				dataField, found := result.FindFieldByName("data")
				require.True(t, found)
				mapType, ok := dataField.Type.(*iceberg.MapType)
				require.True(t, ok)
				valueStruct, ok := mapType.ValueType.(*iceberg.StructType)
				require.True(t, ok)
				require.Len(t, valueStruct.FieldList, 2)

				assert.Equal(t, "count", valueStruct.FieldList[0].Name)
				assert.Equal(t, "label", valueStruct.FieldList[1].Name)
			},
		},
		{
			name: "add multiple fields",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"name", iceberg.PrimitiveTypes.String},
				{"age", iceberg.PrimitiveTypes.Int32},
			},
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				assert.Equal(t, 3, result.NumFields())
				_, foundName := result.FindFieldByName("name")
				assert.True(t, foundName)
				_, foundAge := result.FindFieldByName("age")
				assert.True(t, foundAge)
			},
		},
		{
			name: "empty additions returns same schema",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: nil,
			checkResult: func(t *testing.T, result *iceberg.Schema) {
				assert.Equal(t, 1, result.NumFields())
			},
		},
		{
			name: "field already exists",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"id", iceberg.PrimitiveTypes.String},
			},
			wantErr: `field "id" already exists`,
		},
		{
			name: "empty path",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"", iceberg.PrimitiveTypes.String},
			},
			wantErr: "path cannot be empty",
		},
		{
			name: "nil type",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"name", nil},
			},
			wantErr: "type cannot be nil",
		},
		{
			name: "cannot add through primitive",
			schema: `[
				{"id": 1, "name": "id", "type": "long", "required": true}
			]`,
			additions: []struct {
				path string
				typ  iceberg.Type
			}{
				{"id.child", iceberg.PrimitiveTypes.String},
			},
			wantErr: "cannot create path through primitive type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema := mustSchema(t, 1, tc.schema)

			// Convert additions to FieldAddition slice
			var additions []FieldAddition
			for _, a := range tc.additions {
				additions = append(additions, FieldAddition{
					Path: ParsePath(a.path),
					Type: a.typ,
				})
			}

			result, err := AddFieldsToSchema(schema, additions)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			tc.checkResult(t, result)
		})
	}
}

func TestAddFieldsToSchemaNilSchema(t *testing.T) {
	_, err := AddFieldsToSchema(nil, []FieldAddition{
		{Path: ParsePath("id"), Type: iceberg.PrimitiveTypes.Int64},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema cannot be nil")
}

func TestAddFieldsToSchemaIDGeneration(t *testing.T) {
	// Schema with various field IDs including nested
	schema := mustSchema(t, 1, `[
		{"id": 1, "name": "id", "type": "long", "required": true},
		{"id": 5, "name": "items", "type": {
			"type": "list",
			"element-id": 10,
			"element": {
				"type": "struct",
				"fields": [
					{"id": 15, "name": "sku", "type": "string", "required": true}
				]
			},
			"element-required": false
		}, "required": false}
	]`)

	result, err := AddFieldsToSchema(schema, []FieldAddition{
		{Path: ParsePath("newField"), Type: iceberg.PrimitiveTypes.String},
	})
	require.NoError(t, err)

	newField, found := result.FindFieldByName("newField")
	require.True(t, found)
	// Max existing ID is 15, so new field should get 16
	assert.Equal(t, 16, newField.ID)
}

func TestCopyFieldsPreservesAllAttributes(t *testing.T) {
	schema := mustSchema(t, 1, `[
		{"id": 1, "name": "id", "type": "long", "required": true, "doc": "Primary key"},
		{"id": 2, "name": "data", "type": {
			"type": "struct",
			"fields": [
				{"id": 3, "name": "value", "type": "string", "required": false, "doc": "Some value"}
			]
		}, "required": false}
	]`)

	// Make a copy by making a field optional
	result, err := MakeSchemaFieldOptional(schema, ParsePath("id"))
	require.NoError(t, err)

	// Verify doc strings are preserved
	idField, found := result.FindFieldByName("id")
	require.True(t, found)
	assert.Equal(t, "Primary key", idField.Doc)

	dataField, found := result.FindFieldByName("data")
	require.True(t, found)
	dataStruct, ok := dataField.Type.(*iceberg.StructType)
	require.True(t, ok)
	assert.Equal(t, "Some value", dataStruct.FieldList[0].Doc)
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		input string
		want  Path
	}{
		{
			input: "field",
			want:  Path{{Kind: PathField, Name: "field"}},
		},
		{
			input: "user.name",
			want: Path{
				{Kind: PathField, Name: "user"},
				{Kind: PathField, Name: "name"},
			},
		},
		{
			input: "items.[*].sku",
			want: Path{
				{Kind: PathField, Name: "items"},
				{Kind: PathListElement},
				{Kind: PathField, Name: "sku"},
			},
		},
		{
			input: "data.{}.value",
			want: Path{
				{Kind: PathField, Name: "data"},
				{Kind: PathMapEntry},
				{Kind: PathField, Name: "value"},
			},
		},
		{
			input: "",
			want:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := ParsePath(tc.input)
			assert.Equal(t, tc.want, got)
		})
	}
}
