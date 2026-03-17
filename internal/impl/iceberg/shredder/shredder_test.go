// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// testSink is a test implementation of Sink.
type testSink struct {
	values    []ShreddedValue
	newFields []newFieldRecord
}

type newFieldRecord struct {
	path  icebergx.Path
	name  string
	value any
}

func (s *testSink) EmitValue(sv ShreddedValue) error {
	s.values = append(s.values, sv)
	return nil
}

func (s *testSink) OnNewField(path icebergx.Path, name string, value any) {
	s.newFields = append(s.newFields, newFieldRecord{
		path:  append(icebergx.Path{}, path...), // copy to avoid mutation
		name:  name,
		value: value,
	})
}

func TestShredSimpleRecord(t *testing.T) {
	// Schema: { id: int64, name: string }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	record := map[string]any{
		"id":   int64(42),
		"name": "alice",
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2)

	// id: required field, rep=0, def=0
	assert.Equal(t, 1, sink.values[0].FieldID)
	assert.Equal(t, int64(42), sink.values[0].Value.Int64())
	assert.Equal(t, 0, sink.values[0].RepLevel)
	assert.Equal(t, 0, sink.values[0].DefLevel)

	// name: optional field (defined), rep=0, def=1
	assert.Equal(t, 2, sink.values[1].FieldID)
	assert.Equal(t, "alice", string(sink.values[1].Value.ByteArray()))
	assert.Equal(t, 0, sink.values[1].RepLevel)
	assert.Equal(t, 1, sink.values[1].DefLevel)
}

func TestShredNullOptionalField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	record := map[string]any{
		"id":   int64(42),
		"name": nil, // null value
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2)

	// id: required field
	assert.Equal(t, 1, sink.values[0].FieldID)
	assert.Equal(t, int64(42), sink.values[0].Value.Int64())

	// name: null, rep=0, def=0 (not defined)
	assert.Equal(t, 2, sink.values[1].FieldID)
	assert.True(t, sink.values[1].Value.IsNull())
	assert.Equal(t, 0, sink.values[1].RepLevel)
	assert.Equal(t, 0, sink.values[1].DefLevel)
}

func TestShredList(t *testing.T) {
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

	record := map[string]any{
		"tags": []any{"a", "b", "c"},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 3)

	// First element: rep=0 (new list)
	assert.Equal(t, 2, sink.values[0].FieldID)
	assert.Equal(t, "a", string(sink.values[0].Value.ByteArray()))
	assert.Equal(t, 0, sink.values[0].RepLevel)
	assert.Equal(t, 3, sink.values[0].DefLevel) // list defined (1) + element defined (2)

	// Second element: rep=1 (repeated)
	assert.Equal(t, 2, sink.values[1].FieldID)
	assert.Equal(t, "b", string(sink.values[1].Value.ByteArray()))
	assert.Equal(t, 1, sink.values[1].RepLevel)
	assert.Equal(t, 3, sink.values[1].DefLevel)

	// Third element: rep=1 (repeated)
	assert.Equal(t, 2, sink.values[2].FieldID)
	assert.Equal(t, "c", string(sink.values[2].Value.ByteArray()))
	assert.Equal(t, 1, sink.values[2].RepLevel)
	assert.Equal(t, 3, sink.values[2].DefLevel)
}

func TestShredEmptyList(t *testing.T) {
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

	record := map[string]any{
		"tags": []any{},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 1)

	// Empty list is treated as null.
	assert.Equal(t, 2, sink.values[0].FieldID)
	assert.True(t, sink.values[0].Value.IsNull())
	assert.Equal(t, 0, sink.values[0].RepLevel)
	assert.Equal(t, 1, sink.values[0].DefLevel) // list field's def level
}

func TestShredNestedStruct(t *testing.T) {
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

	record := map[string]any{
		"user": map[string]any{
			"name": "bob",
			"age":  int32(30),
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2)

	// name: def=2 (user defined + name defined)
	assert.Equal(t, 2, sink.values[0].FieldID)
	assert.Equal(t, "bob", string(sink.values[0].Value.ByteArray()))
	assert.Equal(t, 0, sink.values[0].RepLevel)
	assert.Equal(t, 2, sink.values[0].DefLevel)

	// age: def=2 (user defined + age defined)
	assert.Equal(t, 3, sink.values[1].FieldID)
	assert.Equal(t, int32(30), sink.values[1].Value.Int32())
	assert.Equal(t, 0, sink.values[1].RepLevel)
	assert.Equal(t, 2, sink.values[1].DefLevel)
}

func TestShredNullNestedStruct(t *testing.T) {
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

	record := map[string]any{
		"user": nil,
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2)

	// Both fields null with def=0 (user not defined)
	assert.Equal(t, 2, sink.values[0].FieldID)
	assert.True(t, sink.values[0].Value.IsNull())
	assert.Equal(t, 0, sink.values[0].DefLevel)

	assert.Equal(t, 3, sink.values[1].FieldID)
	assert.True(t, sink.values[1].Value.IsNull())
	assert.Equal(t, 0, sink.values[1].DefLevel)
}

func TestShredMap(t *testing.T) {
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

	// Use single-entry map for deterministic iteration.
	record := map[string]any{
		"props": map[string]any{
			"count": int64(100),
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2) // key + value

	// Key
	assert.Equal(t, 2, sink.values[0].FieldID)
	assert.Equal(t, "count", string(sink.values[0].Value.ByteArray()))
	assert.Equal(t, 0, sink.values[0].RepLevel)
	assert.Equal(t, 2, sink.values[0].DefLevel) // map defined + key defined

	// Value
	assert.Equal(t, 3, sink.values[1].FieldID)
	assert.Equal(t, int64(100), sink.values[1].Value.Int64())
	assert.Equal(t, 0, sink.values[1].RepLevel)
	assert.Equal(t, 3, sink.values[1].DefLevel) // map defined + value defined
}

func TestShredListOfStructs(t *testing.T) {
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

	record := map[string]any{
		"events": []any{
			map[string]any{"type": "click", "ts": int64(1000)},
			map[string]any{"type": "view", "ts": int64(2000)},
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 4) // 2 events * 2 fields

	// First event, type field: rep=0 (first in list)
	assert.Equal(t, 3, sink.values[0].FieldID)
	assert.Equal(t, "click", string(sink.values[0].Value.ByteArray()))
	assert.Equal(t, 0, sink.values[0].RepLevel)

	// First event, ts field: rep=0
	assert.Equal(t, 4, sink.values[1].FieldID)
	assert.Equal(t, int64(1000), sink.values[1].Value.Int64())
	assert.Equal(t, 0, sink.values[1].RepLevel)

	// Second event, type field: rep=1 (repeated list element)
	assert.Equal(t, 3, sink.values[2].FieldID)
	assert.Equal(t, "view", string(sink.values[2].Value.ByteArray()))
	assert.Equal(t, 1, sink.values[2].RepLevel)

	// Second event, ts field: rep=1
	assert.Equal(t, 4, sink.values[3].FieldID)
	assert.Equal(t, int64(2000), sink.values[3].Value.Int64())
	assert.Equal(t, 1, sink.values[3].RepLevel)
}

func TestShredMissingRequiredField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Missing required field "id".
	record := map[string]any{
		"name": "alice",
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required field")
	assert.Contains(t, err.Error(), "id")
}

func TestShredNullRequiredField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Null value for required field.
	record := map[string]any{
		"id": nil,
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required field")
}

func TestShredNewFieldDetection(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)

	// Record with extra field not in schema.
	record := map[string]any{
		"id":       int64(42),
		"newField": "surprise",
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)

	// Should have detected the new field.
	require.Len(t, sink.newFields, 1)
	assert.Equal(t, "newField", sink.newFields[0].name)
	assert.Equal(t, "surprise", sink.newFields[0].value)
	assert.Empty(t, sink.newFields[0].path) // top-level field
}

func TestShredNestedNewField(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "user",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
				},
			},
			Required: false,
		},
	)

	// Nested struct with extra field.
	record := map[string]any{
		"user": map[string]any{
			"name":  "alice",
			"email": "alice@example.com", // not in schema
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)

	// Should have detected the new field with path.
	require.Len(t, sink.newFields, 1)
	assert.Equal(t, "email", sink.newFields[0].name)
	assert.Equal(t, "alice@example.com", sink.newFields[0].value)
	require.Len(t, sink.newFields[0].path, 1)
	assert.Equal(t, icebergx.PathField, sink.newFields[0].path[0].Kind)
	assert.Equal(t, "user", sink.newFields[0].path[0].Name)
}

func TestShredNewFieldInList(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "items",
			Type: &iceberg.ListType{
				ElementID: 2,
				Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
					},
				},
				ElementRequired: false,
			},
			Required: false,
		},
	)

	// List element with extra field.
	record := map[string]any{
		"items": []any{
			map[string]any{
				"id":    int64(1),
				"extra": "value", // not in schema
			},
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)

	// Should have detected the new field with path including list marker.
	require.Len(t, sink.newFields, 1)
	assert.Equal(t, "extra", sink.newFields[0].name)
	assert.Equal(t, "value", sink.newFields[0].value)
	require.Len(t, sink.newFields[0].path, 2)
	assert.Equal(t, icebergx.PathField, sink.newFields[0].path[0].Kind)
	assert.Equal(t, "items", sink.newFields[0].path[0].Name)
	assert.Equal(t, icebergx.PathListElement, sink.newFields[0].path[1].Kind)
}

// Tests ported from redpanda/src/v/serde/parquet/tests/shredder_test.cc

// TestListOfStrings tests shredding a simple repeated string field.
// From: ListOfStrings test in shredder_test.cc
func TestListOfStrings(t *testing.T) {
	// Schema: repeated string (as a list with required elements)
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "values",
			Type: &iceberg.ListType{
				ElementID:       2,
				Element:         iceberg.PrimitiveTypes.String,
				ElementRequired: true,
			},
			Required: true,
		},
	)

	record := map[string]any{
		"values": []any{"a", "b", "c"},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 3)

	// Expected: rep levels [0, 1, 1], def level 1 (list is required, element is required)
	expected := []struct {
		value    string
		repLevel int
		defLevel int
	}{
		{"a", 0, 1},
		{"b", 1, 1},
		{"c", 1, 1},
	}

	for i, exp := range expected {
		assert.Equal(t, exp.value, string(sink.values[i].Value.ByteArray()), "value at %d", i)
		assert.Equal(t, exp.repLevel, sink.values[i].RepLevel, "rep level at %d", i)
		assert.Equal(t, exp.defLevel, sink.values[i].DefLevel, "def level at %d", i)
	}
}

// TestDefinitionLevels tests that definition levels correctly track null depth.
// From: DefinitionLevels test in shredder_test.cc
func TestDefinitionLevels(t *testing.T) {
	// Schema: optional { optional { optional { int32 } } }
	// Three levels of optional nesting with an optional leaf.
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
								{
									ID:       3,
									Name:     "c",
									Type:     iceberg.PrimitiveTypes.Int32,
									Required: false,
								},
							},
						},
						Required: false,
					},
				},
			},
			Required: false,
		},
	)

	tests := []struct {
		name     string
		record   map[string]any
		defLevel int
		isNull   bool
		value    int32
	}{
		{
			name:     "all defined",
			record:   map[string]any{"a": map[string]any{"b": map[string]any{"c": int32(42)}}},
			defLevel: 3,
			isNull:   false,
			value:    42,
		},
		{
			name:     "c is null",
			record:   map[string]any{"a": map[string]any{"b": map[string]any{"c": nil}}},
			defLevel: 2,
			isNull:   true,
		},
		{
			name:     "b is null",
			record:   map[string]any{"a": map[string]any{"b": nil}},
			defLevel: 1,
			isNull:   true,
		},
		{
			name:     "a is null",
			record:   map[string]any{"a": nil},
			defLevel: 0,
			isNull:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shredder := NewRecordShredder(schema)
			sink := &testSink{}
			err := shredder.Shred(tc.record, sink)
			require.NoError(t, err)
			require.Len(t, sink.values, 1)

			assert.Equal(t, tc.defLevel, sink.values[0].DefLevel)
			assert.Equal(t, 0, sink.values[0].RepLevel)
			if tc.isNull {
				assert.True(t, sink.values[0].Value.IsNull())
			} else {
				assert.Equal(t, tc.value, sink.values[0].Value.Int32())
			}
		})
	}
}

// TestRepetitionLevels tests that repetition levels correctly track nested list depth.
// From: RepetitionLevels test in shredder_test.cc
func TestRepetitionLevels(t *testing.T) {
	// Schema: repeated { repeated { string } }
	// Two levels of repeated nesting - list of lists of strings.
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "level1",
			Type: &iceberg.ListType{
				ElementID: 2,
				Element: &iceberg.ListType{
					ElementID:       3,
					Element:         iceberg.PrimitiveTypes.String,
					ElementRequired: true,
				},
				ElementRequired: true,
			},
			Required: true,
		},
	)

	// Record 1: [[a, b, c], [d, e, f, g]]
	record1 := map[string]any{
		"level1": []any{
			[]any{"a", "b", "c"},
			[]any{"d", "e", "f", "g"},
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record1, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 7)

	// Expected rep levels: [0, 2, 2, 1, 2, 2, 2]
	// 0 = new record, 1 = new outer list element, 2 = new inner list element
	expectedRep := []int{0, 2, 2, 1, 2, 2, 2}
	expectedValues := []string{"a", "b", "c", "d", "e", "f", "g"}

	for i, expRep := range expectedRep {
		assert.Equal(t, expRep, sink.values[i].RepLevel, "rep level at %d", i)
		assert.Equal(t, expectedValues[i], string(sink.values[i].Value.ByteArray()), "value at %d", i)
	}
}

// TestAddressBookExample tests a practical schema with mixed required/optional/repeated fields.
// From: AddressBookExample test in shredder_test.cc
func TestAddressBookExample(t *testing.T) {
	// Schema:
	// - owner: required string
	// - ownerPhoneNumbers: repeated string
	// - contacts: repeated struct {
	//     name: required string
	//     phoneNumber: optional string
	//   }
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

	// Record 1: owner with phone numbers and contacts with phone numbers
	record1 := map[string]any{
		"owner":             "Julien Le Dem",
		"ownerPhoneNumbers": []any{"555 123 4567", "555 666 1337"},
		"contacts": []any{
			map[string]any{"name": "Dmitriy Ryaboy", "phoneNumber": "555 987 6543"},
			map[string]any{"name": "Chris Aniszczyk", "phoneNumber": nil},
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record1, sink)
	require.NoError(t, err)

	// Expected columns:
	// 1. owner: "Julien Le Dem", rep=0, def=0 (required)
	// 2. ownerPhoneNumbers: "555 123 4567" rep=0, def=2; "555 666 1337" rep=1, def=2
	// 3. contacts.name: "Dmitriy Ryaboy" rep=0; "Chris Aniszczyk" rep=1
	// 4. contacts.phoneNumber: "555 987 6543" rep=0, def=3; NULL rep=1, def=2

	// Find values by field ID
	ownerValues := filterByFieldID(sink.values, 1)
	phoneValues := filterByFieldID(sink.values, 3)
	contactNames := filterByFieldID(sink.values, 6)
	contactPhones := filterByFieldID(sink.values, 7)

	require.Len(t, ownerValues, 1)
	assert.Equal(t, "Julien Le Dem", string(ownerValues[0].Value.ByteArray()))
	assert.Equal(t, 0, ownerValues[0].RepLevel)
	assert.Equal(t, 0, ownerValues[0].DefLevel)

	require.Len(t, phoneValues, 2)
	assert.Equal(t, "555 123 4567", string(phoneValues[0].Value.ByteArray()))
	assert.Equal(t, 0, phoneValues[0].RepLevel)
	assert.Equal(t, "555 666 1337", string(phoneValues[1].Value.ByteArray()))
	assert.Equal(t, 1, phoneValues[1].RepLevel)

	require.Len(t, contactNames, 2)
	assert.Equal(t, "Dmitriy Ryaboy", string(contactNames[0].Value.ByteArray()))
	assert.Equal(t, 0, contactNames[0].RepLevel)
	assert.Equal(t, "Chris Aniszczyk", string(contactNames[1].Value.ByteArray()))
	assert.Equal(t, 1, contactNames[1].RepLevel)

	require.Len(t, contactPhones, 2)
	assert.Equal(t, "555 987 6543", string(contactPhones[0].Value.ByteArray()))
	assert.Equal(t, 0, contactPhones[0].RepLevel)
	assert.True(t, contactPhones[1].Value.IsNull())
	assert.Equal(t, 1, contactPhones[1].RepLevel)
}

// TestAddressBookNoContacts tests a record with no contacts.
// From: AddressBookExample test in shredder_test.cc (second record)
func TestAddressBookNoContacts(t *testing.T) {
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

	// Record with no phone numbers and no contacts
	record := map[string]any{
		"owner":             "A. Nonymous",
		"ownerPhoneNumbers": []any{},
		"contacts":          []any{},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)

	ownerValues := filterByFieldID(sink.values, 1)
	phoneValues := filterByFieldID(sink.values, 3)
	contactNames := filterByFieldID(sink.values, 6)
	contactPhones := filterByFieldID(sink.values, 7)

	require.Len(t, ownerValues, 1)
	assert.Equal(t, "A. Nonymous", string(ownerValues[0].Value.ByteArray()))

	// Empty lists produce null values
	require.Len(t, phoneValues, 1)
	assert.True(t, phoneValues[0].Value.IsNull())

	require.Len(t, contactNames, 1)
	assert.True(t, contactNames[0].Value.IsNull())

	require.Len(t, contactPhones, 1)
	assert.True(t, contactPhones[0].Value.IsNull())
}

// TestRequiredGroupWrappedInOptionalGroup tests required fields inside optional groups.
// From: RequiredGroupWrappedInOptionalGroup test in shredder_test.cc
func TestRequiredGroupWrappedInOptionalGroup(t *testing.T) {
	// Schema: optional { required { required int32 } }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "optional_outer",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{
						ID:   2,
						Name: "required_inner",
						Type: &iceberg.StructType{
							FieldList: []iceberg.NestedField{
								{
									ID:       3,
									Name:     "value",
									Type:     iceberg.PrimitiveTypes.Int32,
									Required: true,
								},
							},
						},
						Required: true,
					},
				},
			},
			Required: false,
		},
	)

	tests := []struct {
		name     string
		record   map[string]any
		defLevel int
		isNull   bool
		value    int32
	}{
		{
			name:     "outer is null",
			record:   map[string]any{"optional_outer": nil},
			defLevel: 0,
			isNull:   true,
		},
		{
			name: "all defined",
			record: map[string]any{
				"optional_outer": map[string]any{
					"required_inner": map[string]any{
						"value": int32(42),
					},
				},
			},
			defLevel: 1, // Only the optional outer contributes to def level
			isNull:   false,
			value:    42,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shredder := NewRecordShredder(schema)
			sink := &testSink{}
			err := shredder.Shred(tc.record, sink)
			require.NoError(t, err)
			require.Len(t, sink.values, 1)

			assert.Equal(t, tc.defLevel, sink.values[0].DefLevel)
			if tc.isNull {
				assert.True(t, sink.values[0].Value.IsNull())
			} else {
				assert.Equal(t, tc.value, sink.values[0].Value.Int32())
			}
		})
	}
}

// TestRequiredValuesNotNullValidation tests that null values in required fields are rejected.
// From: RequiredValuesNotNullValidation test in shredder_test.cc
func TestRequiredValuesNotNullValidation(t *testing.T) {
	// Schema: optional { required { required int32 } }
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "optional_outer",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{
						ID:   2,
						Name: "required_inner",
						Type: &iceberg.StructType{
							FieldList: []iceberg.NestedField{
								{
									ID:       3,
									Name:     "value",
									Type:     iceberg.PrimitiveTypes.Int32,
									Required: true,
								},
							},
						},
						Required: true,
					},
				},
			},
			Required: false,
		},
	)

	tests := []struct {
		name   string
		record map[string]any
	}{
		{
			name: "required_inner is null",
			record: map[string]any{
				"optional_outer": map[string]any{
					"required_inner": nil,
				},
			},
		},
		{
			name: "value is null",
			record: map[string]any{
				"optional_outer": map[string]any{
					"required_inner": map[string]any{
						"value": nil,
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			shredder := NewRecordShredder(schema)
			sink := &testSink{}
			err := shredder.Shred(tc.record, sink)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing required field")
		})
	}
}

// TestLogicalMap tests shredding a map type.
// From: LogicalMap test in shredder_test.cc
func TestLogicalMap(t *testing.T) {
	// Schema: map<string, string> with optional values
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{
			ID:   1,
			Name: "states",
			Type: &iceberg.MapType{
				KeyID:         2,
				KeyType:       iceberg.PrimitiveTypes.String,
				ValueID:       3,
				ValueType:     iceberg.PrimitiveTypes.String,
				ValueRequired: false,
			},
			Required: false,
		},
	)

	// Use a slice to ensure deterministic order in the test
	// We'll test with a single entry first
	record := map[string]any{
		"states": map[string]any{
			"AL": "Alabama",
		},
	}

	shredder := NewRecordShredder(schema)
	sink := &testSink{}
	err := shredder.Shred(record, sink)
	require.NoError(t, err)
	require.Len(t, sink.values, 2) // key + value

	keys := filterByFieldID(sink.values, 2)
	values := filterByFieldID(sink.values, 3)

	require.Len(t, keys, 1)
	assert.Equal(t, "AL", string(keys[0].Value.ByteArray()))
	assert.Equal(t, 0, keys[0].RepLevel)

	require.Len(t, values, 1)
	assert.Equal(t, "Alabama", string(values[0].Value.ByteArray()))
	assert.Equal(t, 0, values[0].RepLevel)
}

// filterByFieldID filters shredded values by field ID.
func filterByFieldID(values []ShreddedValue, fieldID int) []ShreddedValue {
	var result []ShreddedValue
	for _, v := range values {
		if v.FieldID == fieldID {
			result = append(result, v)
		}
	}
	return result
}
