// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// parseSchema is a test helper that round-trips a serialised schema through
// ParseFromAny and returns the result.
func parseSchema(t *testing.T, s any) schema.Common {
	t.Helper()
	require.NotNil(t, s)
	c, err := schema.ParseFromAny(s)
	require.NoError(t, err)
	return c
}

// childByName finds a child by name in a Common schema.
func childByName(t *testing.T, c schema.Common, name string) schema.Common {
	t.Helper()
	for i := range c.Children {
		if c.Children[i].Name == name {
			return c.Children[i]
		}
	}
	t.Fatalf("child %q not found in %v", name, c.Children)
	return schema.Common{}
}

// ---------------------------------------------------------------------------
// Tier 1: $jsonSchema conversion
// ---------------------------------------------------------------------------

func TestBsonTypeStringToCommon(t *testing.T) {
	tests := []struct {
		bsonType string
		expected schema.CommonType
	}{
		{"bool", schema.Boolean},
		{"int", schema.Int32},
		{"long", schema.Int64},
		{"double", schema.Float64},
		{"string", schema.String},
		{"binData", schema.ByteArray},
		{"date", schema.Timestamp},
		{"timestamp", schema.Timestamp},
		{"objectId", schema.String},
		{"decimal", schema.BigDecimal},
		{"object", schema.Object},
		{"array", schema.Array},
		{"", schema.Any},
		{"unknown", schema.Any},
	}
	for _, tt := range tests {
		t.Run(tt.bsonType, func(t *testing.T) {
			assert.Equal(t, tt.expected, bsonTypeStringToCommon(tt.bsonType))
		})
	}
}

func TestSchemaFromJSONSchemaBasic(t *testing.T) {
	s, keys, err := schemaFromJSONSchema("test_coll", bson.M{
		"bsonType": "object",
		"required": bson.A{"name"},
		"properties": bson.M{
			"name": bson.M{"bsonType": "string"},
			"age":  bson.M{"bsonType": "int"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, []string{"_id", "age", "name"}, keys) // _id auto-injected

	c := parseSchema(t, s)
	assert.Equal(t, "test_coll", c.Name)
	assert.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 3)

	// Sorted alphabetically, _id auto-injected first
	assert.Equal(t, "_id", c.Children[0].Name)
	assert.Equal(t, schema.String, c.Children[0].Type)
	assert.True(t, c.Children[0].Optional) // auto-injected

	assert.Equal(t, "age", c.Children[1].Name)
	assert.Equal(t, schema.Int32, c.Children[1].Type)
	assert.True(t, c.Children[1].Optional) // not in required

	assert.Equal(t, "name", c.Children[2].Name)
	assert.Equal(t, schema.String, c.Children[2].Type)
	assert.False(t, c.Children[2].Optional) // in required
}

func TestSchemaFromJSONSchemaBsonTypeArray(t *testing.T) {
	tests := []struct {
		name         string
		bsonType     bson.A
		expectedType schema.CommonType
		expectOptl   bool // additional optionality from null in array
	}{
		{"string_null", bson.A{"string", "null"}, schema.String, true},
		{"string_int", bson.A{"string", "int"}, schema.Any, false},
		{"null_only", bson.A{"null"}, schema.Any, true},
		{"empty", bson.A{}, schema.Any, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _, err := schemaFromJSONSchema("coll", bson.M{
				"bsonType": "object",
				"properties": bson.M{
					"field": bson.M{"bsonType": tt.bsonType},
				},
			})
			require.NoError(t, err)
			c := parseSchema(t, s)
			f := childByName(t, c, "field")
			assert.Equal(t, tt.expectedType, f.Type)
			if tt.expectOptl {
				assert.True(t, f.Optional)
			}
		})
	}
}

func TestSchemaFromJSONSchemaNestedObject(t *testing.T) {
	s, _, err := schemaFromJSONSchema("coll", bson.M{
		"bsonType": "object",
		"properties": bson.M{
			"address": bson.M{
				"bsonType": "object",
				"required": bson.A{"city"},
				"properties": bson.M{
					"city":  bson.M{"bsonType": "string"},
					"zip":   bson.M{"bsonType": "string"},
					"alpha": bson.M{"bsonType": "int"},
				},
			},
		},
	})
	require.NoError(t, err)
	c := parseSchema(t, s)
	addr := childByName(t, c, "address")
	assert.Equal(t, schema.Object, addr.Type)
	require.Len(t, addr.Children, 3)
	// Sorted alphabetically
	assert.Equal(t, "alpha", addr.Children[0].Name)
	assert.Equal(t, "city", addr.Children[1].Name)
	assert.False(t, addr.Children[1].Optional)
	assert.Equal(t, "zip", addr.Children[2].Name)
	assert.True(t, addr.Children[2].Optional)
}

func TestSchemaFromJSONSchemaArrayWithItems(t *testing.T) {
	s, _, err := schemaFromJSONSchema("coll", bson.M{
		"bsonType": "object",
		"properties": bson.M{
			"tags": bson.M{
				"bsonType": "array",
				"items":    bson.M{"bsonType": "string"},
			},
		},
	})
	require.NoError(t, err)
	c := parseSchema(t, s)
	tags := childByName(t, c, "tags")
	assert.Equal(t, schema.Array, tags.Type)
	require.Len(t, tags.Children, 1)
	assert.Equal(t, schema.String, tags.Children[0].Type)
}

func TestSchemaFromJSONSchemaCombinatorField(t *testing.T) {
	for _, combinator := range []string{"oneOf", "anyOf", "allOf", "not"} {
		t.Run(combinator, func(t *testing.T) {
			s, _, err := schemaFromJSONSchema("coll", bson.M{
				"bsonType": "object",
				"properties": bson.M{
					"data": bson.M{combinator: bson.A{}},
				},
			})
			require.NoError(t, err)
			c := parseSchema(t, s)
			assert.Equal(t, schema.Any, childByName(t, c, "data").Type)
		})
	}
}

func TestSchemaFromJSONSchemaNoProperties(t *testing.T) {
	s, keys, err := schemaFromJSONSchema("coll", bson.M{
		"bsonType": "object",
		"oneOf":    bson.A{},
	})
	require.NoError(t, err)
	assert.Nil(t, s)
	assert.Nil(t, keys)
}

// ---------------------------------------------------------------------------
// Tier 2: Document inference
// ---------------------------------------------------------------------------

func TestInferSchemaFromDocumentTypes(t *testing.T) {
	doc := bson.M{
		"bool_field":    true,
		"int32_field":   int32(42),
		"int64_field":   int64(99),
		"float64_field": 3.14,
		"string_field":  "hello",
		"binary_field":  bson.Binary{Data: []byte("data")},
		"date_field":    bson.DateTime(time.Now().UnixMilli()),
		"ts_field":      bson.Timestamp{T: 1, I: 1},
		"oid_field":     bson.ObjectID{},
		"dec_field":     bson.Decimal128{},
		"nested_field":  bson.M{"x": int32(1)},
		"array_field":   bson.A{"a", "b"},
		"nil_field":     nil,
	}

	s, keys := inferSchemaFromDocument("coll", doc)
	require.NotNil(t, s)
	assert.Len(t, keys, 13)

	c := parseSchema(t, s)
	assert.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 13)

	expectations := map[string]schema.CommonType{
		"array_field":   schema.Array,
		"binary_field":  schema.ByteArray,
		"bool_field":    schema.Boolean,
		"date_field":    schema.Timestamp,
		"dec_field":     schema.BigDecimal,
		"float64_field": schema.Float64,
		"int32_field":   schema.Int32,
		"int64_field":   schema.Int64,
		"nested_field":  schema.Object,
		"nil_field":     schema.Any,
		"oid_field":     schema.String,
		"string_field":  schema.String,
		"ts_field":      schema.Timestamp,
	}
	for _, child := range c.Children {
		expected, ok := expectations[child.Name]
		require.True(t, ok, "unexpected child: %s", child.Name)
		assert.Equal(t, expected, child.Type, "wrong type for %s", child.Name)
		assert.True(t, child.Optional, "%s should be optional", child.Name)
	}
}

func TestInferSchemaFromDocumentNestedChildren(t *testing.T) {
	doc := bson.M{
		"outer": bson.M{
			"zebra": "z",
			"alpha": int32(1),
		},
	}
	s, _ := inferSchemaFromDocument("coll", doc)
	c := parseSchema(t, s)
	outer := childByName(t, c, "outer")
	assert.Equal(t, schema.Object, outer.Type)
	require.Len(t, outer.Children, 2)
	assert.Equal(t, "alpha", outer.Children[0].Name)
	assert.Equal(t, "zebra", outer.Children[1].Name)
}

func TestInferSchemaFromDocumentMixedArray(t *testing.T) {
	doc := bson.M{"mixed": bson.A{"string", int32(42)}}
	s, _ := inferSchemaFromDocument("coll", doc)
	c := parseSchema(t, s)
	mixed := childByName(t, c, "mixed")
	assert.Equal(t, schema.Array, mixed.Type)
	require.Len(t, mixed.Children, 1)
	assert.Equal(t, schema.Any, mixed.Children[0].Type)
}

func TestInferSchemaFromDocumentEmpty(t *testing.T) {
	s, keys := inferSchemaFromDocument("coll", bson.M{})
	c := parseSchema(t, s)
	assert.Equal(t, schema.Object, c.Type)
	assert.Empty(t, c.Children)
	assert.Empty(t, keys)
}

// ---------------------------------------------------------------------------
// Deterministic ordering
// ---------------------------------------------------------------------------

func TestInferSchemaFieldOrdering(t *testing.T) {
	doc := bson.M{
		"zulu":  "z",
		"alpha": "a",
		"mike":  "m",
		"bravo": "b",
	}

	// Run multiple times to catch map iteration non-determinism.
	var prev []string
	for range 20 {
		s, keys := inferSchemaFromDocument("coll", doc)
		c := parseSchema(t, s)

		names := make([]string, len(c.Children))
		for i, ch := range c.Children {
			names[i] = ch.Name
		}
		assert.Equal(t, []string{"alpha", "bravo", "mike", "zulu"}, names)
		assert.Equal(t, []string{"alpha", "bravo", "mike", "zulu"}, keys)
		if prev != nil {
			assert.Equal(t, prev, names, "field ordering should be deterministic across iterations")
		}
		prev = names
	}
}

func TestSchemaFromJSONSchemaFieldOrdering(t *testing.T) {
	props := bson.M{
		"zulu":  bson.M{"bsonType": "string"},
		"alpha": bson.M{"bsonType": "int"},
		"mike":  bson.M{"bsonType": "bool"},
	}
	for range 20 {
		s, keys, err := schemaFromJSONSchema("coll", bson.M{
			"bsonType":   "object",
			"properties": props,
		})
		require.NoError(t, err)
		c := parseSchema(t, s)
		names := make([]string, len(c.Children))
		for i, ch := range c.Children {
			names[i] = ch.Name
		}
		assert.Equal(t, []string{"_id", "alpha", "mike", "zulu"}, names)
		assert.Equal(t, []string{"_id", "alpha", "mike", "zulu"}, keys)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func TestSortedMapKeys(t *testing.T) {
	m := bson.M{"z": 1, "a": 2, "m": 3}
	assert.Equal(t, []string{"a", "m", "z"}, sortedMapKeys(m))
}

func decimal128FromString(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	require.NoError(t, err)
	return d
}

func TestNormaliseDecimal128TopLevel(t *testing.T) {
	d := decimal128FromString(t, "1.5")
	got := normaliseDecimal128(d)
	assert.Equal(t, "1.5", got)
}

func TestNormaliseDecimal128InsideMap(t *testing.T) {
	doc := bson.M{
		"amount": decimal128FromString(t, "12345.6789"),
		"name":   "alice",
		"count":  int64(7),
	}
	got := normaliseDecimal128(doc)
	m := got.(bson.M)
	assert.Equal(t, "12345.6789", m["amount"])
	assert.Equal(t, "alice", m["name"])
	assert.Equal(t, int64(7), m["count"])
}

func TestNormaliseDecimal128InsideArray(t *testing.T) {
	arr := bson.A{
		decimal128FromString(t, "1"),
		decimal128FromString(t, "-2.5"),
		"plain string",
	}
	got := normaliseDecimal128(arr)
	out := got.(bson.A)
	assert.Equal(t, "1", out[0])
	assert.Equal(t, "-2.5", out[1])
	assert.Equal(t, "plain string", out[2])
}

func TestNormaliseDecimal128InsideD(t *testing.T) {
	d := bson.D{
		{Key: "amount", Value: decimal128FromString(t, "100.00")},
		{Key: "name", Value: "bob"},
	}
	got := normaliseDecimal128(d)
	out := got.(bson.D)
	assert.Equal(t, "amount", out[0].Key)
	assert.Equal(t, "100.00", out[0].Value)
	assert.Equal(t, "name", out[1].Key)
	assert.Equal(t, "bob", out[1].Value)
}

func TestNormaliseDecimal128Nested(t *testing.T) {
	doc := bson.M{
		"profile": bson.M{
			"balance": decimal128FromString(t, "999.99"),
			"history": bson.A{
				bson.M{"value": decimal128FromString(t, "10")},
				bson.M{"value": decimal128FromString(t, "-5.5")},
			},
		},
	}
	got := normaliseDecimal128(doc).(bson.M)
	profile := got["profile"].(bson.M)
	assert.Equal(t, "999.99", profile["balance"])
	hist := profile["history"].(bson.A)
	assert.Equal(t, "10", hist[0].(bson.M)["value"])
	assert.Equal(t, "-5.5", hist[1].(bson.M)["value"])
}

func TestNormaliseDecimal128PassesThroughNonDecimal(t *testing.T) {
	// Strings, ints, nils, etc. unchanged.
	for _, v := range []any{"hello", int32(42), int64(99), float64(3.14), nil, true, []byte{1, 2, 3}} {
		assert.Equal(t, v, normaliseDecimal128(v))
	}
}

func TestNormaliseDecimal128ScientificNotationCanonicalised(t *testing.T) {
	// Decimal128's String() can emit scientific notation for extreme magnitudes;
	// the walker normalises via ParseBigDecimal/FormatBigDecimal.
	d := decimal128FromString(t, "1.5e10")
	got := normaliseDecimal128(d)
	out, ok := got.(string)
	require.True(t, ok, "decimal128 must be replaced with a string")
	// Must not contain 'e' or 'E' — canonical form has no scientific notation.
	assert.NotContains(t, out, "e")
	assert.NotContains(t, out, "E")
}
