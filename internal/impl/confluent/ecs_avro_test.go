// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package confluent

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func TestEcsAvroFromBytesDecimalLogicalType(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Tx",
		"fields": [
			{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 4}}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 1)
	amount := c.Children[0]
	assert.Equal(t, "amount", amount.Name)
	assert.Equal(t, schema.Decimal, amount.Type)
	require.NotNil(t, amount.Logical)
	require.NotNil(t, amount.Logical.Decimal)
	assert.Equal(t, int32(18), amount.Logical.Decimal.Precision)
	assert.Equal(t, int32(4), amount.Logical.Decimal.Scale)
}

func TestEcsAvroFromBytesDecimalFixed(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Tx",
		"fields": [
			{"name": "amount", "type": {"type": "fixed", "name": "Dec", "size": 16, "logicalType": "decimal", "precision": 38, "scale": 8}}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	amount := c.Children[0]
	assert.Equal(t, schema.Decimal, amount.Type)
	require.NotNil(t, amount.Logical.Decimal)
	assert.Equal(t, int32(38), amount.Logical.Decimal.Precision)
	assert.Equal(t, int32(8), amount.Logical.Decimal.Scale)
}

func TestEcsAvroFromBytesDecimalOptionalUnion(t *testing.T) {
	// [null, {decimal}] under rawUnion=true should collapse to Optional
	// Decimal with logical params preserved — the standard "nullable
	// decimal" idiom in Avro.
	spec := []byte(`{
		"type": "record",
		"name": "Tx",
		"fields": [
			{"name": "amount", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	amount := c.Children[0]
	assert.Equal(t, schema.Decimal, amount.Type)
	assert.True(t, amount.Optional, "[null, decimal] should produce Optional Decimal")
	require.NotNil(t, amount.Logical)
	require.NotNil(t, amount.Logical.Decimal)
	assert.Equal(t, int32(9), amount.Logical.Decimal.Precision)
	assert.Equal(t, int32(2), amount.Logical.Decimal.Scale)
	assert.Equal(t, "amount", amount.Name, "outer field name should be preserved")
}

func TestEcsAvroFromBytesDecimalScaleDefaultsToZero(t *testing.T) {
	// Per the Avro spec, scale is optional and defaults to 0 when omitted.
	spec := []byte(`{
		"type": "record",
		"name": "Tx",
		"fields": [
			{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9}}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	amount := c.Children[0]
	assert.Equal(t, schema.Decimal, amount.Type)
	require.NotNil(t, amount.Logical)
	require.NotNil(t, amount.Logical.Decimal)
	assert.Equal(t, int32(9), amount.Logical.Decimal.Precision)
	assert.Equal(t, int32(0), amount.Logical.Decimal.Scale, "scale should default to 0 when absent")
}

func TestEcsAvroFromBytesDecimalOutOfBoundsRejected(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Tx",
		"fields": [
			{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 50, "scale": 2}}
		]
	}`)
	_, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.Error(t, err)
}

func TestNormaliseAvroDecimalsTopLevelRat(t *testing.T) {
	c := schema.Common{
		Type:    schema.Decimal,
		Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 18, Scale: 4}},
	}
	rat := big.NewRat(15, 10)
	got := normaliseAvroDecimals(rat, c)
	assert.Equal(t, "1.5000", got)
}

func TestNormaliseAvroDecimalsInsideObject(t *testing.T) {
	c := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{
				Name:    "amount",
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 9, Scale: 2}},
			},
			{Name: "name", Type: schema.String},
		},
	}
	value := map[string]any{
		"amount": big.NewRat(150, 1),
		"name":   "alice",
	}
	got := normaliseAvroDecimals(value, c)
	m := got.(map[string]any)
	assert.Equal(t, "150.00", m["amount"])
	assert.Equal(t, "alice", m["name"])
}

func TestNormaliseAvroDecimalsInsideArray(t *testing.T) {
	c := schema.Common{
		Type: schema.Array,
		Children: []schema.Common{{
			Type:    schema.Decimal,
			Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 5, Scale: 2}},
		}},
	}
	value := []any{big.NewRat(100, 100), big.NewRat(-50, 10)}
	got := normaliseAvroDecimals(value, c)
	arr := got.([]any)
	assert.Equal(t, "1.00", arr[0])
	assert.Equal(t, "-5.00", arr[1])
}

// TestNormaliseAvroDecimalsInUntaggedUnion exercises the Union dispatch on
// an unwrapped *big.Rat — what twmb/avro produces in the default decode mode.
func TestNormaliseAvroDecimalsInUntaggedUnion(t *testing.T) {
	c := schema.Common{
		Type: schema.Union,
		Children: []schema.Common{
			{Type: schema.Null},
			{
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 18, Scale: 4}},
			},
		},
	}
	got := normaliseAvroDecimals(big.NewRat(15, 10), c)
	assert.Equal(t, "1.5000", got, "Union must find the Decimal variant and convert")
}

// TestNormaliseAvroDecimalsInTaggedUnion exercises the Union dispatch on a
// {"<tag>": value} map — the shape twmb/avro produces under TaggedUnions.
// Without the recent Union-handling fix this test fails because the walker
// would treat the map as an Object and miss the conversion.
func TestNormaliseAvroDecimalsInTaggedUnion(t *testing.T) {
	c := schema.Common{
		Type: schema.Union,
		Children: []schema.Common{
			{Type: schema.Null},
			{
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 9, Scale: 2}},
			},
		},
	}
	tagged := map[string]any{"bytes": big.NewRat(150, 100)}
	got := normaliseAvroDecimals(tagged, c)
	m := got.(map[string]any)
	assert.Equal(t, "1.50", m["bytes"], "tagged union should unwrap, find Decimal variant, and convert in place")
}

func TestNormaliseAvroDecimalsLeavesNonDecimalRatAlone(t *testing.T) {
	// A Float64 field doesn't trigger conversion even if a *big.Rat happens
	// to be passed in. The walker only converts Decimal-typed paths.
	c := schema.Common{Type: schema.Float64}
	rat := big.NewRat(15, 10)
	got := normaliseAvroDecimals(rat, c)
	assert.Same(t, rat, got, "non-decimal field path must leave value untouched")
}

func TestNormaliseAvroDecimalsInexactRatReturnsOriginal(t *testing.T) {
	// A Rat that doesn't divide exactly at the declared scale (e.g. 1/3 at
	// scale 2) is left as-is so the caller can decide what to do.
	c := schema.Common{
		Type:    schema.Decimal,
		Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 5, Scale: 2}},
	}
	rat := big.NewRat(1, 3)
	got := normaliseAvroDecimals(rat, c)
	assert.Same(t, rat, got)
}

func TestDidConvertDecimal(t *testing.T) {
	rat := big.NewRat(15, 10)
	assert.True(t, didConvertDecimal(rat, "1.5"))
	assert.False(t, didConvertDecimal("1.5", "1.5"))
	assert.False(t, didConvertDecimal(rat, rat))
	assert.False(t, didConvertDecimal(map[string]any{}, map[string]any{}))
}

// TestEcsAvroRawUnionNestedRecord is a regression test for the rawUnion=true
// fallthrough bug: ecsAvroHydrateRawUnion sets *c to the inner record type
// (c.Type=Object) via ecsAvroIsUnionJustOptionalObject, then the bottom
// switch c.Type case was entered and tried to read as["fields"] from the outer
// field object rather than the inner record.
func TestEcsAvroRawUnionNestedRecord(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{"name": "ref", "type": "string"},
			{
				"name": "party",
				"type": ["null", {
					"type": "record",
					"name": "Party",
					"fields": [{"name": "id", "type": "string"}]
				}]
			}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 2)
	assert.Equal(t, "ref", c.Children[0].Name)
	assert.Equal(t, schema.String, c.Children[0].Type)
	party := c.Children[1]
	assert.Equal(t, "party", party.Name)
	assert.Equal(t, schema.Object, party.Type)
	assert.True(t, party.Optional)
	require.Len(t, party.Children, 1)
	assert.Equal(t, "id", party.Children[0].Name)
	assert.Equal(t, schema.String, party.Children[0].Type)
}

// TestEcsAvroRecordWithNilFields is a regression test for schemas where a
// field's type is a record object without a "fields" key (e.g. back-reference
// form {"type":"record","name":"Party"} or "fields":null from some generators).
// ecsAvroFromAnyMap must return an opaque Object rather than failing, so that
// schema metadata extraction succeeds for the rest of the record's fields.
func TestEcsAvroRecordWithNilFields(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{"name": "ref", "type": "string"},
			{"name": "party", "type": {"type": "record", "name": "Party"}}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 2)
	assert.Equal(t, "ref", c.Children[0].Name)
	party := c.Children[1]
	assert.Equal(t, "party", party.Name)
	assert.Equal(t, schema.Object, party.Type)
	assert.Empty(t, party.Children)
}
