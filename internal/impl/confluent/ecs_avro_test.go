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

// TestEcsAvroRawUnionNullableRecordByName is the CON-468 regression for
// nullable record unions where the non-null branch is a string name
// reference to a previously-defined record (rather than an inline
// definition). The Avro JSON spec requires named types to be defined once
// then referenced by name, so any non-trivial customer schema with reused
// records will exercise this path.
func TestEcsAvroRawUnionNullableRecordByName(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "Fee",
					"fields": [
						{"name": "amount", "type": "long"},
						{"name": "currency", "type": "string"}
					]
				}
			},
			{"name": "secondary_fee", "type": ["null", "Fee"], "default": null}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 2)

	primary := c.Children[0]
	assert.Equal(t, "primary_fee", primary.Name)
	require.Equal(t, schema.Object, primary.Type)
	require.Len(t, primary.Children, 2)

	secondary := c.Children[1]
	assert.Equal(t, "secondary_fee", secondary.Name)
	require.Equal(t, schema.Object, secondary.Type, "name reference to Fee should resolve to the same record structure, not VARCHAR")
	assert.True(t, secondary.Optional)
	require.Len(t, secondary.Children, 2)
	assert.Equal(t, "amount", secondary.Children[0].Name)
	assert.Equal(t, schema.Int64, secondary.Children[0].Type)
	assert.Equal(t, "currency", secondary.Children[1].Name)
	assert.Equal(t, schema.String, secondary.Children[1].Type)
}

// TestEcsAvroRawUnionNullableOrderIndependence covers CON-468 acceptance
// criterion 2: the [<type>, "null"] ordering (null second) must resolve
// identically to ["null", <type>] (null first), across inline objects,
// primitives, and name references.
func TestEcsAvroRawUnionNullableOrderIndependence(t *testing.T) {
	t.Run("inline record, null second", func(t *testing.T) {
		spec := []byte(`{
			"type": "record",
			"name": "Transfer",
			"fields": [{
				"name": "fee",
				"type": [{
					"type": "record",
					"name": "Fee",
					"fields": [{"name": "amount", "type": "long"}]
				}, "null"]
			}]
		}`)
		c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
		require.NoError(t, err)
		fee := c.Children[0]
		assert.Equal(t, schema.Object, fee.Type)
		assert.True(t, fee.Optional)
		require.Len(t, fee.Children, 1)
		assert.Equal(t, "amount", fee.Children[0].Name)
	})

	t.Run("primitive, null second", func(t *testing.T) {
		spec := []byte(`{
			"type": "record",
			"name": "Transfer",
			"fields": [{"name": "ref", "type": ["string", "null"]}]
		}`)
		c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
		require.NoError(t, err)
		ref := c.Children[0]
		assert.Equal(t, schema.String, ref.Type)
		assert.True(t, ref.Optional)
	})

	t.Run("name reference, null second", func(t *testing.T) {
		spec := []byte(`{
			"type": "record",
			"name": "Transfer",
			"fields": [
				{
					"name": "primary_fee",
					"type": {
						"type": "record",
						"name": "Fee",
						"fields": [{"name": "amount", "type": "long"}]
					}
				},
				{"name": "secondary_fee", "type": ["Fee", "null"]}
			]
		}`)
		c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
		require.NoError(t, err)
		secondary := c.Children[1]
		assert.Equal(t, schema.Object, secondary.Type)
		assert.True(t, secondary.Optional)
		require.Len(t, secondary.Children, 1)
		assert.Equal(t, "amount", secondary.Children[0].Name)
	})
}

// TestEcsAvroRawUnionNullableRecordNamespaced verifies that namespaced
// record names can be referenced either by short name (Fee) or by fully-
// qualified name (com.example.Fee), matching the Avro spec's name
// resolution rules.
func TestEcsAvroRawUnionNullableRecordNamespaced(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"namespace": "com.example",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "Fee",
					"namespace": "com.example",
					"fields": [{"name": "amount", "type": "long"}]
				}
			},
			{"name": "by_short_name", "type": ["null", "Fee"]},
			{"name": "by_full_name", "type": ["null", "com.example.Fee"]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 3)

	short := c.Children[1]
	assert.Equal(t, "by_short_name", short.Name)
	assert.Equal(t, schema.Object, short.Type)
	require.Len(t, short.Children, 1)

	full := c.Children[2]
	assert.Equal(t, "by_full_name", full.Name)
	assert.Equal(t, schema.Object, full.Type)
	require.Len(t, full.Children, 1)
}

// TestEcsAvroRawUnionNullableRecordNested covers CON-468 acceptance
// criterion 2's "record-with-nested-record" case: a nullable record union
// whose record contains its own nullable record union, both as inline
// definitions and as name references at the inner level.
func TestEcsAvroRawUnionNullableRecordNested(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{
				"name": "inner_template",
				"type": {
					"type": "record",
					"name": "Inner",
					"fields": [{"name": "code", "type": "string"}]
				}
			},
			{
				"name": "outer",
				"type": ["null", {
					"type": "record",
					"name": "Outer",
					"fields": [
						{"name": "label", "type": "string"},
						{"name": "inner", "type": ["null", "Inner"]}
					]
				}]
			}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 2)

	outer := c.Children[1]
	assert.Equal(t, "outer", outer.Name)
	assert.Equal(t, schema.Object, outer.Type)
	assert.True(t, outer.Optional)
	require.Len(t, outer.Children, 2)

	inner := outer.Children[1]
	assert.Equal(t, "inner", inner.Name)
	assert.Equal(t, schema.Object, inner.Type, "nested name reference must resolve, not collapse to VARCHAR")
	assert.True(t, inner.Optional)
	require.Len(t, inner.Children, 1)
	assert.Equal(t, "code", inner.Children[0].Name)
}

// TestEcsAvroLameUnionNameResolution covers the same CON-468 bug class in
// the lame-union (rawUnion=false) path that the schema_registry_decode
// processor takes by default. The lame envelope wraps each branch in a
// tagged Object so the wire shape stays the same, but the inner Common
// must still expand previously-defined named-type references rather than
// collapsing them to schema.Any.
func TestEcsAvroLameUnionNameResolution(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "Fee",
					"fields": [{"name": "amount", "type": "long"}]
				}
			},
			{"name": "secondary_fee", "type": ["null", "Fee"]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)

	secondary := c.Children[1]
	assert.Equal(t, "secondary_fee", secondary.Name)
	assert.Equal(t, schema.Union, secondary.Type)
	require.Len(t, secondary.Children, 2)

	// One child is the null branch (Common.Type=Null, Name="").
	// The other is the tagged-Object envelope wrapping the resolved Fee.
	var feeEnvelope schema.Common
	for _, ch := range secondary.Children {
		if ch.Type == schema.Object {
			feeEnvelope = ch
			break
		}
	}
	require.Equal(t, schema.Object, feeEnvelope.Type, "Fee branch should be tagged-Object envelope")
	require.Len(t, feeEnvelope.Children, 1)
	feeInner := feeEnvelope.Children[0]
	assert.Equal(t, "Fee", feeInner.Name)
	assert.Equal(t, schema.Object, feeInner.Type, "name reference to Fee should resolve to its record structure, not schema.Any")
	require.Len(t, feeInner.Children, 1)
	assert.Equal(t, "amount", feeInner.Children[0].Name)
	assert.Equal(t, schema.Int64, feeInner.Children[0].Type)
}

// TestEcsAvroUnionInlineErrorPropagation pins down the %w-wrapping
// contract through the union resolvers. A malformed inline decimal sitting
// inside a nullable union must surface its root-cause error (the precision
// parse failure), not collapse to a generic "could not resolve type
// map[string]interface{}". Covers both the optional-union fast path and the
// general union fall-through, and both raw- and lame-union flavours.
func TestEcsAvroUnionInlineErrorPropagation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ecsAvroConfig
		spec    string
		wantMsg string
	}{
		{
			name: "raw union, nullable shape, malformed decimal precision",
			cfg:  ecsAvroConfig{rawUnion: true},
			spec: `{
				"type": "record",
				"name": "Tx",
				"fields": [{
					"name": "amount",
					"type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": "not-a-number", "scale": 2}]
				}]
			}`,
			wantMsg: "decimal precision",
		},
		{
			name: "raw union, three-branch shape, malformed decimal scale",
			cfg:  ecsAvroConfig{rawUnion: true},
			spec: `{
				"type": "record",
				"name": "Tx",
				"fields": [{
					"name": "amount",
					"type": ["null", "string", {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": "nope"}]
				}]
			}`,
			wantMsg: "decimal scale",
		},
		{
			name: "lame union, nullable shape, malformed decimal precision",
			cfg:  ecsAvroConfig{},
			spec: `{
				"type": "record",
				"name": "Tx",
				"fields": [{
					"name": "amount",
					"type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": "not-a-number", "scale": 2}]
				}]
			}`,
			wantMsg: "decimal precision",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ecsAvroParseFromBytes(tt.cfg, []byte(tt.spec))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg, "inner error context must propagate via %%w wrapping, got %q", err.Error())
			assert.NotContains(t, err.Error(), "could not resolve type", "must not collapse to the type-stringifier fallback when an inner error exists")
		})
	}
}

// TestEcsAvroNamespaceInheritance verifies that named types defined inside
// a namespaced record inherit the enclosing namespace when they omit their
// own `namespace` field, per the Avro spec's name-resolution rules. Both
// short-name and fully-qualified references to the inherited fullname must
// resolve, including across deeply nested scopes.
func TestEcsAvroNamespaceInheritance(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"namespace": "com.example",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "Fee",
					"fields": [{"name": "amount", "type": "long"}]
				}
			},
			{"name": "by_short_name", "type": ["null", "Fee"]},
			{"name": "by_inherited_full_name", "type": ["null", "com.example.Fee"]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 3)

	short := c.Children[1]
	assert.Equal(t, "by_short_name", short.Name)
	assert.Equal(t, schema.Object, short.Type, "unqualified Fee should resolve via inherited com.example namespace")
	require.Len(t, short.Children, 1)
	assert.Equal(t, "amount", short.Children[0].Name)

	full := c.Children[2]
	assert.Equal(t, "by_inherited_full_name", full.Name)
	assert.Equal(t, schema.Object, full.Type, "Fee implicitly belongs to com.example and must be reachable by FQN")
	require.Len(t, full.Children, 1)
}

// TestEcsAvroNameWithEmbeddedDot exercises the third form of Avro's named-
// type spelling: the `name` field itself contains a dot, in which case the
// dot-bearing value IS the fullname and any sibling `namespace` field is
// ignored. References must resolve under the embedded fullname.
func TestEcsAvroNameWithEmbeddedDot(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"namespace": "ignored.outer",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "com.example.Fee",
					"namespace": "this.is.ignored",
					"fields": [{"name": "amount", "type": "long"}]
				}
			},
			{"name": "by_fqn", "type": ["null", "com.example.Fee"]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 2)

	fqn := c.Children[1]
	assert.Equal(t, "by_fqn", fqn.Name)
	assert.Equal(t, schema.Object, fqn.Type)
	require.Len(t, fqn.Children, 1)
}

// TestEcsAvroSelfReferentialRecord verifies that a record whose own field
// references it by name (e.g. a linked-list `next` pointer) resolves to a
// structural stub — a one-level Object carrying the record's short name —
// rather than collapsing to schema.Any (VARCHAR downstream).
//
// True recursive resolution is out of scope: schema.Common is a value type
// with no back-reference machinery, so the self-reference is necessarily a
// stub. The fix exists so downstream sinks see "this is some kind of
// record" rather than an opaque blob.
func TestEcsAvroSelfReferentialRecord(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Node",
		"fields": [
			{"name": "value", "type": "long"},
			{"name": "next", "type": ["null", "Node"]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 2)
	assert.Equal(t, "value", c.Children[0].Name)
	assert.Equal(t, schema.Int64, c.Children[0].Type)

	next := c.Children[1]
	assert.Equal(t, "next", next.Name)
	assert.Equal(t, schema.Object, next.Type, "self-reference should resolve to Object stub, not schema.Any")
	assert.True(t, next.Optional)
	assert.Empty(t, next.Children, "self-reference is a one-level stub — recursive resolution is out of scope")
}

// TestEcsAvroNamesMapIsolation verifies the clone-on-retrieval contract for
// the names map: mutating a resolved Common (e.g. appending to its Children)
// must not propagate to subsequent lookups of the same named type. Without
// the clone, the second `secondary_fee` resolution would see the mutated
// shape of the first.
func TestEcsAvroNamesMapIsolation(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Transfer",
		"fields": [
			{
				"name": "primary_fee",
				"type": {
					"type": "record",
					"name": "Fee",
					"fields": [{"name": "amount", "type": "long"}]
				}
			},
			{"name": "first_ref", "type": "Fee"},
			{"name": "second_ref", "type": "Fee"}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 3)

	first := c.Children[1]
	require.Equal(t, schema.Object, first.Type)
	require.Len(t, first.Children, 1)
	// Mutate the first resolved copy. If the names map were aliased, the
	// next retrieval would see a record with two children.
	first.Children = append(first.Children, schema.Common{Name: "poison", Type: schema.String})
	assert.Len(t, first.Children, 2)

	second := c.Children[2]
	require.Equal(t, schema.Object, second.Type)
	assert.Len(t, second.Children, 1, "later retrieval of Fee must not see mutations applied to earlier resolutions")
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

// TestEcsAvroLogicalTypeDispatcher exercises the full dispatcher in
// applyAvroLogicalType for every Avro logical type the connector now
// preserves. Each case drives the original-issue-#4399 path: the schema's
// logicalType annotation is honoured rather than dropped.
func TestEcsAvroLogicalTypeDispatcher(t *testing.T) {
	type expect struct {
		ctype       schema.CommonType
		hasLogical  bool
		unit        schema.TimeUnit
		adjustToUTC bool
	}
	cases := []struct {
		name   string
		field  string // raw JSON for the field's "type" entry
		expect expect
	}{
		{
			name:   "timestamp-millis",
			field:  `{"type":"long","logicalType":"timestamp-millis"}`,
			expect: expect{ctype: schema.Timestamp, hasLogical: true, unit: schema.TimeUnitMillis, adjustToUTC: true},
		},
		{
			name:   "timestamp-micros",
			field:  `{"type":"long","logicalType":"timestamp-micros"}`,
			expect: expect{ctype: schema.Timestamp, hasLogical: true, unit: schema.TimeUnitMicros, adjustToUTC: true},
		},
		{
			name:   "timestamp-nanos",
			field:  `{"type":"long","logicalType":"timestamp-nanos"}`,
			expect: expect{ctype: schema.Timestamp, hasLogical: true, unit: schema.TimeUnitNanos, adjustToUTC: true},
		},
		{
			name:   "local-timestamp-millis",
			field:  `{"type":"long","logicalType":"local-timestamp-millis"}`,
			expect: expect{ctype: schema.Timestamp, hasLogical: true, unit: schema.TimeUnitMillis, adjustToUTC: false},
		},
		{
			name:   "local-timestamp-micros",
			field:  `{"type":"long","logicalType":"local-timestamp-micros"}`,
			expect: expect{ctype: schema.Timestamp, hasLogical: true, unit: schema.TimeUnitMicros, adjustToUTC: false},
		},
		{
			name:   "date",
			field:  `{"type":"int","logicalType":"date"}`,
			expect: expect{ctype: schema.Date},
		},
		{
			name:   "time-millis",
			field:  `{"type":"int","logicalType":"time-millis"}`,
			expect: expect{ctype: schema.TimeOfDay, hasLogical: true, unit: schema.TimeUnitMillis},
		},
		{
			name:   "time-micros",
			field:  `{"type":"long","logicalType":"time-micros"}`,
			expect: expect{ctype: schema.TimeOfDay, hasLogical: true, unit: schema.TimeUnitMicros},
		},
		{
			name:   "uuid",
			field:  `{"type":"string","logicalType":"uuid"}`,
			expect: expect{ctype: schema.UUID},
		},
		{
			name: "unknown logicalType falls back to base primitive",
			// Per Avro 1.10 spec readers must ignore unknown logicalType
			// values. The base primitive (long) survives.
			field:  `{"type":"long","logicalType":"frobnicate-millis"}`,
			expect: expect{ctype: schema.Int64},
		},
		{
			name: "logicalType on mismatched primitive falls back",
			// timestamp-millis declared on `string` is malformed; reader
			// silently uses the base primitive instead of failing the schema.
			field:  `{"type":"string","logicalType":"timestamp-millis"}`,
			expect: expect{ctype: schema.String},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := []byte(`{
				"type": "record",
				"name": "Row",
				"fields": [
					{"name": "field", "type": ` + tc.field + `}
				]
			}`)
			c, err := ecsAvroParseFromBytes(ecsAvroConfig{preserveLogicalTypes: true}, spec)
			require.NoError(t, err)
			require.Len(t, c.Children, 1)
			f := c.Children[0]
			assert.Equal(t, tc.expect.ctype, f.Type, "type mapping")
			if tc.expect.hasLogical {
				require.NotNil(t, f.Logical, "Logical should be populated")
				switch tc.expect.ctype {
				case schema.Timestamp:
					require.NotNil(t, f.Logical.Timestamp)
					assert.Equal(t, tc.expect.unit, f.Logical.Timestamp.Unit)
					assert.Equal(t, tc.expect.adjustToUTC, f.Logical.Timestamp.AdjustToUTC)
				case schema.TimeOfDay:
					require.NotNil(t, f.Logical.TimeOfDay)
					assert.Equal(t, tc.expect.unit, f.Logical.TimeOfDay.Unit)
					assert.Equal(t, tc.expect.adjustToUTC, f.Logical.TimeOfDay.AdjustToUTC)
				}
			}
		})
	}
}

// TestEcsAvroDecimalOnWrongPrimitiveFallsBack verifies that a `decimal`
// logical-type annotation on something other than `bytes` or `fixed` is
// silently dropped per the spec, rather than producing a malformed
// schema.Decimal whose precision/scale claims to govern an int64 value.
func TestEcsAvroDecimalOnWrongPrimitiveFallsBack(t *testing.T) {
	spec := []byte(`{
		"type":"record","name":"Row",
		"fields":[
			{"name":"amount","type":{"type":"long","logicalType":"decimal","precision":18,"scale":4}}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	amount := c.Children[0]
	assert.Equal(t, schema.Int64, amount.Type, "decimal on long should fall back to base type")
	assert.Nil(t, amount.Logical, "no Logical params should be set when annotation falls back")
}

// TestEcsAvroLogicalTypeOptionalUnion verifies that the [null, {logical}]
// idiom under rawUnion=true unwraps to an Optional node with the logical
// type fully preserved. This is the shape the original issue #4399 reported.
func TestEcsAvroLogicalTypeOptionalUnion(t *testing.T) {
	spec := []byte(`{
		"type": "record",
		"name": "Row",
		"fields": [
			{"name": "event_time", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}
		]
	}`)
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true, preserveLogicalTypes: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	f := c.Children[0]
	assert.Equal(t, "event_time", f.Name)
	assert.Equal(t, schema.Timestamp, f.Type)
	assert.True(t, f.Optional, "[null, timestamp-millis] should produce Optional Timestamp")
	require.NotNil(t, f.Logical)
	require.NotNil(t, f.Logical.Timestamp)
	assert.Equal(t, schema.TimeUnitMillis, f.Logical.Timestamp.Unit)
	assert.True(t, f.Logical.Timestamp.AdjustToUTC)
}
