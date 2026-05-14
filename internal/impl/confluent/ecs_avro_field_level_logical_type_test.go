// Copyright 2026 Redpanda Data, Inc.

package confluent

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// TestEcsAvroFieldLevelLogicalType verifies that ecsAvroParseFromBytes
// honours a `logicalType` annotation regardless of where it sits relative
// to its `type` declaration.
//
// Two idiomatic shapes appear in the wild for a nullable logically-typed
// field:
//
//  1. logicalType nested inside the union's object element (spec-blessed):
//
//     {
//     "name": "ts",
//     "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]
//     }
//
//  2. logicalType as a sibling of `type` at the field level (Java/JDBC idiom):
//
//     {
//     "name": "ts",
//     "type": ["null", "long"],
//     "logicalType": "timestamp-millis"
//     }
//
// Both must resolve to the same Common. The value-side decoder honours
// both, so the schema-metadata side must agree — otherwise downstream
// consumers (e.g. the iceberg output) pick column types from the metadata
// that mismatch what the decoded values actually are.
func TestEcsAvroFieldLevelLogicalType(t *testing.T) {
	type expect struct {
		typ       schema.CommonType
		unit      schema.TimeUnit // for timestamp/time-of-day
		adjustUTC bool            // for timestamp/time-of-day
		precision int32           // for decimal
		scale     int32           // for decimal
	}

	cases := []struct {
		name      string
		baseType  string // the second element of the union when in sibling form
		logical   string
		precision int32
		scale     int32
		want      expect
	}{
		{
			name: "timestamp-millis", baseType: "long", logical: "timestamp-millis",
			want: expect{typ: schema.Timestamp, unit: schema.TimeUnitMillis, adjustUTC: true},
		},
		{
			name: "timestamp-micros", baseType: "long", logical: "timestamp-micros",
			want: expect{typ: schema.Timestamp, unit: schema.TimeUnitMicros, adjustUTC: true},
		},
		{
			name: "local-timestamp-millis", baseType: "long", logical: "local-timestamp-millis",
			want: expect{typ: schema.Timestamp, unit: schema.TimeUnitMillis, adjustUTC: false},
		},
		{
			name: "date", baseType: "int", logical: "date",
			want: expect{typ: schema.Date},
		},
		{
			name: "time-millis", baseType: "int", logical: "time-millis",
			want: expect{typ: schema.TimeOfDay, unit: schema.TimeUnitMillis},
		},
		{
			name: "time-micros", baseType: "long", logical: "time-micros",
			want: expect{typ: schema.TimeOfDay, unit: schema.TimeUnitMicros},
		},
		{
			name: "uuid", baseType: "string", logical: "uuid",
			want: expect{typ: schema.UUID},
		},
		{
			name: "decimal", baseType: "bytes", logical: "decimal", precision: 38, scale: 2,
			want: expect{typ: schema.Decimal, precision: 38, scale: 2},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("nested-in-type", func(t *testing.T) {
				inner := fmt.Sprintf(`{"type":"%s","logicalType":"%s"`, tc.baseType, tc.logical)
				if tc.logical == "decimal" {
					inner += fmt.Sprintf(`,"precision":%d,"scale":%d`, tc.precision, tc.scale)
				}
				inner += "}"
				spec := fmt.Appendf(nil, `{
					"type": "record", "name": "Row",
					"fields": [{"name": "v", "type": ["null", %s]}]
				}`, inner)
				assertLogicalField(t, spec, tc.want)
			})

			t.Run("sibling-of-type", func(t *testing.T) {
				extras := ""
				if tc.logical == "decimal" {
					extras = fmt.Sprintf(`,"precision":%d,"scale":%d`, tc.precision, tc.scale)
				}
				spec := fmt.Appendf(nil, `{
					"type": "record", "name": "Row",
					"fields": [{"name": "v", "type": ["null", "%s"], "logicalType": "%s"%s}]
				}`, tc.baseType, tc.logical, extras)
				assertLogicalField(t, spec, tc.want)
			})
		})
	}
}

// TestEcsAvroEncoderDecoderRoundTrip verifies that the schema we emit via
// commonToAvroNode is correctly parsed back by ecsAvroParseFromBytes — i.e.
// the encoder and decoder agree on at least one canonical wire form.
func TestEcsAvroEncoderDecoderRoundTrip(t *testing.T) {
	original := schema.Common{
		Type: schema.Object, Name: "Row",
		Children: []schema.Common{
			{
				Name: "ts", Optional: true, Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: true},
				},
			},
			{
				Name: "amount", Optional: true, Type: schema.Decimal,
				Logical: &schema.LogicalParams{
					Decimal: &schema.DecimalParams{Precision: 38, Scale: 4},
				},
			},
		},
	}

	avroNode, err := commonToAvroNode(original, "Row", "", true)
	require.NoError(t, err)
	avroJSON, err := json.Marshal(avroNode)
	require.NoError(t, err)
	t.Logf("encoder emitted: %s", string(avroJSON))

	roundTripped, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, avroJSON)
	require.NoError(t, err)

	require.Len(t, roundTripped.Children, 2)

	ts := roundTripped.Children[0]
	assert.Equal(t, "ts", ts.Name)
	assert.Equal(t, schema.Timestamp, ts.Type)
	assert.True(t, ts.Optional)
	require.NotNil(t, ts.Logical)
	require.NotNil(t, ts.Logical.Timestamp)
	assert.Equal(t, schema.TimeUnitMillis, ts.Logical.Timestamp.Unit)
	assert.True(t, ts.Logical.Timestamp.AdjustToUTC)

	amt := roundTripped.Children[1]
	assert.Equal(t, "amount", amt.Name)
	assert.Equal(t, schema.Decimal, amt.Type)
	assert.True(t, amt.Optional)
	require.NotNil(t, amt.Logical)
	require.NotNil(t, amt.Logical.Decimal)
	assert.EqualValues(t, 38, amt.Logical.Decimal.Precision)
	assert.EqualValues(t, 4, amt.Logical.Decimal.Scale)
}

func assertLogicalField(t *testing.T, spec []byte, want struct {
	typ       schema.CommonType
	unit      schema.TimeUnit
	adjustUTC bool
	precision int32
	scale     int32
},
) {
	t.Helper()
	c, err := ecsAvroParseFromBytes(ecsAvroConfig{rawUnion: true}, spec)
	require.NoError(t, err)
	require.Len(t, c.Children, 1)
	f := c.Children[0]
	t.Logf("result: %s", mustJSON(c.ToAny()))
	assert.Equal(t, want.typ, f.Type)
	assert.True(t, f.Optional, "optional should be propagated")

	switch want.typ {
	case schema.Timestamp:
		require.NotNil(t, f.Logical)
		require.NotNil(t, f.Logical.Timestamp)
		assert.Equal(t, want.unit, f.Logical.Timestamp.Unit)
		assert.Equal(t, want.adjustUTC, f.Logical.Timestamp.AdjustToUTC)
	case schema.TimeOfDay:
		require.NotNil(t, f.Logical)
		require.NotNil(t, f.Logical.TimeOfDay)
		assert.Equal(t, want.unit, f.Logical.TimeOfDay.Unit)
	case schema.Decimal:
		require.NotNil(t, f.Logical)
		require.NotNil(t, f.Logical.Decimal)
		assert.Equal(t, want.precision, f.Logical.Decimal.Precision)
		assert.Equal(t, want.scale, f.Logical.Decimal.Scale)
	}
}

func mustJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}

// TestUpstreamTwmbHonoursSiblingFormLogicalType is a regression guard
// for the upstream twmb/avro dependency. After the bump that landed our
// own field-level logicalType fix upstream (twmb/avro PR #38), the
// value-side decoder now produces time.Time for sibling-form schemas
// natively — previously it returned int64 because the parser silently
// dropped the field-level annotation.
//
// If twmb ever regresses on this, the shredder's metadata-driven
// numeric scaling path in iceberg/shredder/temporal.go would become
// load-bearing again. Pinning the upstream behaviour here makes any
// such regression surface immediately, in the package that depends on
// it, rather than as a customer report.
func TestUpstreamTwmbHonoursSiblingFormLogicalType(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{
			"primitive timestamp-millis",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":"long","logicalType":"timestamp-millis"}
			]}`,
		},
		{
			"union timestamp-millis (null first)",
			`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"],"logicalType":"timestamp-millis"}
			]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := avro.Parse(tc.schema)
			require.NoError(t, err)

			// Encode a value via the binary-shape side schema to avoid
			// requiring time.Time on the encode path; this is the same
			// trick our integration test uses, and what a Java/JDBC
			// producer would do — write raw long bytes on the wire and
			// rely on the schema's logicalType for interpretation.
			bin := avro.MustParse(`{"type":"record","name":"R","fields":[
				{"name":"ts","type":["null","long"]}
			]}`)
			tsMillis := int64(1700000000000)
			payload, err := bin.Encode(&struct {
				TS *int64 `avro:"ts"`
			}{TS: &tsMillis})
			require.NoError(t, err)

			var native any
			_, err = s.Decode(payload, &native)
			require.NoError(t, err)

			row, ok := native.(map[string]any)
			require.True(t, ok, "expected map, got %T", native)
			_, isTime := row["ts"].(time.Time)
			assert.True(t, isTime,
				"upstream twmb must decode sibling-form timestamp-millis as time.Time; got %T (regression in twmb/avro PR #38)", row["ts"])
		})
	}
}
