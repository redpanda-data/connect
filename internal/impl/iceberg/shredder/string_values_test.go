// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"math"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStringValue(t *testing.T) {
	cases := []struct {
		name    string
		typ     iceberg.Type
		in      any
		layout  string
		want    any
		wantErr string
	}{
		{"long from string", iceberg.PrimitiveTypes.Int64, "123", "", int64(123), ""},
		{"long beyond 2^53 exact", iceberg.PrimitiveTypes.Int64, "9007199254740993", "", int64(9007199254740993), ""},
		{"int from negative", iceberg.PrimitiveTypes.Int32, "-7", "", int64(-7), ""},
		{"long empty string", iceberg.PrimitiveTypes.Int64, "", "", nil, `cannot parse "" as long`},
		{"long padded", iceberg.PrimitiveTypes.Int64, " 12", "", nil, `cannot parse " 12" as long`},
		{"long fractional", iceberg.PrimitiveTypes.Int64, "1.0", "", nil, `cannot parse "1.0" as long`},
		{"double from string", iceberg.PrimitiveTypes.Float64, "45.67", "", 45.67, ""},
		{"float from string", iceberg.PrimitiveTypes.Float32, "1.5", "", 1.5, ""},
		{"double NaN accepted", iceberg.PrimitiveTypes.Float64, "NaN", "", math.NaN(), ""},
		{"timestamp default layout", iceberg.PrimitiveTypes.Timestamp, "2025-05-29T11:54:26.721451319Z", "", time.Date(2025, 5, 29, 11, 54, 26, 721451319, time.UTC), ""},
		{"timestamp custom layout", iceberg.PrimitiveTypes.Timestamp, "2025-05-29 11:54:26.721451319", "2006-01-02 15:04:05.999999999", time.Date(2025, 5, 29, 11, 54, 26, 721451319, time.UTC), ""},
		{"timestamptz zone-less is UTC", iceberg.PrimitiveTypes.TimestampTz, "2025-05-29 11:54:26", "2006-01-02 15:04:05", time.Date(2025, 5, 29, 11, 54, 26, 0, time.UTC), ""},
		{"timestamp wrong layout", iceberg.PrimitiveTypes.Timestamp, "2025-05-29 11:54:26", "", nil, `with timestamp_format "2006-01-02T15:04:05.999999999Z07:00"`},
		{"long from bytes", iceberg.PrimitiveTypes.Int64, []byte("42"), "", int64(42), ""},
		{"long from non-numeric bytes", iceberg.PrimitiveTypes.Int64, []byte("x"), "", nil, `cannot parse "x" as long`},
		{"double from bytes", iceberg.PrimitiveTypes.Float64, []byte("45.67"), "", 45.67, ""},
		{"timestamp from bytes custom layout", iceberg.PrimitiveTypes.Timestamp, []byte("2025-05-29 11:54:26.721451319"), "2006-01-02 15:04:05.999999999", time.Date(2025, 5, 29, 11, 54, 26, 721451319, time.UTC), ""},
		{"decimal bytes untouched", iceberg.DecimalTypeOf(18, 2), []byte("12345.67"), "", []byte("12345.67"), ""},
		{"binary bytes untouched", iceberg.PrimitiveTypes.Binary, []byte("42"), "", []byte("42"), ""},
		{"decimal untouched", iceberg.DecimalTypeOf(10, 2), "1.50", "", "1.50", ""},
		{"string column untouched", iceberg.PrimitiveTypes.String, "123", "", "123", ""},
		{"non-string untouched", iceberg.PrimitiveTypes.Int64, int64(5), "", int64(5), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseStringValue(tc.in, tc.typ, tc.layout)
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			if f, isFloat := tc.want.(float64); isFloat && math.IsNaN(f) {
				assert.True(t, math.IsNaN(got.(float64)))
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestShredderParsesNumericAndTimestampStrings(t *testing.T) {
	schema := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "l", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "d", Type: iceberg.PrimitiveTypes.Float64},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 4, Name: "lst", Type: &iceberg.ListType{
			ElementID: 5,
			Element:   iceberg.PrimitiveTypes.Int64,
		}},
		iceberg.NestedField{ID: 6, Name: "m", Type: &iceberg.MapType{
			KeyID:     7,
			KeyType:   iceberg.PrimitiveTypes.Int32,
			ValueID:   8,
			ValueType: iceberg.PrimitiveTypes.String,
		}},
	)

	rs := NewRecordShredder(schema, true)
	rs.TimestampLayout = "2006-01-02 15:04:05.999999999"
	sink := &testSink{}
	require.NoError(t, rs.Shred(map[string]any{
		"l":   "42",
		"d":   "2.5",
		"ts":  "2025-05-29 11:54:26.721451319",
		"lst": []any{"1", "2"},
		"m":   map[string]any{"7": "x"},
	}, sink))

	byID := map[int][]ShreddedValue{}
	for _, v := range sink.values {
		byID[v.FieldID] = append(byID[v.FieldID], v)
	}
	require.Len(t, byID[1], 1)
	assert.Equal(t, int64(42), byID[1][0].Value.Int64())
	require.Len(t, byID[2], 1)
	assert.Equal(t, 2.5, byID[2][0].Value.Double())
	require.Len(t, byID[3], 1)
	assert.Equal(t, int64(1748519666721451), byID[3][0].Value.Int64())
	require.Len(t, byID[5], 2)
	assert.Equal(t, int64(1), byID[5][0].Value.Int64())
	assert.Equal(t, int64(2), byID[5][1].Value.Int64())
	require.Len(t, byID[7], 1)
	assert.Equal(t, int32(7), byID[7][0].Value.Int32())
}
