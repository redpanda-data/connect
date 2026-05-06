// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// TestConvertTimestampSchemaAware exercises the unit-aware numeric path that
// closes the silent-corruption case from issue #4399. Without schema
// metadata, bloblang.ValueAsTimestamp interprets a numeric input as Unix
// seconds — a millisecond input therefore lands ~50,000 years in the
// future. With schema metadata declaring `timestamp-millis`, the same
// numeric input lands at the correct microsecond offset.
func TestConvertTimestampSchemaAware(t *testing.T) {
	const epochMillis = int64(1_730_000_000_000) // 2024-10-27 03:33:20 UTC
	const wantMicros = epochMillis * 1000

	cases := []struct {
		name    string
		value   any
		common  *schema.Common
		typ     iceberg.Type
		wantOK  bool
		wantVal int64
		exactly bool // assert exact equality on wantVal
	}{
		{
			name:    "millis with schema metadata → correct micros",
			value:   epochMillis,
			common:  tsCommon(schema.TimeUnitMillis, true),
			typ:     iceberg.TimestampTzType{},
			wantOK:  true,
			wantVal: wantMicros,
			exactly: true,
		},
		{
			name:    "micros with schema metadata → identity micros",
			value:   wantMicros,
			common:  tsCommon(schema.TimeUnitMicros, true),
			typ:     iceberg.TimestampTzType{},
			wantOK:  true,
			wantVal: wantMicros,
			exactly: true,
		},
		{
			name:    "nanos with schema metadata → micros (truncated)",
			value:   wantMicros * 1000,
			common:  tsCommon(schema.TimeUnitNanos, true),
			typ:     iceberg.TimestampTzType{},
			wantOK:  true,
			wantVal: wantMicros,
			exactly: true,
		},
		{
			name:    "time.Time always wins over numeric path",
			value:   time.UnixMilli(epochMillis).UTC(),
			common:  nil, // even without schema metadata, time.Time is unambiguous
			typ:     iceberg.TimestampTzType{},
			wantOK:  true,
			wantVal: wantMicros,
			exactly: true,
		},
		{
			name:    "numeric without schema falls back to bloblang seconds default",
			value:   epochMillis,               // user intended millis
			common:  nil,                       // but no schema to disambiguate
			typ:     iceberg.TimestampTzType{}, // → bloblang treats as seconds
			wantOK:  true,
			wantVal: epochMillis * 1_000_000, // year 56755, but loud at least via comparison
			exactly: false,                   // we don't assert exact correctness here, just existence
		},
		{
			name:    "TIMESTAMP_NS with millis metadata scales to nanos",
			value:   epochMillis,
			common:  tsCommon(schema.TimeUnitMillis, true),
			typ:     iceberg.TimestampTzNsType{},
			wantOK:  true,
			wantVal: epochMillis * 1_000_000,
			exactly: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := convertLeafValue(tc.value, tc.typ, tc.common)
			if !tc.wantOK {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.exactly {
				assert.Equal(t, tc.wantVal, pq.Int64())
			}
		})
	}
}

func TestConvertDateSchemaAware(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		want    int32
		wantErr bool
	}{
		{
			name:  "time.Time at UTC midnight",
			value: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			want:  19737, // days since epoch
		},
		{
			name:  "int32 days passes through",
			value: int32(19737),
			want:  19737,
		},
		{
			name:  "int days passes through",
			value: 19737,
			want:  19737,
		},
		{
			name:    "string rejected",
			value:   "2024-01-15",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := convertLeafValue(tc.value, iceberg.DateType{}, nil)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, pq.Int32())
		})
	}
}

func TestConvertTimeOfDaySchemaAware(t *testing.T) {
	const eightThirtyMicros = int64(8*3600+30*60) * 1_000_000
	cases := []struct {
		name   string
		value  any
		common *schema.Common
		want   int64
	}{
		{
			name:   "time.Duration passes through directly",
			value:  8*time.Hour + 30*time.Minute,
			common: nil,
			want:   eightThirtyMicros,
		},
		{
			name:   "millis numeric with schema metadata scales correctly",
			value:  int64(8*3600+30*60) * 1_000,
			common: todCommon(schema.TimeUnitMillis),
			want:   eightThirtyMicros,
		},
		{
			name:   "micros numeric with schema metadata is identity",
			value:  eightThirtyMicros,
			common: todCommon(schema.TimeUnitMicros),
			want:   eightThirtyMicros,
		},
		{
			name:   "nanos numeric with schema metadata divides correctly",
			value:  eightThirtyMicros * 1_000,
			common: todCommon(schema.TimeUnitNanos),
			want:   eightThirtyMicros,
		},
		{
			name:   "numeric without schema treated as already-micros (legacy)",
			value:  eightThirtyMicros,
			common: nil,
			want:   eightThirtyMicros,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := convertLeafValue(tc.value, iceberg.TimeType{}, tc.common)
			require.NoError(t, err)
			assert.Equal(t, tc.want, pq.Int64())
		})
	}
}

// TestConvertDatePreEpoch covers the floor-vs-truncate bug for negative
// Unix timestamps. 1969-12-31 23:59:59 UTC has Unix() == -1; truncating
// integer division gives 0 (Jan 1 1970) instead of the correct -1
// (Dec 31 1969). Iceberg DATE supports negative day indices.
func TestConvertDatePreEpoch(t *testing.T) {
	cases := []struct {
		name string
		t    time.Time
		want int32
	}{
		{"1969-12-31 23:59:59 UTC", time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC), -1},
		{"1969-12-31 00:00:00 UTC", time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC), -1},
		{"1969-01-01 UTC", time.Date(1969, 1, 1, 0, 0, 0, 0, time.UTC), -365},
		{"1900-01-01 UTC", time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), -25567},
		{"1970-01-01 00:00:00 UTC (boundary)", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), 0},
		{"1970-01-01 23:59:59 UTC", time.Date(1970, 1, 1, 23, 59, 59, 0, time.UTC), 0},
		{"1970-01-02 UTC", time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := convertLeafValue(tc.t, iceberg.DateType{}, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, pq.Int32())
		})
	}
}

// TestConvertTimeUsesValueLocation verifies that time.Time inputs are
// extracted in the value's own location (zoneless wall-clock semantics)
// rather than UTC-shifted. A 14:30 EST time.Time should produce the
// 14:30 microsecond offset, not 19:30 UTC.
func TestConvertTimeUsesValueLocation(t *testing.T) {
	est, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	v := time.Date(2024, 1, 15, 14, 30, 0, 0, est)
	pq, err := convertLeafValue(v, iceberg.TimeType{}, nil)
	require.NoError(t, err)
	want := int64(14*3600+30*60) * 1_000_000
	assert.Equal(t, want, pq.Int64())
}

// TestConvertTimestampLegacyNilLogical verifies that a Timestamp schema
// with no Logical params (legacy / hand-constructed) treats numeric inputs
// as millis (the EffectiveTimestamp default) rather than seconds (the
// bloblang fallback). Without this guard, a numeric millis value declared
// by the schema as Timestamp would land 1000× too far in the future.
func TestConvertTimestampLegacyNilLogical(t *testing.T) {
	const epochMillis = int64(1_730_000_000_000) // 2024-10-27 03:33:20 UTC
	const wantMicros = epochMillis * 1000

	common := &schema.Common{Type: schema.Timestamp} // nil Logical
	pq, err := convertLeafValue(epochMillis, iceberg.TimestampTzType{}, common)
	require.NoError(t, err)
	assert.Equal(t, wantMicros, pq.Int64())
}

// TestScaleTimestampNumericPanicsOnInvalid asserts the defense-in-depth
// behavior: an invalid TimeUnit slipping past Validate() must not silently
// degrade to a wrong unit interpretation.
func TestScaleTimestampNumericPanicsOnInvalid(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invalid TimeUnit")
		}
	}()
	scaleTimestampNumeric(123, schema.TimeUnit(99), false)
}

func tsCommon(u schema.TimeUnit, utc bool) *schema.Common {
	return &schema.Common{
		Type:    schema.Timestamp,
		Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: u, AdjustToUTC: utc}},
	}
}

func todCommon(u schema.TimeUnit) *schema.Common {
	return &schema.Common{
		Type:    schema.TimeOfDay,
		Logical: &schema.LogicalParams{TimeOfDay: &schema.TimeOfDayParams{Unit: u}},
	}
}
