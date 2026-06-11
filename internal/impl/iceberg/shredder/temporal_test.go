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
			pq, err := convertLeafValue(tc.value, tc.typ, tc.common, false)
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
			pq, err := convertLeafValue(tc.value, iceberg.DateType{}, nil, false)
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
			pq, err := convertLeafValue(tc.value, iceberg.TimeType{}, tc.common, false)
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
			pq, err := convertLeafValue(tc.t, iceberg.DateType{}, nil, false)
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
	pq, err := convertLeafValue(v, iceberg.TimeType{}, nil, false)
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
	pq, err := convertLeafValue(epochMillis, iceberg.TimestampTzType{}, common, false)
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

// TestTemporalRejectsNaNInf locks in a defense-in-depth guard against
// silent corruption: a NaN or ±Inf float reaching a time-typed column
// must fail loudly rather than be cast to implementation-defined
// int32/int64 garbage (typically 0, i.e. year 1970).
func TestTemporalRejectsNaNInf(t *testing.T) {
	cases := []struct {
		name string
		typ  iceberg.Type
		val  any
	}{
		{"date NaN", iceberg.DateType{}, math.NaN()},
		{"date +Inf", iceberg.DateType{}, math.Inf(1)},
		{"date -Inf", iceberg.DateType{}, math.Inf(-1)},
		{"time NaN", iceberg.TimeType{}, math.NaN()},
		{"time +Inf", iceberg.TimeType{}, math.Inf(1)},
		{"timestamp NaN", iceberg.TimestampTzType{}, math.NaN()},
		{"timestamp +Inf", iceberg.TimestampTzType{}, math.Inf(1)},
		{"timestamp -Inf", iceberg.TimestampTzType{}, math.Inf(-1)},
		{"timestamp_ns NaN", iceberg.TimestampNsType{}, math.NaN()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertLeafValue(tc.val, tc.typ, nil, false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot convert")
		})
	}
}

// TestNumericInt64FiltersNaNInf verifies the helper returns (0, false)
// for non-finite floats so callers can route them to explicit error paths
// instead of letting the int64 cast produce garbage.
func TestNumericInt64FiltersNaNInf(t *testing.T) {
	for _, v := range []any{math.NaN(), math.Inf(1), math.Inf(-1), float32(math.NaN()), float32(math.Inf(1))} {
		_, ok := numericInt64(v)
		assert.False(t, ok, "%v should not extract as int64", v)
	}
}

// TestStrictTemporalModeRejectsNumericWithoutMetadata exercises the
// require_schema_metadata=true path: numeric inputs into time-typed
// columns must fail loudly when no schema.Common has been registered for
// the field.
func TestStrictTemporalModeRejectsNumericWithoutMetadata(t *testing.T) {
	cases := []struct {
		name string
		typ  iceberg.Type
		val  any
	}{
		{"date numeric strict", iceberg.DateType{}, int64(19737)},
		{"time numeric strict", iceberg.TimeType{}, int64(123456)},
		{"timestamp numeric strict", iceberg.TimestampTzType{}, int64(1_730_000_000_000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertLeafValue(tc.val, tc.typ, nil, true)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "require_schema_metadata=true")
		})
	}
}

// TestStrictTemporalModeRejectsWrongTypeMetadata verifies that strict
// mode is not satisfied by *any* schema.Common — the registered Type
// must match the iceberg-side column type. Otherwise a hand-crafted
// metadata mismatch (e.g. claiming a column is `string` while iceberg
// actually has it as DATE) would silently pass the strict check and
// the value-converter would interpret the numeric using the wrong
// shape.
func TestStrictTemporalModeRejectsWrongTypeMetadata(t *testing.T) {
	cases := []struct {
		name   string
		typ    iceberg.Type
		val    any
		common *schema.Common
	}{
		{
			name:   "DATE column with String-typed metadata",
			typ:    iceberg.DateType{},
			val:    int64(19737),
			common: &schema.Common{Type: schema.String},
		},
		{
			name:   "DATE column with Int64-typed metadata",
			typ:    iceberg.DateType{},
			val:    int64(19737),
			common: &schema.Common{Type: schema.Int64},
		},
		{
			name: "TIME column with Timestamp-typed metadata",
			typ:  iceberg.TimeType{},
			val:  int64(8 * 3600 * 1_000_000),
			common: &schema.Common{
				Type:    schema.Timestamp,
				Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: true}},
			},
		},
		{
			name:   "TIMESTAMPTZ column with Date-typed metadata",
			typ:    iceberg.TimestampTzType{},
			val:    int64(1_730_000_000_000),
			common: &schema.Common{Type: schema.Date},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := convertLeafValue(tc.val, tc.typ, tc.common, true)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "require_schema_metadata=true")
		})
	}
}

// TestStrictTemporalModeAcceptsTimeTypeNatives confirms that strict mode
// has no effect when the input is already an unambiguous Go time type.
func TestStrictTemporalModeAcceptsTimeTypeNatives(t *testing.T) {
	const epochMillis = int64(1_730_000_000_000)
	t.Run("time.Time into TIMESTAMPTZ without metadata", func(t *testing.T) {
		v := time.UnixMilli(epochMillis).UTC()
		pq, err := convertLeafValue(v, iceberg.TimestampTzType{}, nil, true)
		require.NoError(t, err)
		assert.Equal(t, epochMillis*1000, pq.Int64())
	})
	t.Run("time.Duration into TIME without metadata", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		pq, err := convertLeafValue(d, iceberg.TimeType{}, nil, true)
		require.NoError(t, err)
		assert.Equal(t, int64(8*3600+30*60)*1_000_000, pq.Int64())
	})
	t.Run("time.Time into DATE without metadata", func(t *testing.T) {
		v := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		pq, err := convertLeafValue(v, iceberg.DateType{}, nil, true)
		require.NoError(t, err)
		assert.Equal(t, int32(19737), pq.Int32())
	})
}

// TestStrictTemporalModeAcceptsNumericWithMetadata confirms that strict
// mode is inert once schema metadata is registered for the field.
func TestStrictTemporalModeAcceptsNumericWithMetadata(t *testing.T) {
	const epochMillis = int64(1_730_000_000_000)
	common := tsCommon(schema.TimeUnitMillis, true)
	pq, err := convertLeafValue(epochMillis, iceberg.TimestampTzType{}, common, true)
	require.NoError(t, err)
	assert.Equal(t, epochMillis*1000, pq.Int64())
}

// TestCoerceTemporalIntoNumericColumn covers the rolling-upgrade case where
// an existing iceberg table has BIGINT / INT columns (from the pre-fix
// metadata bug for #4399) but the upgraded upstream now emits time.Time /
// time.Duration values together with correct schema.Common metadata.
//
// Without this path the shredder hands the temporal value to
// bloblang.ValueAsInt64 and fails with "expected number value, got
// timestamp" — the exact symptom that surfaced this work. The conversion
// must honour the schema's declared unit so the integer that lands in the
// column matches what the operator's pre-fix pipeline had been storing.
func TestCoerceTemporalIntoNumericColumn(t *testing.T) {
	const tsMillis = int64(1_700_000_000_000) // 2023-11-14 22:13:20 UTC

	t.Run("time.Time into Int64 with Timestamp(Millis)", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		pq, err := convertLeafValue(v, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), false)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, pq.Int64())
	})

	t.Run("time.Time into Int64 with Timestamp(Micros)", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		pq, err := convertLeafValue(v, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMicros, true), false)
		require.NoError(t, err)
		assert.Equal(t, tsMillis*1000, pq.Int64())
	})

	t.Run("time.Time into Int64 with Timestamp(Nanos)", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		pq, err := convertLeafValue(v, iceberg.Int64Type{}, tsCommon(schema.TimeUnitNanos, true), false)
		require.NoError(t, err)
		assert.Equal(t, tsMillis*1_000_000, pq.Int64())
	})

	t.Run("time.Time into Int32 with Date", func(t *testing.T) {
		v := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		pq, err := convertLeafValue(v, iceberg.Int32Type{}, &schema.Common{Type: schema.Date}, false)
		require.NoError(t, err)
		assert.Equal(t, int32(19737), pq.Int32())
	})

	t.Run("time.Duration into Int64 with TimeOfDay(Millis)", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		pq, err := convertLeafValue(d, iceberg.Int64Type{}, todCommon(schema.TimeUnitMillis), false)
		require.NoError(t, err)
		assert.Equal(t, int64(8*3600+30*60)*1000, pq.Int64())
	})

	t.Run("time.Duration into Int32 with TimeOfDay(Millis)", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		pq, err := convertLeafValue(d, iceberg.Int32Type{}, todCommon(schema.TimeUnitMillis), false)
		require.NoError(t, err)
		assert.Equal(t, int32(8*3600+30*60)*1000, pq.Int32())
	})

	// Non-temporal values into numeric columns continue to flow through the
	// existing bloblang.ValueAsInt64 path unchanged — coerce is opt-in via
	// the temporal-value-type predicate.
	t.Run("plain int64 into Int64 with Timestamp metadata is unchanged", func(t *testing.T) {
		pq, err := convertLeafValue(tsMillis, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), false)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, pq.Int64())
	})

	t.Run("string into Int64 with Timestamp metadata still errors", func(t *testing.T) {
		_, err := convertLeafValue("not a number", iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), false)
		require.Error(t, err)
	})

	// Metadata-mismatched cases fall through to bloblang, which fails loudly
	// rather than silently misinterpreting the value.
	t.Run("time.Time into Int64 without metadata falls through to bloblang and errors", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		_, err := convertLeafValue(v, iceberg.Int64Type{}, nil, false)
		require.Error(t, err)
	})

	t.Run("time.Duration into Int64 with Timestamp metadata (mismatch) falls through and errors", func(t *testing.T) {
		d := 8 * time.Hour
		_, err := convertLeafValue(d, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), false)
		require.Error(t, err)
	})
}

// TestCoerceTemporalInt32OverflowGuard covers the schema/column mismatch
// where a TIMESTAMP schema common reaches an Int32 column — coerceTemporal-
// ToNumeric returns a UnixMilli value (~10^12) that vastly exceeds int32
// range. The Int32 arm is intended for Date / TimeOfDay coercions whose
// values fit in int32; a Timestamp value silently truncating into a garbage
// year is the failure mode this guard exists to prevent.
//
// The complementary Int64 arm has no bounds problem (Timestamp values fit
// comfortably in int64) and is verified separately by
// TestCoerceTemporalIntoNumericColumn.
func TestCoerceTemporalInt32OverflowGuard(t *testing.T) {
	const tsMillis = int64(1_700_000_000_000) // 2023-11-14, ~10^12, well beyond int32

	t.Run("Timestamp(Millis) into Int32 errors with overflow message", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		_, err := convertLeafValue(v, iceberg.Int32Type{}, tsCommon(schema.TimeUnitMillis, true), false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows int32", "must reject the schema/column mismatch loudly, not truncate to a garbage year")
	})

	t.Run("Timestamp(Micros) into Int32 errors with overflow message", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		_, err := convertLeafValue(v, iceberg.Int32Type{}, tsCommon(schema.TimeUnitMicros, true), false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "overflows int32")
	})

	// Sanity check: the in-range coercions the Int32 arm was designed for
	// still succeed.
	t.Run("Date into Int32 still succeeds", func(t *testing.T) {
		v := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		pq, err := convertLeafValue(v, iceberg.Int32Type{}, &schema.Common{Type: schema.Date}, false)
		require.NoError(t, err)
		assert.Equal(t, int32(19737), pq.Int32())
	})

	t.Run("TimeOfDay into Int32 still succeeds", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		pq, err := convertLeafValue(d, iceberg.Int32Type{}, todCommon(schema.TimeUnitMillis), false)
		require.NoError(t, err)
		assert.Equal(t, int32(8*3600+30*60)*1000, pq.Int32())
	})
}

// TestCoerceTemporalRejectedInStrictMode confirms that the temporal->numeric
// coerce path is disabled when require_schema_metadata=true. In strict mode
// a type disagreement between the existing column and the schema metadata
// is a hard error rather than a silent conversion — the operator has
// explicitly asked us not to bridge across stale tables.
func TestCoerceTemporalRejectedInStrictMode(t *testing.T) {
	const tsMillis = int64(1_700_000_000_000)

	t.Run("time.Time into Int64 with Timestamp(Millis) errors", func(t *testing.T) {
		v := time.UnixMilli(tsMillis).UTC()
		_, err := convertLeafValue(v, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "require_schema_metadata=true")
	})

	t.Run("time.Time into Int32 with Date errors", func(t *testing.T) {
		v := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
		_, err := convertLeafValue(v, iceberg.Int32Type{}, &schema.Common{Type: schema.Date}, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "require_schema_metadata=true")
	})

	t.Run("time.Duration into Int64 with TimeOfDay errors", func(t *testing.T) {
		d := 8*time.Hour + 30*time.Minute
		_, err := convertLeafValue(d, iceberg.Int64Type{}, todCommon(schema.TimeUnitMillis), true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "require_schema_metadata=true")
	})

	// Strict mode is inert when the value/column types already agree — it
	// only rejects coerce situations, not the happy path.
	t.Run("plain int64 into Int64 with Timestamp metadata still succeeds in strict", func(t *testing.T) {
		pq, err := convertLeafValue(tsMillis, iceberg.Int64Type{}, tsCommon(schema.TimeUnitMillis, true), true)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, pq.Int64())
	})

	t.Run("plain int64 into Int64 without metadata still succeeds in strict", func(t *testing.T) {
		pq, err := convertLeafValue(tsMillis, iceberg.Int64Type{}, nil, true)
		require.NoError(t, err)
		assert.Equal(t, tsMillis, pq.Int64())
	})
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
