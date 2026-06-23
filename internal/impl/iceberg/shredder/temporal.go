// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/schema"
)

// commonForField returns the upstream schema.Common registered for the given
// iceberg field ID, or nil when no metadata has been registered for that
// field. Returns nil when the shredder itself has no metadata at all.
func (rs *RecordShredder) commonForField(fieldID int) *schema.Common {
	if rs.fieldCommons == nil {
		return nil
	}
	return rs.fieldCommons[fieldID]
}

// convertDate converts a Go value into the Parquet representation of an
// Iceberg DATE column (int32 days since the unix epoch). Accepts time.Time
// (UTC midnight idiom from twmb/avro `date` decode), or a numeric value
// already expressing days since epoch.
//
// strictTemporal causes numeric inputs without registered schema metadata
// to be rejected — the column is DATE (days since epoch) but a bare
// numeric could be days, milliseconds, or seconds depending on upstream;
// strict mode forces the operator to attach metadata or convert upstream.
func convertDate(value any, common *schema.Common, strictTemporal bool) (parquet.Value, error) {
	switch v := value.(type) {
	case time.Time:
		// Iceberg / Avro DATE is days since 1970-01-01, including negative
		// values for pre-epoch dates. Go integer division truncates toward
		// zero, so we floor explicitly: 1969-12-31 23:59:59 UTC has
		// Unix() == -1, and floor(-1/86400) == -1, not 0.
		secs := v.UTC().Unix()
		days := secs / 86400
		if secs < 0 && secs%86400 != 0 {
			days--
		}
		return parquet.Int32Value(int32(days)), nil
	case int32, int, int64, float64:
		// In strict mode, a numeric value into a DATE column must come
		// with metadata whose Type is also Date — anything else either
		// has no shape information at all or has the wrong shape, both
		// of which the operator asked us to fail on.
		if strictTemporal && (common == nil || common.Type != schema.Date) {
			return parquet.NullValue(), errors.New("date column received numeric value without matching schema.Common (Type=Date); require_schema_metadata=true demands a Date metadata entry to disambiguate the unit")
		}
		switch v := v.(type) {
		case int32:
			return parquet.Int32Value(v), nil
		case int:
			return parquet.Int32Value(int32(v)), nil
		case int64:
			return parquet.Int32Value(int32(v)), nil
		case float64:
			// NaN and ±Inf cast to int32 as implementation-defined garbage;
			// silent corruption (year 1970) is worse than a hard error.
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return parquet.NullValue(), fmt.Errorf("cannot convert %v to date", v)
			}
			return parquet.Int32Value(int32(v)), nil
		}
		return parquet.NullValue(), fmt.Errorf("cannot convert %T to date", value)
	default:
		return parquet.NullValue(), fmt.Errorf("cannot convert %T to date", value)
	}
}

// convertTime converts a Go value into the Parquet representation of an
// Iceberg TIME column (int64 microseconds since midnight). Accepts
// time.Duration directly (the twmb/avro decode of time-millis/time-micros),
// or a numeric value whose unit is consulted via common.Logical.TimeOfDay
// when present. Without schema metadata, numeric inputs are treated as
// already in microseconds — matching the historical behavior for numeric
// values landing in this column.
//
// time.Time inputs are extracted in the value's own [time.Location], not
// converted to UTC first. This treats time-of-day as zoneless wall-clock
// (matching Java's LocalTime, Postgres TIME, and Iceberg TIME): a
// time.Time of 14:30 EST yields 14:30 in the column, not 19:30 UTC.
// If the upstream wants UTC-shifted, it should v.UTC() before passing in.
func convertTime(value any, common *schema.Common, strictTemporal bool) (parquet.Value, error) {
	switch v := value.(type) {
	case time.Duration:
		return parquet.Int64Value(v.Microseconds()), nil
	case time.Time:
		dur := time.Duration(v.Hour())*time.Hour +
			time.Duration(v.Minute())*time.Minute +
			time.Duration(v.Second())*time.Second +
			time.Duration(v.Nanosecond())*time.Nanosecond
		return parquet.Int64Value(dur.Microseconds()), nil
	case int64, int32, int, float64:
		// In strict mode, a numeric value into a TIME column must come
		// with TimeOfDay-typed metadata that names the unit. Anything
		// else (no metadata, wrong-typed metadata, or a TimeOfDay common
		// missing its Logical params) means the unit is undeclared and
		// the operator asked us to fail rather than guess.
		if strictTemporal && (common == nil || common.Type != schema.TimeOfDay || common.Logical == nil || common.Logical.TimeOfDay == nil) {
			return parquet.NullValue(), errors.New("time column received numeric value without matching schema.Common (Type=TimeOfDay with Logical.TimeOfDay); require_schema_metadata=true demands a TimeOfDay metadata entry with declared unit")
		}
		switch v := v.(type) {
		case int64:
			return parquet.Int64Value(numericToTimeMicros(v, common)), nil
		case int32:
			// Avro time-millis is int32; accept it directly as well.
			return parquet.Int64Value(numericToTimeMicros(int64(v), common)), nil
		case int:
			return parquet.Int64Value(numericToTimeMicros(int64(v), common)), nil
		case float64:
			// NaN/±Inf cast is implementation-defined; reject explicitly.
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return parquet.NullValue(), fmt.Errorf("cannot convert %v to time", v)
			}
			return parquet.Int64Value(numericToTimeMicros(int64(v), common)), nil
		}
		return parquet.NullValue(), fmt.Errorf("cannot convert %T to time", value)
	default:
		return parquet.NullValue(), fmt.Errorf("cannot convert %T to time", value)
	}
}

// numericToTimeMicros scales a numeric time-of-day value to microseconds
// using the schema's declared unit. Without schema metadata, the input is
// assumed to already be microseconds (Iceberg's internal representation),
// which preserves the pre-PR behavior for numeric inputs.
//
// An unrecognised TimeUnit reaching this function indicates a benthos
// schema-package contract violation — Validate() guards against it
// upstream — and we panic rather than silently misinterpret. Returning n
// unchanged would conflate a malformed schema with the no-metadata case.
func numericToTimeMicros(n int64, common *schema.Common) int64 {
	if common == nil || common.Logical == nil || common.Logical.TimeOfDay == nil {
		return n
	}
	switch common.Logical.TimeOfDay.Unit {
	case schema.TimeUnitSeconds:
		return n * 1_000_000
	case schema.TimeUnitMillis:
		return n * 1_000
	case schema.TimeUnitMicros:
		return n
	case schema.TimeUnitNanos:
		return n / 1_000
	default:
		panic(fmt.Sprintf("unreachable: invalid TimeUnit %v escaped Validate()", common.Logical.TimeOfDay.Unit))
	}
}

// convertTimestamp converts a Go value into the Parquet representation of an
// Iceberg TIMESTAMP / TIMESTAMPTZ column (int64 microseconds since epoch by
// default; nanoseconds when nanos is true, for the V3 *NsType variants).
// Accepts time.Time directly. For numeric inputs, prefers
// common.Logical.Timestamp.Unit to interpret the unit. When common is a
// Timestamp schema with no Logical (legacy / hand-constructed), falls back
// to EffectiveTimestamp's millis/UTC default rather than to
// bloblang.ValueAsTimestamp's seconds default — so a numeric millis value
// declared by the schema as Timestamp lands at the correct microsecond
// offset, not 1000× too far in the future.
//
// Only when no schema metadata is available at all do we fall through to
// bloblang.ValueAsTimestamp — which preserves the pre-PR behavior for
// callers that genuinely store unix-seconds in a numeric column without
// any schema annotation.
func convertTimestamp(value any, common *schema.Common, nanos, strictTemporal bool) (parquet.Value, error) {
	if v, ok := value.(time.Time); ok {
		if nanos {
			return parquet.Int64Value(v.UnixNano()), nil
		}
		return parquet.Int64Value(v.UnixMicro()), nil
	}

	// Reject NaN/±Inf up front. int64(NaN) is implementation-defined
	// garbage; left to fall through, this would silently produce year 1970
	// (or worse) timestamps via either the metadata path or the bloblang
	// fallback. Mirrors the guard the decimal converter already enforces.
	switch v := value.(type) {
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return parquet.NullValue(), fmt.Errorf("cannot convert %v to timestamp", v)
		}
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return parquet.NullValue(), fmt.Errorf("cannot convert %v to timestamp", v)
		}
	}

	// Numeric path. Honour the schema's declared unit when we have one.
	// A Timestamp schema with nil Logical falls through EffectiveTimestamp
	// to the legacy millis/UTC default, which is also the right
	// interpretation for the population that produced this PR: an
	// upstream Avro timestamp-millis value flowing through the schema
	// metadata path.
	if n, ok := numericInt64(value); ok && common != nil && common.Type == schema.Timestamp {
		p := common.EffectiveTimestamp()
		out := scaleTimestampNumeric(n, p.Unit, nanos)
		return parquet.Int64Value(out), nil
	}

	// Strict mode: refuse the bloblang fallback for numeric values when
	// the operator has asked us to fail loudly on missing metadata.
	if strictTemporal {
		if _, ok := numericInt64(value); ok {
			return parquet.NullValue(), errors.New("timestamp column received numeric value with no Timestamp schema metadata; require_schema_metadata=true demands a schema.Common with Type=Timestamp to disambiguate the unit")
		}
	}

	// No applicable schema metadata: keep the historical bloblang path.
	// This preserves behavior for callers that pass unix-seconds directly
	// into a numeric column without schema annotation.
	t, err := bloblang.ValueAsTimestamp(value)
	if err != nil {
		return parquet.NullValue(), err
	}
	if nanos {
		return parquet.Int64Value(t.UnixNano()), nil
	}
	return parquet.Int64Value(t.UnixMicro()), nil
}

// scaleTimestampNumeric scales a numeric timestamp from its declared unit
// into either microseconds (the standard Iceberg TIMESTAMP/TIMESTAMPTZ
// internal representation) or nanoseconds (the V3 *NsType internal
// representation).
//
// An unrecognised TimeUnit reaching this function indicates a benthos
// schema-package contract violation — Validate() guards against it
// upstream — and we panic rather than silently misinterpret. Returning n
// unchanged would conflate "unit=micros (no scaling needed)" with
// "malformed schema (no scaling possible)".
func scaleTimestampNumeric(n int64, unit schema.TimeUnit, nanos bool) int64 {
	if nanos {
		switch unit {
		case schema.TimeUnitSeconds:
			return n * 1_000_000_000
		case schema.TimeUnitMillis:
			return n * 1_000_000
		case schema.TimeUnitMicros:
			return n * 1_000
		case schema.TimeUnitNanos:
			return n
		default:
			panic(fmt.Sprintf("unreachable: invalid TimeUnit %v escaped Validate()", unit))
		}
	}
	switch unit {
	case schema.TimeUnitSeconds:
		return n * 1_000_000
	case schema.TimeUnitMillis:
		return n * 1_000
	case schema.TimeUnitMicros:
		return n
	case schema.TimeUnitNanos:
		return n / 1_000
	default:
		panic(fmt.Sprintf("unreachable: invalid TimeUnit %v escaped Validate()", unit))
	}
}

// coerceTemporalToNumeric handles the rolling-upgrade case where the iceberg
// table holds a numeric column (BIGINT / INT) that pre-dates the metadata
// fix for issue #4399, but the upgraded upstream now emits a temporal Go
// value (`time.Time` for Timestamp / Date, `time.Duration` for TimeOfDay)
// together with correct schema.Common metadata.
//
// Without this conversion the shredder's Int64Type / Int32Type arms would
// hand the temporal value to bloblang.ValueAsInt64, which fails with
// "expected number value, got timestamp" — the exact shape of the customer
// regression that surfaced this work.
//
// Returns (0, false) when no coercion applies (value is not temporal, common
// is absent, or the common's declared type doesn't match the value's Go
// type). Callers fall back to their existing numeric coercion path in that
// case.
//
// The conversion is unit-aware: a `time.Time` against a Timestamp(Millis)
// common produces UnixMilli, Micros produces UnixMicro, etc. — preserving
// the wire-equivalent representation the operator's pre-fix pipeline had
// been storing in the same column.
func coerceTemporalToNumeric(value any, common *schema.Common) (int64, bool) {
	if common == nil {
		return 0, false
	}
	switch v := value.(type) {
	case time.Time:
		switch common.Type {
		case schema.Timestamp:
			p := common.EffectiveTimestamp()
			switch p.Unit {
			case schema.TimeUnitSeconds:
				return v.Unix(), true
			case schema.TimeUnitMillis:
				return v.UnixMilli(), true
			case schema.TimeUnitMicros:
				return v.UnixMicro(), true
			case schema.TimeUnitNanos:
				return v.UnixNano(), true
			}
		case schema.Date:
			// Days since epoch, floored toward negative infinity to match
			// convertDate's pre-epoch handling.
			secs := v.UTC().Unix()
			days := secs / 86400
			if secs < 0 && secs%86400 != 0 {
				days--
			}
			return days, true
		}
	case time.Duration:
		if common.Type == schema.TimeOfDay && common.Logical != nil && common.Logical.TimeOfDay != nil {
			switch common.Logical.TimeOfDay.Unit {
			case schema.TimeUnitSeconds:
				return int64(v / time.Second), true
			case schema.TimeUnitMillis:
				return v.Milliseconds(), true
			case schema.TimeUnitMicros:
				return v.Microseconds(), true
			case schema.TimeUnitNanos:
				return v.Nanoseconds(), true
			}
		}
	}
	return 0, false
}

// numericInt64 extracts an int64 from an arbitrary numeric Go value. Returns
// (0, false) for non-numeric inputs and for NaN/±Inf floats — so callers
// can either error explicitly on those cases or fall through to a more
// permissive parsing path.
//
// Float inputs are truncated toward zero — sub-unit precision is lost
// silently. Floats outside the int64 representable range overflow to
// undefined int64 values per Go conversion semantics; in practice this
// is unreachable for any sane timestamp/date/time-of-day value, but
// callers passing arbitrary numeric values should prefer integer types
// to avoid the precision loss. NaN and ±Inf are filtered to (0, false)
// because their int64 conversion is implementation-defined garbage; the
// callers add explicit error paths so these can't silently corrupt.
//
// uint64 values above MaxInt64 overflow to negative int64 values; this
// matches what the existing iceberg.Int64Type arm rejects via an
// explicit range check, but here we accept the wrap because timestamp
// values that high are nonsensical regardless.
func numericInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case uint32:
		return int64(v), true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, false
		}
		return int64(v), true
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return 0, false
		}
		return int64(v), true
	}
	return 0, false
}
