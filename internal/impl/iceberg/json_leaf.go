// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
	"github.com/redpanda-data/connect/v4/internal/impl/parquet/parquetdecimal"
)

// dayMicros is the number of microseconds in a 24h day, bounding the
// wall-clock range a TIME column can represent.
const dayMicros = 24 * 60 * 60 * 1_000_000

// jsonLeafValue canonicalises a single primitive leaf value into the
// JSON-encodable form that table.SchemaToArrowSchema + array.RecordFromJSON
// parse losslessly for the column's iceberg type.
//
// It is the ONE leaf encoder shared by every mutation path — the copy-on-write
// rewrite (cowMassage), the merge-on-read equality-delete keys
// (writeEqualityDeletes), and the copy-on-write filter literals (cowKeyLiteral)
// — and its overriding invariant is agreement with the INSERT path (the
// shredder): for any (iceberg type, Go input shape) the value encoded here must
// land, after the Arrow JSON round trip, exactly the bytes the shredder stores
// for the same input, or the input must be rejected loudly on both sides. Any
// drift is a silent no-match on a merge key (upserts that duplicate forever,
// deletes that remove nothing) or a silent value change on a copy-on-write
// rewrite. TestJSONLeafValueMatchesInsertPath pins the agreement input shape by
// input shape.
//
// common carries the field's upstream schema metadata (or nil) and
// requireSchemaMetadata mirrors the shredder's StrictTemporalMode; together
// they make a bare numeric value in a temporal column — including a temporal
// MERGE KEY — interpreted with the identical unit-aware conversion the insert
// path applies (shredder.NumericTemporalToTime). Numeric temporal keys used to
// be rejected as ambiguous; sharing the data columns' metadata-aware
// interpretation is strictly more faithful, because both sides of the key
// comparison now apply one interpretation (and strict mode still refuses the
// genuinely ambiguous no-metadata case).
//
// Deliberate strictness (divergences from what the Arrow JSON readers would
// happily accept):
//   - integer columns reject numeric STRINGS ("42"): the insert path rejects
//     them, so accepting them here would let mutating batches commit values the
//     equivalent insert loudly refuses (a flip-flopping pipeline). This
//     intentionally removes an accidental leniency of the old mutation-path
//     encoder. A consequence is that map columns with integer KEY types are
//     rejected (Go map keys arrive as strings) — the insert path can never
//     write such a map either.
//   - boolean columns accept only bool: Arrow's boolean builder would parse
//     "true"/"1", which the insert path rejects.
//
// Integers and decimals are emitted as JSON strings to avoid the float64
// precision loss JSON number decoding would otherwise incur; temporal values
// are formatted to the canonical string forms the Arrow readers accept,
// truncated to the column's microsecond resolution exactly as the insert path
// truncates (UnixMicro). A time.Time bound for a STRING column is NOT
// truncated: the insert path stores the full-nanosecond RFC 3339 text, so
// truncating would silently change the stored text on a rewrite.
func jsonLeafValue(t iceberg.Type, v any, common *schema.Common, requireSchemaMetadata bool) (any, error) {
	// Resolve a bare numeric in a temporal column to a time.Time using the
	// shredder's unit-aware conversion (honouring the field's schema metadata
	// and require_schema_metadata) so the encoded value matches what the
	// insert path stores for the identical input.
	if tm, ok, err := shredder.NumericTemporalToTime(v, t, common, requireSchemaMetadata); err != nil {
		return nil, err
	} else if ok {
		v = tm
	}

	switch tt := t.(type) {
	case iceberg.Int32Type, iceberg.Int64Type:
		// Temporal-to-numeric coercion bridge, mirroring the shredder: a table
		// whose column pre-dates the schema-metadata fix holds BIGINT/INT while
		// the upgraded upstream emits time.Time/time.Duration plus metadata
		// declaring the temporal type. The insert path accepts exactly that
		// population (shredder.CoerceTemporalToNumeric), so mutations must too
		// — including the strict-mode refusal.
		if n, ok := shredder.CoerceTemporalToNumeric(v, common); ok {
			if requireSchemaMetadata {
				return nil, fmt.Errorf("integer column received %T while schema metadata declares type %v; require_schema_metadata=true demands the existing column type match the schema metadata — recreate the table to migrate", v, common.Type)
			}
			if _, is32 := t.(iceberg.Int32Type); is32 && (n > math.MaxInt32 || n < math.MinInt32) {
				return nil, fmt.Errorf("int column received %T with schema metadata type %v; coerced value %d overflows int32 — the column should be BIGINT or the schema metadata is wrong", v, common.Type, n)
			}
			return strconv.FormatInt(n, 10), nil
		}
		switch n := v.(type) {
		case json.Number:
			return n.String(), nil
		case int:
			return strconv.FormatInt(int64(n), 10), nil
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		case float64:
			if n != math.Trunc(n) {
				return nil, fmt.Errorf("integer column given non-integer value %v", n)
			}
			// A float64 only represents integers exactly up to 2^53; beyond
			// that int64(n) loses precision or overflows, silently producing a
			// wrong value or key. Reject rather than corrupt — pass large
			// integers as an int64/json.Number instead.
			if math.Abs(n) >= 1<<53 {
				return nil, fmt.Errorf("integer column given value %v outside the range representable exactly as a float64 (provide it as an integer)", n)
			}
			return strconv.FormatInt(int64(n), 10), nil
		default:
			// Deliberately includes string — see the doc comment.
			return nil, fmt.Errorf("unsupported value type %T for integer column", v)
		}
	case iceberg.DecimalType:
		// Emitted as a string at the column's exact scale so the encoded value
		// matches the stored decimal; JSON number decoding or the shortest
		// float representation would not.
		prec, scale := tt.Precision(), tt.Scale()
		switch n := v.(type) {
		case json.Number:
			return n.String(), nil
		case string:
			return n, nil
		case float64:
			// Ties must round half-AWAY-from-zero exactly like the shredder's
			// unscaled conversion (0.125 at scale 2 stores unscaled 13, so the
			// encoded form must be "0.13"); strconv.FormatFloat's half-to-even
			// would silently diverge on every exact binary tie.
			return shredder.DecimalFloatToString(n, prec, scale)
		case float32:
			return shredder.DecimalFloatToString(float64(n), prec, scale)
		case int:
			return strconv.Itoa(n), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case int16:
			return strconv.FormatInt(int64(n), 10), nil
		case int8:
			return strconv.FormatInt(int64(n), 10), nil
		case uint:
			return strconv.FormatUint(uint64(n), 10), nil
		case uint64:
			return strconv.FormatUint(n, 10), nil
		case uint32:
			return strconv.FormatUint(uint64(n), 10), nil
		case uint16:
			return strconv.FormatUint(uint64(n), 10), nil
		case uint8:
			return strconv.FormatUint(uint64(n), 10), nil
		case []byte:
			// The insert path stores an exact-width big-endian two's-complement
			// unscaled value verbatim; render that unscaled value at the column
			// scale so the Arrow decimal parser reproduces the same bytes.
			expected := parquetdecimal.ByteWidth(prec)
			if len(n) != expected {
				return nil, fmt.Errorf("decimal []byte length %d does not match expected %d for precision %d", len(n), expected, prec)
			}
			unscaled := new(big.Int).SetBytes(n)
			if n[0]&0x80 != 0 {
				unscaled.Sub(unscaled, new(big.Int).Lsh(big.NewInt(1), uint(len(n))*8))
			}
			// The fraction is exact (denominator 10^scale), so FloatString
			// performs no rounding.
			return new(big.Rat).SetFrac(unscaled, parquetdecimal.Pow10(scale)).FloatString(scale), nil
		default:
			return nil, fmt.Errorf("unsupported value type %T for decimal column", v)
		}
	case iceberg.BooleanType:
		// Only bool — see the doc comment.
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("cannot convert %T to boolean", v)
	case iceberg.StringType:
		switch s := v.(type) {
		case []byte:
			// json.Marshal would base64-encode []byte; the insert path stores
			// the raw bytes verbatim.
			return string(s), nil
		case time.Time:
			// Full-nanosecond RFC 3339 in the value's own location — exactly
			// the text the insert path stores. No µs truncation here: that
			// applies only to the µs-resolution temporal columns.
			return s.Format(time.RFC3339Nano), nil
		default:
			// Strings pass through verbatim; any other shape is left for the
			// Arrow string builder to reject, matching the insert path's
			// rejection of non-textual values.
			return v, nil
		}
	case iceberg.BinaryType:
		// Arrow's BinaryBuilder base64-DECODES every JSON string, so every
		// accepted shape must be base64-encoded on the way in — passing a Go
		// string through unchanged would store its base64 decoding (silent
		// corruption) or fail the batch when it isn't valid base64.
		switch b := v.(type) {
		case []byte:
			return base64.StdEncoding.EncodeToString(b), nil
		case string:
			return base64.StdEncoding.EncodeToString([]byte(b)), nil
		case time.Time:
			// The insert path accepts a time.Time into a binary column as its
			// full-nanosecond RFC 3339 text bytes.
			return base64.StdEncoding.EncodeToString(b.AppendFormat(nil, time.RFC3339Nano)), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to binary", v)
		}
	case iceberg.FixedType:
		// The insert path accepts ONLY []byte for fixed — mirror it exactly:
		// encode []byte for the base64-decoding Arrow reader, reject everything
		// else loudly rather than let a plain string be stored as its base64
		// decoding (or fail the batch when it isn't valid base64).
		if b, ok := v.([]byte); ok {
			return base64.StdEncoding.EncodeToString(b), nil
		}
		return nil, fmt.Errorf("cannot convert %T to fixed", v)
	case iceberg.UUIDType:
		switch u := v.(type) {
		case string:
			// Canonical text passes through unchanged (the pre-existing
			// contract); Arrow's uuid extension parses it into the same 16
			// bytes the insert path stores.
			return u, nil
		case []byte:
			// The insert path accepts a 16-byte value (the avro fixed[16]
			// decode shape); Arrow's uuid extension parses only text, so
			// canonicalise the bytes to the equivalent canonical string.
			id, err := uuid.FromBytes(u)
			if err != nil {
				return nil, fmt.Errorf("invalid UUID bytes: %w", err)
			}
			return id.String(), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to UUID", v)
		}
	case iceberg.DateType:
		// UTC calendar date: the insert path floors the instant to days since
		// the epoch in UTC, and formatting the UTC date reproduces that day.
		if tm, ok := v.(time.Time); ok {
			return tm.UTC().Format("2006-01-02"), nil
		}
		return nil, fmt.Errorf("date column requires a time or numeric value, got %T", v)
	case iceberg.TimeType:
		switch tv := v.(type) {
		case time.Time:
			// Wall clock in the value's OWN location, truncated to
			// microseconds. Both halves must match what the insert path
			// stores: the shredder's convertTime extracts H/M/S/ns in the
			// value's location (14:30 EST stores 14:30, not 19:30 UTC) at
			// microsecond resolution — formatting tv.UTC() here would shift
			// the encoded key/value by the zone offset and silently match
			// nothing. Go's ".999999" verb truncates (never rounds) and
			// Arrow's time64[us] parsers reject more than 6 fractional
			// digits, so µs truncation is also what makes the value
			// parseable downstream.
			return tv.Format("15:04:05.999999"), nil
		case time.Duration:
			// The insert path accepts time.Duration directly (the twmb/avro
			// decode of time-millis/time-micros) as microseconds since
			// midnight; render the equivalent wall clock. A duration outside
			// [0, 24h) has no wall-clock text, so reject it loudly rather
			// than wrap around.
			us := tv.Microseconds()
			if us < 0 || us >= dayMicros {
				return nil, fmt.Errorf("time column given duration %v outside the time-of-day range [0, 24h)", tv)
			}
			return time.UnixMicro(us).UTC().Format("15:04:05.999999"), nil
		default:
			return nil, fmt.Errorf("time column requires a time value, got %T", v)
		}
	case iceberg.TimestampType, iceberg.TimestampTzType:
		// A string timestamp is accepted by the insert path (the shredder's
		// convertTimestamp falls back to bloblang.ValueAsTimestamp for
		// non-numeric values, parsing RFC 3339), so the mutation paths must
		// accept it identically — rejecting it here would deterministically
		// fail every mutating batch over data the table already holds.
		if s, ok := v.(string); ok {
			tm, err := bloblang.ValueAsTimestamp(s)
			if err != nil {
				return nil, fmt.Errorf("timestamp column given unparseable string %q: %w", s, err)
			}
			v = tm
		}
		if tm, ok := v.(time.Time); ok {
			// Truncate to the column's microsecond resolution BEFORE
			// formatting: the insert path stores UnixMicro (identical
			// truncation) and the Arrow timestamp[us] JSON parser hard-errors
			// on more than six fractional digits — an untruncated
			// nanosecond-precision value would fail every mutating batch
			// carrying it.
			return tm.Truncate(time.Microsecond).UTC().Format(time.RFC3339Nano), nil
		}
		return nil, fmt.Errorf("timestamp column requires a time or numeric value, got %T", v)
	case iceberg.Float32Type, iceberg.Float64Type:
		// json.Marshal rejects NaN/±Inf outright, but the insert path stores
		// them (parquet supports non-finite doubles); Arrow's float builders
		// parse them back from their strconv string forms. Finite values pass
		// through as JSON numbers.
		switch f := v.(type) {
		case float64:
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return strconv.FormatFloat(f, 'g', -1, 64), nil
			}
		case float32:
			if f64 := float64(f); math.IsNaN(f64) || math.IsInf(f64, 0) {
				return strconv.FormatFloat(f64, 'g', -1, 32), nil
			}
		}
		return v, nil
	default:
		// Everything else (the V3 nanosecond timestamps, unknown types) has no
		// JSON round trip that matches what the insert path stores; both
		// copy-on-write (checkCOWSchemaSupported) and merge-on-read
		// (deleteRecordFields) gate these upstream, so reaching here is
		// surfaced loudly rather than passed through to be mis-stored.
		return nil, fmt.Errorf("unsupported column type %s for the JSON mutation path", t)
	}
}
