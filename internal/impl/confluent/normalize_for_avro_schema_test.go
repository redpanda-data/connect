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
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	goavro "github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Primitives ---

func TestNormalizeAvroPrimitives(t *testing.T) {
	tests := []struct {
		name     string
		data     any
		schema   any
		expected any
	}{
		{"bool true", true, "boolean", true},
		{"bool false", false, "boolean", false},
		{"string", "hello", "string", "hello"},
		{"float64 passthrough", float64(3.14), "double", float64(3.14)},
		{"float64 to int32", float64(42), "int", int32(42)},
		{"float64 to int64", float64(1e12), "long", int64(1e12)},
		{"float64 to float32", float64(1.5), "float", float32(1.5)},
		{"int to int32", int(99), "int", int32(99)},
		{"int64 to int32", int64(7), "int", int32(7)},
		{"int32 to int64", int32(5), "long", int64(5)},
		{"json.Number to int32", json.Number("42"), "int", int32(42)},
		{"json.Number to int64", json.Number("9999999999"), "long", int64(9999999999)},
		{"json.Number to float32", json.Number("1.5"), "float", float32(1.5)},
		{"json.Number to float64", json.Number("3.14"), "double", float64(3.14)},
		{"bytes from []byte", []byte("raw"), "bytes", []byte("raw")},
		{"bytes from string", "raw", "bytes", []byte("raw")},
		{"null returns nil", "anything", "null", nil},
		{"nil data", nil, "string", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := normalizeForAvroSchema(tc.data, tc.schema, true)
			require.NoError(t, err)
			if tc.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestNormalizeAvroPrimitiveErrors(t *testing.T) {
	tests := []struct {
		name        string
		data        any
		schema      any
		errContains string
	}{
		{"int32 overflow", float64(3e10), "int", "overflows int32"},
		{"non-integer float for int", float64(1.5), "int", "expected integer"},
		{"wrong type for int", "nope", "int", "expected numeric"},
		{"wrong type for bool", "true", "boolean", "expected bool"},
		{"wrong type for string", 42, "string", "expected string"},
		{"wrong type for bytes", 42, "bytes", "expected []byte or string"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := normalizeForAvroSchema(tc.data, tc.schema, true)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

// --- Logical types ---

func TestNormalizeAvroTimestamp(t *testing.T) {
	millis := map[string]any{"type": "long", "logicalType": "timestamp-millis"}
	micros := map[string]any{"type": "long", "logicalType": "timestamp-micros"}

	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)

	t.Run("millis from time.Time", func(t *testing.T) {
		result, err := normalizeForAvroSchema(ts, millis, true)
		require.NoError(t, err)
		assert.Equal(t, ts, result)
	})

	t.Run("millis from RFC3339 string", func(t *testing.T) {
		result, err := normalizeForAvroSchema("2026-03-19T10:00:00Z", millis, true)
		require.NoError(t, err)
		assert.True(t, ts.Equal(result.(time.Time)))
	})

	t.Run("millis from int64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(ts.UnixMilli(), millis, true)
		require.NoError(t, err)
		assert.True(t, ts.Equal(result.(time.Time)))
	})

	t.Run("millis from float64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(float64(ts.UnixMilli()), millis, true)
		require.NoError(t, err)
		assert.True(t, ts.Equal(result.(time.Time)))
	})

	t.Run("millis from json.Number", func(t *testing.T) {
		n := json.Number(fmt.Sprintf("%d", ts.UnixMilli()))
		result, err := normalizeForAvroSchema(n, millis, true)
		require.NoError(t, err)
		assert.True(t, ts.Equal(result.(time.Time)))
	})

	t.Run("micros from int64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(ts.UnixMicro(), micros, true)
		require.NoError(t, err)
		assert.True(t, ts.Equal(result.(time.Time)))
	})

	t.Run("millis invalid string", func(t *testing.T) {
		_, err := normalizeForAvroSchema("not-a-time", millis, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing timestamp")
	})

	t.Run("millis wrong type", func(t *testing.T) {
		_, err := normalizeForAvroSchema(true, millis, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected time.Time, string, or numeric")
	})
}

func TestNormalizeAvroTimeDuration(t *testing.T) {
	timeMillis := map[string]any{"type": "int", "logicalType": "time-millis"}
	timeMicros := map[string]any{"type": "long", "logicalType": "time-micros"}

	t.Run("millis from int", func(t *testing.T) {
		result, err := normalizeForAvroSchema(int64(35245000), timeMillis, true)
		require.NoError(t, err)
		assert.Equal(t, time.Duration(35245000)*time.Millisecond, result)
	})

	t.Run("millis from float64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(float64(1000), timeMillis, true)
		require.NoError(t, err)
		assert.Equal(t, time.Second, result)
	})

	t.Run("millis from json.Number", func(t *testing.T) {
		result, err := normalizeForAvroSchema(json.Number("5000"), timeMillis, true)
		require.NoError(t, err)
		assert.Equal(t, 5*time.Second, result)
	})

	t.Run("millis from time.Duration", func(t *testing.T) {
		d := 3 * time.Second
		result, err := normalizeForAvroSchema(d, timeMillis, true)
		require.NoError(t, err)
		assert.Equal(t, d, result)
	})

	t.Run("micros from int64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(int64(1000000), timeMicros, true)
		require.NoError(t, err)
		assert.Equal(t, time.Second, result)
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := normalizeForAvroSchema("nope", timeMillis, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected time.Duration or numeric")
	})
}

func TestNormalizeAvroDate(t *testing.T) {
	dateSchema := map[string]any{"type": "int", "logicalType": "date"}
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("from int days since epoch", func(t *testing.T) {
		result, err := normalizeForAvroSchema(int64(19436), dateSchema, true)
		require.NoError(t, err)
		expected := epoch.AddDate(0, 0, 19436)
		assert.True(t, expected.Equal(result.(time.Time)))
	})

	t.Run("from date string", func(t *testing.T) {
		result, err := normalizeForAvroSchema("2026-03-19", dateSchema, true)
		require.NoError(t, err)
		expected := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
		assert.True(t, expected.Equal(result.(time.Time)))
	})

	t.Run("from time.Time passthrough", func(t *testing.T) {
		ts := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
		result, err := normalizeForAvroSchema(ts, dateSchema, true)
		require.NoError(t, err)
		assert.Equal(t, ts, result)
	})

	t.Run("from json.Number", func(t *testing.T) {
		result, err := normalizeForAvroSchema(json.Number("100"), dateSchema, true)
		require.NoError(t, err)
		expected := epoch.AddDate(0, 0, 100)
		assert.True(t, expected.Equal(result.(time.Time)))
	})

	t.Run("invalid date string", func(t *testing.T) {
		_, err := normalizeForAvroSchema("not-a-date", dateSchema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "parsing date")
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := normalizeForAvroSchema(true, dateSchema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected time.Time, string, or numeric")
	})
}

func TestNormalizeAvroDecimal(t *testing.T) {
	decSchema := map[string]any{"type": "bytes", "logicalType": "decimal", "precision": float64(16), "scale": float64(2)}

	t.Run("from *big.Rat passthrough", func(t *testing.T) {
		r := new(big.Rat).SetFloat64(3.14)
		result, err := normalizeForAvroSchema(r, decSchema, true)
		require.NoError(t, err)
		assert.Equal(t, r, result)
	})

	t.Run("from float64", func(t *testing.T) {
		result, err := normalizeForAvroSchema(float64(3.14), decSchema, true)
		require.NoError(t, err)
		rat := result.(*big.Rat)
		f, _ := rat.Float64()
		assert.InDelta(t, 3.14, f, 0.001)
	})

	t.Run("from numeric string", func(t *testing.T) {
		result, err := normalizeForAvroSchema("3.14", decSchema, true)
		require.NoError(t, err)
		rat := result.(*big.Rat)
		f, _ := rat.Float64()
		assert.InDelta(t, 3.14, f, 0.001)
	})

	t.Run("from json.Number", func(t *testing.T) {
		result, err := normalizeForAvroSchema(json.Number("1.5"), decSchema, true)
		require.NoError(t, err)
		rat := result.(*big.Rat)
		f, _ := rat.Float64()
		assert.InDelta(t, 1.5, f, 0.001)
	})

	t.Run("from raw bytes positive", func(t *testing.T) {
		// 0x21 = 33 decimal, with scale 2 → 0.33
		result, err := normalizeForAvroSchema([]byte{0x21}, decSchema, true)
		require.NoError(t, err)
		rat := result.(*big.Rat)
		f, _ := rat.Float64()
		assert.InDelta(t, 0.33, f, 0.001)
	})

	t.Run("from raw bytes negative", func(t *testing.T) {
		// 0xFF = -1 in two's complement, with scale 2 → -0.01
		result, err := normalizeForAvroSchema([]byte{0xFF}, decSchema, true)
		require.NoError(t, err)
		rat := result.(*big.Rat)
		f, _ := rat.Float64()
		assert.InDelta(t, -0.01, f, 0.001)
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := normalizeForAvroSchema(true, decSchema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected *big.Rat, string, or numeric")
	})
}

// --- Record ---

func TestNormalizeAvroRecord(t *testing.T) {
	schema := map[string]any{
		"type": "record",
		"name": "test",
		"fields": []any{
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "age", "type": "int"},
		},
	}

	t.Run("normalizes fields", func(t *testing.T) {
		data := map[string]any{"name": "alice", "age": float64(30)}
		result, err := normalizeForAvroSchema(data, schema, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, "alice", m["name"])
		assert.Equal(t, int32(30), m["age"])
	})

	t.Run("missing required field errors", func(t *testing.T) {
		data := map[string]any{"name": "alice"}
		_, err := normalizeForAvroSchema(data, schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `required field "age" is missing`)
	})

	t.Run("missing field with default is skipped", func(t *testing.T) {
		s := map[string]any{
			"type": "record",
			"name": "test",
			"fields": []any{
				map[string]any{"name": "name", "type": "string"},
				map[string]any{"name": "count", "type": "int", "default": float64(0)},
			},
		}
		data := map[string]any{"name": "alice"}
		result, err := normalizeForAvroSchema(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, "alice", m["name"])
		_, exists := m["count"]
		assert.False(t, exists, "field with default should be omitted for goavro")
	})

	t.Run("missing nullable union field fills nil", func(t *testing.T) {
		s := map[string]any{
			"type": "record",
			"name": "test",
			"fields": []any{
				map[string]any{"name": "name", "type": "string"},
				map[string]any{"name": "nick", "type": []any{"null", "string"}, "default": nil},
			},
		}
		data := map[string]any{"name": "alice"}
		result, err := normalizeForAvroSchema(data, s, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Nil(t, m["nick"])
	})

	t.Run("wrong type errors", func(t *testing.T) {
		_, err := normalizeForAvroSchema("not a map", schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map for Avro record")
	})
}

// --- Array ---

func TestNormalizeAvroArray(t *testing.T) {
	schema := map[string]any{"type": "array", "items": "int"}

	t.Run("normalizes elements", func(t *testing.T) {
		data := []any{float64(1), float64(2), float64(3)}
		result, err := normalizeForAvroSchema(data, schema, true)
		require.NoError(t, err)
		arr := result.([]any)
		assert.Equal(t, int32(1), arr[0])
		assert.Equal(t, int32(2), arr[1])
		assert.Equal(t, int32(3), arr[2])
	})

	t.Run("wrong type errors", func(t *testing.T) {
		_, err := normalizeForAvroSchema("not a slice", schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected slice for Avro array")
	})
}

// --- Map ---

func TestNormalizeAvroMap(t *testing.T) {
	schema := map[string]any{"type": "map", "values": "long"}

	t.Run("normalizes values", func(t *testing.T) {
		data := map[string]any{"a": float64(100), "b": json.Number("200")}
		result, err := normalizeForAvroSchema(data, schema, true)
		require.NoError(t, err)
		m := result.(map[string]any)
		assert.Equal(t, int64(100), m["a"])
		assert.Equal(t, int64(200), m["b"])
	})

	t.Run("wrong type errors", func(t *testing.T) {
		_, err := normalizeForAvroSchema(42, schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected map for Avro map")
	})
}

// --- Enum ---

func TestNormalizeAvroEnum(t *testing.T) {
	schema := map[string]any{"type": "enum", "name": "Color", "symbols": []any{"RED", "GREEN"}}

	t.Run("string passthrough", func(t *testing.T) {
		result, err := normalizeForAvroSchema("RED", schema, true)
		require.NoError(t, err)
		assert.Equal(t, "RED", result)
	})

	t.Run("wrong type errors", func(t *testing.T) {
		_, err := normalizeForAvroSchema(42, schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected string for Avro enum")
	})
}

// --- Union ---

func TestNormalizeAvroUnion(t *testing.T) {
	t.Run("rawJSON wraps first matching branch", func(t *testing.T) {
		schema := []any{"null", "string", "int"}
		result, err := normalizeForAvroSchema("hello", schema, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "hello"}, result)
	})

	t.Run("rawJSON numeric matches int branch", func(t *testing.T) {
		schema := []any{"null", "string", "int"}
		result, err := normalizeForAvroSchema(float64(42), schema, true)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"int": int32(42)}, result)
	})

	t.Run("nil returns nil", func(t *testing.T) {
		schema := []any{"null", "string"}
		result, err := normalizeForAvroSchema(nil, schema, true)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("no matching branch errors", func(t *testing.T) {
		schema := []any{"null", "int"}
		_, err := normalizeForAvroSchema("not a number", schema, true)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no union branch matched")
	})

	t.Run("non-rawJSON pre-wrapped", func(t *testing.T) {
		schema := []any{"null", "string"}
		result, err := normalizeForAvroSchema(map[string]any{"string": "hello"}, schema, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"string": "hello"}, result)
	})

	t.Run("non-rawJSON pre-wrapped coerces inner value", func(t *testing.T) {
		schema := []any{"null", "int"}
		result, err := normalizeForAvroSchema(map[string]any{"int": float64(7)}, schema, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"int": int32(7)}, result)
	})

	t.Run("non-rawJSON unknown key passes through", func(t *testing.T) {
		schema := []any{"null", "int"}
		result, err := normalizeForAvroSchema(map[string]any{"long": float64(7)}, schema, false)
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"long": float64(7)}, result)
	})

	t.Run("timestamp-millis in union uses logical type key", func(t *testing.T) {
		tsSchema := map[string]any{"type": "long", "logicalType": "timestamp-millis"}
		schema := []any{"null", tsSchema}
		result, err := normalizeForAvroSchema("2026-03-19T10:00:00Z", schema, true)
		require.NoError(t, err)
		wrapped := result.(map[string]any)
		key := "long.timestamp-millis"
		inner, ok := wrapped[key]
		require.True(t, ok, "expected key %q in %v", key, wrapped)
		assert.IsType(t, time.Time{}, inner)
	})
}

// --- avroSchemaTypeName ---

func TestAvroSchemaTypeName(t *testing.T) {
	tests := []struct {
		name     string
		schema   any
		expected string
	}{
		{"primitive string", "string", "string"},
		{"primitive int", "int", "int"},
		{"primitive null", "null", "null"},
		{"record no namespace", map[string]any{"type": "record", "name": "Foo"}, "Foo"},
		{"record with namespace", map[string]any{"type": "record", "name": "Foo", "namespace": "com.example"}, "com.example.Foo"},
		{"enum no namespace", map[string]any{"type": "enum", "name": "Color"}, "Color"},
		{"enum with namespace", map[string]any{"type": "enum", "name": "Color", "namespace": "com.example"}, "com.example.Color"},
		{"array", map[string]any{"type": "array", "items": "string"}, "array"},
		{"map", map[string]any{"type": "map", "values": "string"}, "map"},
		{"logical type", map[string]any{"type": "long", "logicalType": "timestamp-millis"}, "long.timestamp-millis"},
		{"logical type time-millis", map[string]any{"type": "int", "logicalType": "time-millis"}, "int.time-millis"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, avroSchemaTypeName(tc.schema))
		})
	}
}

// --- avroFieldTypeSchema ---

func TestAvroFieldTypeSchema(t *testing.T) {
	t.Run("simple type returns string", func(t *testing.T) {
		fd := map[string]any{"name": "x", "type": "string"}
		assert.Equal(t, "string", avroFieldTypeSchema(fd))
	})

	t.Run("nested complex type returns nested object", func(t *testing.T) {
		inner := map[string]any{"type": "record", "name": "inner", "fields": []any{}}
		fd := map[string]any{"name": "x", "type": inner}
		assert.Equal(t, inner, avroFieldTypeSchema(fd))
	})

	t.Run("flat map returns whole field def", func(t *testing.T) {
		fd := map[string]any{"name": "x", "type": "map", "values": "long"}
		assert.Equal(t, fd, avroFieldTypeSchema(fd))
	})

	t.Run("flat array returns whole field def", func(t *testing.T) {
		fd := map[string]any{"name": "x", "type": "array", "items": "string"}
		assert.Equal(t, fd, avroFieldTypeSchema(fd))
	})

	t.Run("flat enum returns whole field def", func(t *testing.T) {
		fd := map[string]any{"name": "x", "type": "enum", "symbols": []any{"A", "B"}}
		assert.Equal(t, fd, avroFieldTypeSchema(fd))
	})

	t.Run("flat logical type returns whole field def", func(t *testing.T) {
		fd := map[string]any{"name": "x", "type": "int", "logicalType": "time-millis"}
		assert.Equal(t, fd, avroFieldTypeSchema(fd))
	})

	t.Run("union type returns union", func(t *testing.T) {
		union := []any{"null", "string"}
		fd := map[string]any{"name": "x", "type": union}
		assert.Equal(t, union, avroFieldTypeSchema(fd))
	})
}

// --- timeFromUnits ---

func TestTimeFromUnits(t *testing.T) {
	t.Run("millis precision", func(t *testing.T) {
		ts := timeFromUnits(1742378400000, time.Millisecond)
		expected := time.Date(2025, 3, 19, 10, 0, 0, 0, time.UTC)
		assert.True(t, expected.Equal(ts), "expected %v, got %v", expected, ts)
	})

	t.Run("micros precision no overflow", func(t *testing.T) {
		// 62135596800000000 microseconds — large value that would overflow
		// time.Duration if naively multiplied.
		ts := timeFromUnits(62135596800000000, time.Microsecond)
		expected := time.Unix(62135596800, 0).UTC()
		assert.True(t, expected.Equal(ts), "expected %v, got %v", expected, ts)
	})

	t.Run("millis with sub-second remainder", func(t *testing.T) {
		ts := timeFromUnits(1742378400123, time.Millisecond)
		assert.Equal(t, 123000000, ts.Nanosecond())
	})
}

// --- decimalFromRawBytes ---

func TestDecimalFromRawBytes(t *testing.T) {
	t.Run("positive value", func(t *testing.T) {
		// 0x21 = 33, scale 2 → 33/100 = 0.33
		r := decimalFromRawBytes([]byte{0x21}, 2)
		f, _ := r.Float64()
		assert.InDelta(t, 0.33, f, 0.001)
	})

	t.Run("negative value", func(t *testing.T) {
		// 0xFF = -1 in two's complement, scale 2 → -1/100 = -0.01
		r := decimalFromRawBytes([]byte{0xFF}, 2)
		f, _ := r.Float64()
		assert.InDelta(t, -0.01, f, 0.001)
	})

	t.Run("multi-byte positive", func(t *testing.T) {
		// 0x01, 0x00 = 256, scale 2 → 256/100 = 2.56
		r := decimalFromRawBytes([]byte{0x01, 0x00}, 2)
		f, _ := r.Float64()
		assert.InDelta(t, 2.56, f, 0.001)
	})

	t.Run("empty bytes is zero", func(t *testing.T) {
		r := decimalFromRawBytes([]byte{}, 2)
		f, _ := r.Float64()
		assert.Equal(t, float64(0), f)
	})
}

// --- Round-trip through goavro ---

func TestNormalizeForAvroSchemaRoundTrip(t *testing.T) {
	schemaJSON := `{
		"type": "record",
		"name": "AllTypes",
		"fields": [
			{"name": "s", "type": "string"},
			{"name": "i32", "type": "int"},
			{"name": "i64", "type": "long"},
			{"name": "f32", "type": "float"},
			{"name": "f64", "type": "double"},
			{"name": "b", "type": "boolean"},
			{"name": "blob", "type": "bytes"},
			{"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
			{"name": "opt_s", "type": ["null", "string"], "default": null},
			{"name": "opt_null", "type": ["null", "string"], "default": null},
			{"name": "arr", "type": {"type": "array", "items": "int"}},
			{"name": "m", "type": {"type": "map", "values": "string"}},
			{"name": "nested", "type": {"type": "record", "name": "Inner", "fields": [
				{"name": "x", "type": "int"},
				{"name": "y", "type": "string"}
			]}}
		]
	}`

	var parsedSchema any
	require.NoError(t, json.Unmarshal([]byte(schemaJSON), &parsedSchema))

	codec, err := goavro.NewCodecForStandardJSONFull(schemaJSON)
	require.NoError(t, err)

	ts := time.Date(2026, 3, 19, 10, 0, 0, 0, time.UTC)

	data := map[string]any{
		"s":        "hello",
		"i32":      float64(42),
		"i64":      float64(9876543210),
		"f32":      float64(1.5),
		"f64":      float64(3.14159),
		"b":        true,
		"blob":     "binary",
		"ts":       "2026-03-19T10:00:00Z",
		"opt_s":    "present",
		"opt_null": nil,
		"arr":      []any{float64(1), float64(2)},
		"m":        map[string]any{"env": "prod"},
		"nested":   map[string]any{"x": float64(7), "y": "inner"},
	}

	normalized, err := normalizeForAvroSchema(data, parsedSchema, true)
	require.NoError(t, err)

	binary, err := codec.BinaryFromNative(nil, normalized)
	require.NoError(t, err)
	require.NotEmpty(t, binary)

	native, _, err := codec.NativeFromBinary(binary)
	require.NoError(t, err)
	m := native.(map[string]any)

	assert.Equal(t, "hello", m["s"])
	assert.Equal(t, int32(42), m["i32"])
	assert.Equal(t, int64(9876543210), m["i64"])
	assert.InDelta(t, 1.5, m["f32"], 0.01)
	assert.InDelta(t, 3.14159, m["f64"], 0.0001)
	assert.Equal(t, true, m["b"])
	assert.Equal(t, []byte("binary"), m["blob"])

	// Non-optional timestamp decodes directly as time.Time.
	decodedTs := m["ts"].(time.Time)
	assert.True(t, ts.Equal(decodedTs))

	assert.Equal(t, map[string]any{"string": "present"}, m["opt_s"])
	assert.Nil(t, m["opt_null"])

	arr := m["arr"].([]any)
	assert.Len(t, arr, 2)
	assert.Equal(t, int32(1), arr[0])

	mp := m["m"].(map[string]any)
	assert.Equal(t, "prod", mp["env"])

	nested := m["nested"].(map[string]any)
	assert.Equal(t, int32(7), nested["x"])
	assert.Equal(t, "inner", nested["y"])
}
