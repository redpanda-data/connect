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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestCoercer(t *testing.T, schemaStr string, refs map[string]string) *jsonSchemaCoercer {
	t.Helper()
	c, err := newJSONSchemaCoercer(schemaStr, refs)
	require.NoError(t, err)
	return c
}

func mustCoerce(t *testing.T, schemaStr, dataStr string) any {
	t.Helper()
	out, err := newTestCoercer(t, schemaStr, nil).coerceMessage([]byte(dataStr))
	require.NoError(t, err)
	return out
}

func coerceErr(t *testing.T, schemaStr, dataStr string) error {
	t.Helper()
	_, err := newTestCoercer(t, schemaStr, nil).coerceMessage([]byte(dataStr))
	require.Error(t, err)
	return err
}

//------------------------------------------------------------------------------
// Scalars and nullability

func TestCoerceScalars(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   string
		want   any
	}{
		{
			name:   "integer to int64",
			schema: `{"type":"object","properties":{"n":{"type":"integer"}}}`,
			data:   `{"n":42}`,
			want:   map[string]any{"n": int64(42)},
		},
		{
			name:   "large integer keeps precision",
			schema: `{"type":"object","properties":{"n":{"type":"integer"}}}`,
			// 9007199254740993 == 2^53 + 1, not representable as a float64.
			data: `{"n":9007199254740993}`,
			want: map[string]any{"n": int64(9007199254740993)},
		},
		{
			name:   "negative integer",
			schema: `{"type":"object","properties":{"n":{"type":"integer"}}}`,
			data:   `{"n":-17}`,
			want:   map[string]any{"n": int64(-17)},
		},
		{
			name:   "max int64 round-trips",
			schema: `{"type":"object","properties":{"n":{"type":"integer"}}}`,
			data:   `{"n":9223372036854775807}`,
			want:   map[string]any{"n": int64(9223372036854775807)},
		},
		{
			name:   "number stays float64",
			schema: `{"type":"object","properties":{"n":{"type":"number"}}}`,
			data:   `{"n":3.5}`,
			want:   map[string]any{"n": float64(3.5)},
		},
		{
			name:   "integer-valued number stays float64",
			schema: `{"type":"object","properties":{"n":{"type":"number"}}}`,
			data:   `{"n":3}`,
			want:   map[string]any{"n": float64(3)},
		},
		{
			name:   "boolean",
			schema: `{"type":"object","properties":{"b":{"type":"boolean"}}}`,
			data:   `{"b":true}`,
			want:   map[string]any{"b": true},
		},
		{
			name:   "plain string",
			schema: `{"type":"object","properties":{"s":{"type":"string"}}}`,
			data:   `{"s":"hello"}`,
			want:   map[string]any{"s": "hello"},
		},
		{
			name:   "date string stays string",
			schema: `{"type":"object","properties":{"d":{"type":"string","format":"date"}}}`,
			data:   `{"d":"2021-01-02"}`,
			want:   map[string]any{"d": "2021-01-02"},
		},
		{
			name:   "uuid string stays string",
			schema: `{"type":"object","properties":{"u":{"type":"string","format":"uuid"}}}`,
			data:   `{"u":"00000000-0000-0000-0000-000000000000"}`,
			want:   map[string]any{"u": "00000000-0000-0000-0000-000000000000"},
		},
		{
			name:   "explicit null type",
			schema: `{"type":"object","properties":{"n":{"type":"null"}}}`,
			data:   `{"n":null}`,
			want:   map[string]any{"n": nil},
		},
		{
			name:   "nullable integer with null value",
			schema: `{"type":"object","properties":{"n":{"type":["integer","null"]}}}`,
			data:   `{"n":null}`,
			want:   map[string]any{"n": nil},
		},
		{
			name:   "nullable integer with value",
			schema: `{"type":"object","properties":{"n":{"type":["integer","null"]}}}`,
			data:   `{"n":8}`,
			want:   map[string]any{"n": int64(8)},
		},
		{
			name:   "null-first type union picks string",
			schema: `{"type":"object","properties":{"s":{"type":["null","string"]}}}`,
			data:   `{"s":"x"}`,
			want:   map[string]any{"s": "x"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, mustCoerce(t, tc.schema, tc.data))
		})
	}
}

func TestCoerceDateTime(t *testing.T) {
	out := mustCoerce(t,
		`{"type":"object","properties":{"ts":{"type":"string","format":"date-time"}}}`,
		`{"ts":"2021-01-02T15:04:05Z"}`)

	m, ok := out.(map[string]any)
	require.True(t, ok)
	ts, ok := m["ts"].(time.Time)
	require.True(t, ok, "expected time.Time, got %T", m["ts"])
	assert.Equal(t, time.Date(2021, 1, 2, 15, 4, 5, 0, time.UTC), ts.UTC())
}

//------------------------------------------------------------------------------
// Structures: objects, arrays, additional/unknown properties

func TestCoerceNestedObject(t *testing.T) {
	out := mustCoerce(t,
		`{"type":"object","properties":{"inner":{"type":"object","properties":{"count":{"type":"integer"}}}}}`,
		`{"inner":{"count":11}}`)
	assert.Equal(t, map[string]any{"inner": map[string]any{"count": int64(11)}}, out)
}

func TestCoerceArrays(t *testing.T) {
	t.Run("array of integers", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"vals":{"type":"array","items":{"type":"integer"}}}}`,
			`{"vals":[1,2,3]}`)
		assert.Equal(t, map[string]any{"vals": []any{int64(1), int64(2), int64(3)}}, out)
	})

	t.Run("array of objects with date-time", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"array","items":{"type":"object","properties":{"id":{"type":"integer"},"at":{"type":"string","format":"date-time"}}}}`,
			`[{"id":1,"at":"2021-01-02T15:04:05Z"}]`)
		arr, ok := out.([]any)
		require.True(t, ok)
		require.Len(t, arr, 1)
		m := arr[0].(map[string]any)
		assert.Equal(t, int64(1), m["id"])
		assert.IsType(t, time.Time{}, m["at"])
	})

	t.Run("tuple items with extra passthrough", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"array","items":[{"type":"integer"},{"type":"string"}]}`,
			`[1,"two",3.5]`)
		// First two coerced positionally; the extra element falls through with
		// the number normalised to float64.
		assert.Equal(t, []any{int64(1), "two", float64(3.5)}, out)
	})

	t.Run("array without items schema passes through", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"array"}`,
			`[1,2.5,"x"]`)
		assert.Equal(t, []any{float64(1), float64(2.5), "x"}, out)
	})
}

func TestCoerceAdditionalProperties(t *testing.T) {
	t.Run("schema coerces extra props", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"known":{"type":"string"}},"additionalProperties":{"type":"integer"}}`,
			`{"known":"a","extra":5}`)
		assert.Equal(t, map[string]any{"known": "a", "extra": int64(5)}, out)
	})

	t.Run("false leaves unknown props as decoded float", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"known":{"type":"integer"}},"additionalProperties":false}`,
			`{"known":1,"extra":2}`)
		assert.Equal(t, map[string]any{"known": int64(1), "extra": float64(2)}, out)
	})

	t.Run("absent leaves unknown props as decoded float", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"known":{"type":"integer"}}}`,
			`{"known":1,"extra":2}`)
		assert.Equal(t, map[string]any{"known": int64(1), "extra": float64(2)}, out)
	})
}

func TestCoerceUntypedPassthrough(t *testing.T) {
	t.Run("empty schema scalar", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"any":{}}}`,
			`{"any":7}`)
		assert.Equal(t, map[string]any{"any": float64(7)}, out)
	})

	t.Run("empty schema nested container", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"any":{}}}`,
			`{"any":{"a":[1,2],"b":3}}`)
		assert.Equal(t, map[string]any{"any": map[string]any{
			"a": []any{float64(1), float64(2)},
			"b": float64(3),
		}}, out)
	})
}

func TestCoerceDefaults(t *testing.T) {
	t.Run("scalar default applied and coerced", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer","default":7}}}`,
			`{"a":1}`)
		assert.Equal(t, map[string]any{"a": int64(1), "b": int64(7)}, out)
	})

	t.Run("date-time default coerced to time", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"ts":{"type":"string","format":"date-time","default":"2020-06-01T00:00:00Z"}}}`,
			`{}`)
		m := out.(map[string]any)
		assert.IsType(t, time.Time{}, m["ts"])
	})

	t.Run("present value not overridden by default", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"b":{"type":"integer","default":7}}}`,
			`{"b":3}`)
		assert.Equal(t, map[string]any{"b": int64(3)}, out)
	})
}

//------------------------------------------------------------------------------
// Combinators: allOf / oneOf / anyOf

func TestCoerceAllOf(t *testing.T) {
	t.Run("merges object properties across subschemas", func(t *testing.T) {
		out := mustCoerce(t,
			`{"allOf":[{"type":"object","properties":{"a":{"type":"integer"}}},{"type":"object","properties":{"b":{"type":"integer"}}}]}`,
			`{"a":1,"b":2}`)
		assert.Equal(t, map[string]any{"a": int64(1), "b": int64(2)}, out)
	})

	t.Run("refines a scalar", func(t *testing.T) {
		out := mustCoerce(t,
			`{"type":"object","properties":{"n":{"allOf":[{"type":"integer"},{"minimum":0}]}}}`,
			`{"n":5}`)
		assert.Equal(t, map[string]any{"n": int64(5)}, out)
	})
}

func TestCoerceOneOf(t *testing.T) {
	schema := `{"type":"object","properties":{"v":{"oneOf":[{"type":"integer"},{"type":"string"}]}}}`

	t.Run("integer branch", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": int64(5)}, mustCoerce(t, schema, `{"v":5}`))
	})
	t.Run("string branch", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": "hello"}, mustCoerce(t, schema, `{"v":"hello"}`))
	})
}

func TestCoerceAnyOf(t *testing.T) {
	t.Run("scalar branches", func(t *testing.T) {
		schema := `{"type":"object","properties":{"v":{"anyOf":[{"type":"integer"},{"type":"string"}]}}}`
		assert.Equal(t, map[string]any{"v": int64(9)}, mustCoerce(t, schema, `{"v":9}`))
	})

	t.Run("object branch exercises validation of nested data", func(t *testing.T) {
		schema := `{"anyOf":[{"type":"object","properties":{"x":{"type":"integer"}}},{"type":"string"}]}`
		out := mustCoerce(t, schema, `{"x":5}`)
		assert.Equal(t, map[string]any{"x": int64(5)}, out)
	})
}

func TestCoerceBranchNoMatch(t *testing.T) {
	// A boolean matches neither the integer nor the string branch.
	err := coerceErr(t,
		`{"type":"object","properties":{"v":{"oneOf":[{"type":"integer"},{"type":"string"}]}}}`,
		`{"v":true}`)
	assert.Contains(t, err.Error(), "does not match any oneOf branch")
}

//------------------------------------------------------------------------------
// References

func TestCoerceRef(t *testing.T) {
	c := newTestCoercer(t,
		`{"type":"object","properties":{"amount":{"$ref":"Money"}}}`,
		map[string]string{"Money": `{"type":"integer"}`})
	out, err := c.coerceMessage([]byte(`{"amount":100}`))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"amount": int64(100)}, out)
}

func TestCoerceLocalRef(t *testing.T) {
	out := mustCoerce(t,
		`{"type":"object","definitions":{"Id":{"type":"integer"}},"properties":{"id":{"$ref":"#/definitions/Id"}}}`,
		`{"id":99}`)
	assert.Equal(t, map[string]any{"id": int64(99)}, out)
}

func TestCoerceExternalRefWithFragment(t *testing.T) {
	c := newTestCoercer(t,
		`{"type":"object","properties":{"id":{"$ref":"common#/definitions/Id"}}}`,
		map[string]string{"common": `{"definitions":{"Id":{"type":"integer"}}}`})
	out, err := c.coerceMessage([]byte(`{"id":7}`))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"id": int64(7)}, out)
}

func TestCoercePointerEscapes(t *testing.T) {
	// A definition key containing a slash must be referenced via the ~1 escape.
	out := mustCoerce(t,
		`{"type":"object","definitions":{"a/b":{"type":"integer"}},"properties":{"id":{"$ref":"#/definitions/a~1b"}}}`,
		`{"id":4}`)
	assert.Equal(t, map[string]any{"id": int64(4)}, out)
}

func TestCoerceRecursiveRefTerminates(t *testing.T) {
	// A self-referential schema with finite data must terminate and coerce at
	// every level without tripping the depth guard.
	c := newTestCoercer(t,
		`{"$ref":"Node"}`,
		map[string]string{"Node": `{"type":"object","properties":{"val":{"type":"integer"},"next":{"$ref":"Node"}}}`})
	out, err := c.coerceMessage([]byte(`{"val":1,"next":{"val":2,"next":{"val":3}}}`))
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"val": int64(1),
		"next": map[string]any{
			"val":  int64(2),
			"next": map[string]any{"val": int64(3)},
		},
	}, out)
}

func TestCoerceCyclicRefErrors(t *testing.T) {
	// A reference that resolves to itself with no data progress must fail with a
	// clear error rather than overflowing the stack.
	c := newTestCoercer(t,
		`{"$ref":"Node"}`,
		map[string]string{"Node": `{"$ref":"Node"}`})
	_, err := c.coerceMessage([]byte(`{"anything":1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cyclic reference")
}

//------------------------------------------------------------------------------
// Error and edge paths

func TestCoerceErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		data   string
	}{
		{"non-integral for integer", `{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":1.5}`},
		{"overflow for integer", `{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":1e30}`},
		// 9223372036854775808 == 2^63 == math.MaxInt64 + 1: must error, not wrap.
		{"overflow at 2^63 boundary", `{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":9223372036854775808}`},
		{"string for integer", `{"type":"object","properties":{"n":{"type":"integer"}}}`, `{"n":"abc"}`},
		{"bool for number", `{"type":"object","properties":{"n":{"type":"number"}}}`, `{"n":true}`},
		{"number for boolean", `{"type":"object","properties":{"b":{"type":"boolean"}}}`, `{"b":1}`},
		{"number for string", `{"type":"object","properties":{"s":{"type":"string"}}}`, `{"s":5}`},
		{"scalar for object", `{"type":"object","properties":{"o":{"type":"object","properties":{"x":{"type":"integer"}}}}}`, `{"o":5}`},
		{"scalar for array", `{"type":"object","properties":{"a":{"type":"array","items":{"type":"integer"}}}}`, `{"a":5}`},
		{"bad date-time", `{"type":"object","properties":{"ts":{"type":"string","format":"date-time"}}}`, `{"ts":"not-a-date"}`},
		{"unresolved ref", `{"$ref":"Missing"}`, `{"x":1}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_ = coerceErr(t, tc.schema, tc.data)
		})
	}
}

func TestCoerceMessageDecodeError(t *testing.T) {
	_, err := newTestCoercer(t, `{"type":"object"}`, nil).coerceMessage([]byte(`{not valid json`))
	require.Error(t, err)
}

func TestCoerceRootLevelScalar(t *testing.T) {
	out := mustCoerce(t, `{"type":"integer"}`, `42`)
	assert.Equal(t, int64(42), out)
}

//------------------------------------------------------------------------------
// Constructor

func TestNewJSONSchemaCoercerErrors(t *testing.T) {
	t.Run("invalid root schema", func(t *testing.T) {
		_, err := newJSONSchemaCoercer(`{not json`, nil)
		require.Error(t, err)
	})
	t.Run("invalid reference schema", func(t *testing.T) {
		_, err := newJSONSchemaCoercer(`{"type":"object"}`, map[string]string{"bad": `{not json`})
		require.Error(t, err)
	})
}

//------------------------------------------------------------------------------
// Helper-level unit tests for branches not reachable via JSON input

func TestNodeType(t *testing.T) {
	tests := []struct {
		name      string
		node      map[string]any
		wantTypes []string
		wantNull  bool
	}{
		{"string type", map[string]any{"type": "integer"}, []string{"integer"}, false},
		{"nullable union", map[string]any{"type": []any{"integer", "null"}}, []string{"integer"}, true},
		{"null-first union", map[string]any{"type": []any{"null", "string"}}, []string{"string"}, true},
		{"multi-type union", map[string]any{"type": []any{"integer", "string"}}, []string{"integer", "string"}, false},
		{"no type", map[string]any{}, nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			types, nullable := nodeType(tc.node)
			assert.Equal(t, tc.wantTypes, types)
			assert.Equal(t, tc.wantNull, nullable)
		})
	}
}

func TestCoerceMultiTypeUnion(t *testing.T) {
	intOrStr := `{"type":"object","properties":{"v":{"type":["integer","string"]}}}`
	numOrInt := `{"type":"object","properties":{"v":{"type":["number","integer"]}}}`

	t.Run("string value picks string", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": "hi"}, mustCoerce(t, intOrStr, `{"v":"hi"}`))
	})
	t.Run("integer value picks integer", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": int64(5)}, mustCoerce(t, intOrStr, `{"v":5}`))
	})
	t.Run("integral value prefers integer over number", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": int64(5)}, mustCoerce(t, numOrInt, `{"v":5}`))
	})
	t.Run("fractional value falls to number", func(t *testing.T) {
		assert.Equal(t, map[string]any{"v": float64(5.5)}, mustCoerce(t, numOrInt, `{"v":5.5}`))
	})
	t.Run("kind with no matching type passes through", func(t *testing.T) {
		// A boolean matches neither integer nor string; it is left untouched
		// rather than forced (and failing) into a coercion.
		assert.Equal(t, map[string]any{"v": true}, mustCoerce(t, intOrStr, `{"v":true}`))
	})
}

func TestFloatToInt64(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		v, err := floatToInt64(42)
		require.NoError(t, err)
		assert.Equal(t, int64(42), v)
	})
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1.5, 1e30, -1e30} {
		_, err := floatToInt64(f)
		assert.Error(t, err, "expected error for %v", f)
	}
}

func TestToInt64(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want int64
	}{
		{json.Number("9"), 9},
		{int(5), 5},
		{int64(7), 7},
		{float64(3), 3},
	} {
		v, err := toInt64(tc.in)
		require.NoError(t, err)
		assert.Equal(t, tc.want, v)
	}
	_, err := toInt64(true)
	assert.Error(t, err)
}

func TestToFloat64(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want float64
	}{
		{json.Number("3.5"), 3.5},
		{json.Number("3"), 3},
		{float64(2), 2},
		{float32(2), 2},
		{int(5), 5},
		{int64(6), 6},
	} {
		v, err := toFloat64(tc.in)
		require.NoError(t, err)
		assert.Equal(t, tc.want, v)
	}
	_, err := toFloat64("x")
	assert.Error(t, err)
}

//------------------------------------------------------------------------------
// Second round: defensive branches, $ref-in-combinator, malformed schemas.

func TestCoerceBranchWithRef(t *testing.T) {
	t.Run("oneOf branch is a $ref", func(t *testing.T) {
		c := newTestCoercer(t,
			`{"oneOf":[{"$ref":"IntType"},{"type":"string"}]}`,
			map[string]string{"IntType": `{"type":"integer"}`})
		out, err := c.coerceMessage([]byte(`5`))
		require.NoError(t, err)
		assert.Equal(t, int64(5), out)
	})

	t.Run("oneOf branch $ref is unresolved", func(t *testing.T) {
		c := newTestCoercer(t, `{"oneOf":[{"$ref":"Missing"}]}`, nil)
		_, err := c.coerceMessage([]byte(`5`))
		require.Error(t, err)
	})

	t.Run("anyOf with array data normalises and coerces", func(t *testing.T) {
		out := mustCoerce(t,
			`{"anyOf":[{"type":"array","items":{"type":"integer"}},{"type":"string"}]}`,
			`[1,2]`)
		assert.Equal(t, []any{int64(1), int64(2)}, out)
	})
}

func TestCoerceBranchInvalidSchema(t *testing.T) {
	// A oneOf branch that is not a valid JSON Schema (type must be a string or
	// array, not a number) surfaces as an error from the validation step.
	err := coerceErr(t,
		`{"type":"object","properties":{"v":{"oneOf":[{"type":12345}]}}}`,
		`{"v":1}`)
	require.Error(t, err)
}

func TestCoerceMalformedCombinators(t *testing.T) {
	// oneOf/allOf declared as a non-array are malformed; the coercer passes the
	// value through rather than panicking.
	t.Run("oneOf not an array", func(t *testing.T) {
		assert.Equal(t, float64(5), mustCoerce(t, `{"oneOf":{"type":"integer"}}`, `5`))
	})
	t.Run("allOf not an array", func(t *testing.T) {
		assert.Equal(t, float64(5), mustCoerce(t, `{"allOf":{"type":"integer"}}`, `5`))
	})
}

func TestCoerceUnknownTypePassthrough(t *testing.T) {
	// An unrecognised type keyword falls through to passthrough.
	out := mustCoerce(t,
		`{"type":"object","properties":{"x":{"type":"frobnicate"}}}`,
		`{"x":5}`)
	assert.Equal(t, map[string]any{"x": float64(5)}, out)
}

func TestCoerceEmptyRefErrors(t *testing.T) {
	err := coerceErr(t,
		`{"type":"object","properties":{"id":{"$ref":""}}}`,
		`{"id":1}`)
	assert.Contains(t, err.Error(), "empty $ref")
}

func TestResolvePointerErrors(t *testing.T) {
	tests := []struct {
		name   string
		schema string
	}{
		{
			name:   "token not found",
			schema: `{"type":"object","properties":{"id":{"$ref":"#/definitions/Missing"}}}`,
		},
		{
			name:   "intermediate is not an object",
			schema: `{"title":"X","type":"object","properties":{"id":{"$ref":"#/title/x"}}}`,
		},
		{
			name:   "ref does not point to a schema object",
			schema: `{"title":"X","type":"object","properties":{"id":{"$ref":"#/title"}}}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_ = coerceErr(t, tc.schema, `{"id":1}`)
		})
	}
}

//------------------------------------------------------------------------------
// Regression: the originating Old Mutual Payment schema.

func TestCoercePaymentRegression(t *testing.T) {
	schema := `{"type":"object","title":"Payment","properties":{"payment_id":{"type":"string"},"from_account":{"type":"string"},"to_account":{"type":"string"},"amount_cents":{"type":"integer"},"currency":{"type":"string"},"status":{"type":"string","enum":["PENDING","COMPLETED","FAILED"]},"initiated_at":{"type":"integer","description":"epoch millis"}},"required":["payment_id","from_account","to_account","amount_cents","currency","status","initiated_at"]}`

	data := `{"payment_id":"p-1","from_account":"a","to_account":"b","amount_cents":123456789012345,"currency":"ZAR","status":"COMPLETED","initiated_at":1777990438791}`

	out := mustCoerce(t, schema, data)
	m, ok := out.(map[string]any)
	require.True(t, ok)

	assert.IsType(t, int64(0), m["amount_cents"])
	assert.Equal(t, int64(123456789012345), m["amount_cents"])
	assert.IsType(t, int64(0), m["initiated_at"])
	assert.Equal(t, int64(1777990438791), m["initiated_at"])
	assert.Equal(t, "COMPLETED", m["status"])
}
