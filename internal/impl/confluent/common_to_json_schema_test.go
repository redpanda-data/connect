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
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func jsonSchemaUnmarshal(t *testing.T, c schema.Common) map[string]any {
	t.Helper()
	out, err := commonToJSONSchema(c)
	require.NoError(t, err)
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	return result
}

func TestCommonToJSONSchemaPrimitives(t *testing.T) {
	tests := []struct {
		ct       schema.CommonType
		wantType string
	}{
		{schema.Int32, "integer"},
		{schema.Int64, "integer"},
		{schema.Float32, "number"},
		{schema.Float64, "number"},
		{schema.Boolean, "boolean"},
		{schema.String, "string"},
		{schema.Null, "null"},
	}
	for _, tt := range tests {
		t.Run(tt.wantType, func(t *testing.T) {
			got := jsonSchemaUnmarshal(t, schema.Common{Type: tt.ct})
			assert.Equal(t, tt.wantType, got["type"])
		})
	}
}

func TestCommonToJSONSchemaTimestamp(t *testing.T) {
	got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.Timestamp})
	assert.Equal(t, "string", got["type"])
	assert.Equal(t, "date-time", got["format"])
}

// TestCommonToJSONSchemaDateTimeUUID covers the three semantic types
// the schema-coverage audit flagged as missing. JSON Schema draft 2019-09
// defines `date`, `time`, and `uuid` format keywords; mapping the
// schema.Common variants to those keeps the encoder symmetric with the
// rest of the type matrix.
func TestCommonToJSONSchemaDateTimeUUID(t *testing.T) {
	t.Run("Date", func(t *testing.T) {
		got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.Date})
		assert.Equal(t, "string", got["type"])
		assert.Equal(t, "date", got["format"])
	})
	t.Run("TimeOfDay", func(t *testing.T) {
		got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.TimeOfDay})
		assert.Equal(t, "string", got["type"])
		assert.Equal(t, "time", got["format"])
	})
	t.Run("UUID", func(t *testing.T) {
		got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.UUID})
		assert.Equal(t, "string", got["type"])
		assert.Equal(t, "uuid", got["format"])
	})
}

func TestCommonToJSONSchemaByteArray(t *testing.T) {
	got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.ByteArray})
	assert.Equal(t, "string", got["type"])
	assert.Equal(t, "base64", got["contentEncoding"])
}

func TestCommonToJSONSchemaAny(t *testing.T) {
	got := jsonSchemaUnmarshal(t, schema.Common{Type: schema.Any})
	assert.Empty(t, got)
}

func TestCommonToJSONSchemaObjectRequired(t *testing.T) {
	c := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "id", Type: schema.Int32},
			{Name: "label", Type: schema.String},
			{Name: "note", Type: schema.String, Optional: true},
		},
	}
	got := jsonSchemaUnmarshal(t, c)
	assert.Equal(t, "object", got["type"])

	props := got["properties"].(map[string]any)
	assert.Contains(t, props, "id")
	assert.Contains(t, props, "label")
	assert.Contains(t, props, "note")

	required := got["required"].([]any)
	assert.ElementsMatch(t, []any{"id", "label"}, required)
}

func TestCommonToJSONSchemaObjectAllOptional(t *testing.T) {
	c := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "x", Type: schema.Int32, Optional: true},
			{Name: "y", Type: schema.Int32, Optional: true},
		},
	}
	got := jsonSchemaUnmarshal(t, c)
	_, hasRequired := got["required"]
	assert.False(t, hasRequired)
}

func TestCommonToJSONSchemaArray(t *testing.T) {
	c := schema.Common{Type: schema.Array, Children: []schema.Common{{Type: schema.String}}}
	got := jsonSchemaUnmarshal(t, c)
	assert.Equal(t, "array", got["type"])
	items := got["items"].(map[string]any)
	assert.Equal(t, "string", items["type"])
}

func TestCommonToJSONSchemaMapType(t *testing.T) {
	c := schema.Common{Type: schema.Map, Children: []schema.Common{{Type: schema.Int64}}}
	got := jsonSchemaUnmarshal(t, c)
	assert.Equal(t, "object", got["type"])
	addl := got["additionalProperties"].(map[string]any)
	assert.Equal(t, "integer", addl["type"])
}

func TestCommonToJSONSchemaUnion(t *testing.T) {
	c := schema.Common{Type: schema.Union, Children: []schema.Common{
		{Type: schema.String},
		{Type: schema.Int32},
	}}
	got := jsonSchemaUnmarshal(t, c)
	oneOf := got["oneOf"].([]any)
	require.Len(t, oneOf, 2)
	assert.Equal(t, "string", oneOf[0].(map[string]any)["type"])
	assert.Equal(t, "integer", oneOf[1].(map[string]any)["type"])
}

func TestCommonToJSONSchemaDecimal(t *testing.T) {
	cases := []struct {
		name      string
		precision int32
		scale     int32
		matches   []string
		rejects   []string
	}{
		{
			name:      "scale 0 integer-only",
			precision: 5,
			scale:     0,
			matches:   []string{"0", "1", "12345", "-12345"},
			rejects:   []string{"123456", "1.5", "01", "+1", "1e2", ""},
		},
		{
			name:      "scale 4 mixed",
			precision: 18,
			scale:     4,
			matches:   []string{"0.0000", "1.5000", "-12345.6789", "12345678901234.5678"},
			rejects:   []string{"1.5", "1.50000", "1", "01.5000", "+1.5000", "1.5e2", ""},
		},
		{
			name:      "scale equals precision fractional only",
			precision: 4,
			scale:     4,
			matches:   []string{"0.0000", "0.1234", "-0.9999"},
			rejects:   []string{"1.0000", "0.123", "0.12345", "1"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := schema.Common{
				Name:    "amount",
				Type:    schema.Decimal,
				Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: tc.precision, Scale: tc.scale}},
			}
			got := jsonSchemaUnmarshal(t, c)
			assert.Equal(t, "string", got["type"])
			pattern, ok := got["pattern"].(string)
			require.True(t, ok, "pattern field must be a string")

			re := regexp.MustCompile(pattern)
			for _, ok := range tc.matches {
				assert.True(t, re.MatchString(ok), "pattern %q should match %q", pattern, ok)
			}
			for _, bad := range tc.rejects {
				assert.False(t, re.MatchString(bad), "pattern %q should NOT match %q", pattern, bad)
			}
		})
	}
}

func TestCommonToJSONSchemaDecimalMissingLogical(t *testing.T) {
	_, err := commonToJSONSchema(schema.Common{Name: "amount", Type: schema.Decimal})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing precision/scale")
}

func TestCommonToJSONSchemaBigDecimal(t *testing.T) {
	c := schema.Common{Name: "amount", Type: schema.BigDecimal}
	got := jsonSchemaUnmarshal(t, c)
	assert.Equal(t, "string", got["type"])
	pattern, ok := got["pattern"].(string)
	require.True(t, ok)
	re := regexp.MustCompile(pattern)
	for _, s := range []string{"0", "1.5", "-1.5", "12345.67890", "1"} {
		assert.True(t, re.MatchString(s), "permissive pattern should match %q", s)
	}
	for _, s := range []string{"1.5e2", "+1.5", "01", "", "1.2.3"} {
		assert.False(t, re.MatchString(s), "permissive pattern should NOT match %q", s)
	}
}

func TestCommonToJSONSchemaOptionalAllowsNull(t *testing.T) {
	tests := []struct {
		name   string
		input  schema.Common
		want   string
		accept []string
		reject []string
	}{
		{
			name: "object property: optional accepts null, required does not",
			input: schema.Common{Type: schema.Object, Children: []schema.Common{
				{Name: "id", Type: schema.Int64},
				{Name: "name", Type: schema.String, Optional: true},
			}},
			want:   `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":["string","null"]}},"required":["id"]}`,
			accept: []string{`{"id":1}`, `{"id":1,"name":null}`, `{"id":1,"name":"x"}`},
			reject: []string{`{}`, `{"id":null}`, `{"id":1,"name":5}`},
		},
		{
			name:   "timestamp keeps format",
			input:  schema.Common{Type: schema.Timestamp, Optional: true},
			want:   `{"type":["string","null"],"format":"date-time"}`,
			accept: []string{`null`, `"2026-10-09T00:00:00Z"`},
			reject: []string{`5`},
		},
		{
			name:   "decimal keeps pattern",
			input:  schema.Common{Type: schema.Decimal, Optional: true, Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 10, Scale: 2}}},
			want:   `{"type":["string","null"],"pattern":"^-?(0|[1-9][0-9]{0,7})\\.[0-9]{2}$"}`,
			accept: []string{`null`, `"12.50"`},
			reject: []string{`"12.5"`},
		},
		{
			name:   "array with optional items",
			input:  schema.Common{Type: schema.Array, Optional: true, Children: []schema.Common{{Type: schema.String, Optional: true}}},
			want:   `{"type":["array","null"],"items":{"type":["string","null"]}}`,
			accept: []string{`null`, `["a",null]`},
			reject: []string{`[5]`},
		},
		{
			name:   "object keeps required children",
			input:  schema.Common{Type: schema.Object, Optional: true, Children: []schema.Common{{Name: "city", Type: schema.String}}},
			want:   `{"type":["object","null"],"properties":{"city":{"type":"string"}},"required":["city"]}`,
			accept: []string{`null`, `{"city":"c"}`},
			reject: []string{`{"city":null}`},
		},
		{
			name:   "union gets a null branch in its oneOf",
			input:  schema.Common{Type: schema.Union, Optional: true, Children: []schema.Common{{Type: schema.String}, {Type: schema.Int32}}},
			want:   `{"oneOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}`,
			accept: []string{`null`, `"x"`, `3`},
			reject: []string{`true`},
		},
		{
			name:   "union with a branch that accepts null is unchanged",
			input:  schema.Common{Type: schema.Union, Optional: true, Children: []schema.Common{{Type: schema.String, Optional: true}, {Type: schema.Int32}}},
			want:   `{"oneOf":[{"type":["string","null"]},{"type":"integer"}]}`,
			accept: []string{`null`, `"x"`, `3`},
			reject: []string{`true`},
		},
		{
			name:   "any is unchanged",
			input:  schema.Common{Type: schema.Any, Optional: true},
			want:   `{}`,
			accept: []string{`null`, `true`},
		},
		{
			name:   "null is unchanged",
			input:  schema.Common{Type: schema.Null, Optional: true},
			want:   `{"type":"null"}`,
			accept: []string{`null`},
			reject: []string{`"x"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commonToJSONSchema(tt.input)
			require.NoError(t, err)
			assert.JSONEq(t, tt.want, got)

			sch, err := gojsonschema.NewSchema(gojsonschema.NewStringLoader(got))
			require.NoError(t, err)
			for _, doc := range tt.accept {
				res, err := sch.Validate(gojsonschema.NewStringLoader(doc))
				require.NoError(t, err)
				assert.True(t, res.Valid(), "%s should be accepted: %v", doc, res.Errors())
			}
			for _, doc := range tt.reject {
				res, err := sch.Validate(gojsonschema.NewStringLoader(doc))
				require.NoError(t, err)
				assert.False(t, res.Valid(), "%s should be rejected", doc)
			}
		})
	}
}
