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
