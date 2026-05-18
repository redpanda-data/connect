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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func avroUnmarshal(t *testing.T, c schema.Common, recordName, namespace string) any {
	t.Helper()
	out, err := commonToAvroSchema(c, recordName, namespace)
	require.NoError(t, err)
	var result any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	return result
}

func TestCommonToAvroPrimitives(t *testing.T) {
	tests := []struct {
		ct   schema.CommonType
		want string
	}{
		{schema.Boolean, "boolean"},
		{schema.Int32, "int"},
		{schema.Int64, "long"},
		{schema.Float32, "float"},
		{schema.Float64, "double"},
		{schema.String, "string"},
		{schema.ByteArray, "bytes"},
		{schema.Null, "null"},
		{schema.Any, "bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := avroUnmarshal(t, schema.Common{Type: tt.ct}, "", "")
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCommonToAvroTimestamp(t *testing.T) {
	got := avroUnmarshal(t, schema.Common{Type: schema.Timestamp}, "", "")
	m := got.(map[string]any)
	assert.Equal(t, "long", m["type"])
	assert.Equal(t, "timestamp-millis", m["logicalType"])
}

func TestCommonToAvroOptional(t *testing.T) {
	got := avroUnmarshal(t, schema.Common{Type: schema.String, Optional: true}, "", "")
	arr := got.([]any)
	assert.Equal(t, []any{"null", "string"}, arr)
}

func TestCommonToAvroRecord(t *testing.T) {
	c := schema.Common{
		Type: schema.Object,
		Name: "MyRecord",
		Children: []schema.Common{
			{Name: "id", Type: schema.Int32},
			{Name: "name", Type: schema.String},
		},
	}
	got := avroUnmarshal(t, c, "fallback", "").(map[string]any)
	assert.Equal(t, "record", got["type"])
	assert.Equal(t, "MyRecord", got["name"])

	fields := got["fields"].([]any)
	require.Len(t, fields, 2)
	assert.Equal(t, "id", fields[0].(map[string]any)["name"])
	assert.Equal(t, "int", fields[0].(map[string]any)["type"])
	assert.Equal(t, "name", fields[1].(map[string]any)["name"])
}

func TestCommonToAvroRecordFallbackName(t *testing.T) {
	c := schema.Common{Type: schema.Object, Children: []schema.Common{
		{Name: "x", Type: schema.Int32},
	}}
	got := avroUnmarshal(t, c, "fallback_name", "").(map[string]any)
	assert.Equal(t, "fallback_name", got["name"])
}

func TestCommonToAvroOptionalFieldDefault(t *testing.T) {
	c := schema.Common{
		Type: schema.Object,
		Name: "Rec",
		Children: []schema.Common{
			{Name: "opt", Type: schema.String, Optional: true},
		},
	}
	got := avroUnmarshal(t, c, "", "").(map[string]any)
	field := got["fields"].([]any)[0].(map[string]any)
	assert.Equal(t, []any{"null", "string"}, field["type"])
	assert.Nil(t, field["default"])
	_, hasDefault := field["default"]
	assert.True(t, hasDefault)
}

func TestCommonToAvroNamespace(t *testing.T) {
	c := schema.Common{Type: schema.Object, Name: "Root", Children: []schema.Common{
		{Name: "child", Type: schema.Object, Children: []schema.Common{
			{Name: "x", Type: schema.Int32},
		}},
	}}
	got := avroUnmarshal(t, c, "", "com.example").(map[string]any)
	assert.Equal(t, "com.example", got["namespace"])

	childType := got["fields"].([]any)[0].(map[string]any)["type"].(map[string]any)
	_, hasNS := childType["namespace"]
	assert.False(t, hasNS, "nested record must not have namespace")
}

func TestCommonToAvroNamespaceOmittedWhenEmpty(t *testing.T) {
	c := schema.Common{Type: schema.Object, Name: "Root"}
	got := avroUnmarshal(t, c, "", "").(map[string]any)
	_, hasNS := got["namespace"]
	assert.False(t, hasNS)
}

func TestCommonToAvroArray(t *testing.T) {
	c := schema.Common{Type: schema.Array, Children: []schema.Common{{Type: schema.String}}}
	got := avroUnmarshal(t, c, "", "").(map[string]any)
	assert.Equal(t, "array", got["type"])
	assert.Equal(t, "string", got["items"])
}

func TestCommonToAvroMap(t *testing.T) {
	c := schema.Common{Type: schema.Map, Children: []schema.Common{{Type: schema.Int64}}}
	got := avroUnmarshal(t, c, "", "").(map[string]any)
	assert.Equal(t, "map", got["type"])
	assert.Equal(t, "long", got["values"])
}

func TestCommonToAvroUnion(t *testing.T) {
	c := schema.Common{Type: schema.Union, Children: []schema.Common{
		{Type: schema.String},
		{Type: schema.Int32},
		{Type: schema.Null},
	}}
	got := avroUnmarshal(t, c, "", "").([]any)
	assert.Equal(t, []any{"string", "int", "null"}, got)
}

func TestCommonToAvroDecimal(t *testing.T) {
	c := schema.Common{
		Name:    "amount",
		Type:    schema.Decimal,
		Logical: &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 18, Scale: 4}},
	}
	got := avroUnmarshal(t, c, "", "").(map[string]any)
	assert.Equal(t, "bytes", got["type"])
	assert.Equal(t, "decimal", got["logicalType"])
	// JSON numbers come back as float64; both fields are present.
	assert.Equal(t, float64(18), got["precision"])
	assert.Equal(t, float64(4), got["scale"])
}

func TestCommonToAvroDecimalOptionalUnion(t *testing.T) {
	c := schema.Common{
		Name:     "amount",
		Type:     schema.Decimal,
		Optional: true,
		Logical:  &schema.LogicalParams{Decimal: &schema.DecimalParams{Precision: 9, Scale: 2}},
	}
	got := avroUnmarshal(t, c, "", "").([]any)
	require.Len(t, got, 2)
	assert.Equal(t, "null", got[0])
	inner := got[1].(map[string]any)
	assert.Equal(t, "decimal", inner["logicalType"])
	assert.Equal(t, float64(9), inner["precision"])
	assert.Equal(t, float64(2), inner["scale"])
}

func TestCommonToAvroDecimalMissingLogical(t *testing.T) {
	_, err := commonToAvroSchema(schema.Common{Name: "amount", Type: schema.Decimal}, "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing precision/scale")
}

func TestCommonToAvroBigDecimalRejected(t *testing.T) {
	_, err := commonToAvroSchema(schema.Common{Name: "amount", Type: schema.BigDecimal}, "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BigDecimal")
}

func TestSanitizeAvroName(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"my-topic-value", "my_topic_value"},
		{"123bad", "_123bad"},
		{"", "_"},
		{"valid_Name", "valid_Name"},
		{"alreadyValid", "alreadyValid"},
		{"with spaces", "with_spaces"},
		{"dot.separated", "dot_separated"},
		{"9", "_9"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeAvroName(tt.input))
		})
	}
}
