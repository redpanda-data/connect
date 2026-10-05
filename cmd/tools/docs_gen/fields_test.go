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

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildValueConfigYAML(t *testing.T) {
	tests := []struct {
		name, root, component string
		conf                  fieldSpec
		want                  string
	}{
		{
			name: "string config", root: "input", component: "sequence_ref",
			conf: fieldSpec{Type: "string", Kind: "scalar", Default: json.RawMessage(`""`)},
			want: "input:\n  label: \"\"\n  sequence_ref: \"\"\n",
		},
		{
			name: "list config", root: "output", component: "fallback",
			conf: fieldSpec{Type: "output", Kind: "array", Default: json.RawMessage(`[]`)},
			want: "output:\n  label: \"\"\n  fallback: []\n",
		},
		{
			name: "list config without a default", root: "cache", component: "multilevel",
			conf: fieldSpec{Type: "string", Kind: "array", Default: json.RawMessage(`null`)},
			want: "cache:\n  label: \"\"\n  multilevel: []\n",
		},
		{
			name: "object without fields", root: "metrics", component: "json_api",
			conf: fieldSpec{Type: "object", Kind: "scalar", Default: json.RawMessage(`{}`)},
			want: "metrics:\n  json_api: {}\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, buildValueConfigYAML(test.root, test.component, test.conf))
		})
	}
}

func TestBuildConfigYAMLShapes(t *testing.T) {
	str := func(name string) fieldSpec { return fieldSpec{Name: name, Type: "string", Kind: "scalar"} }
	tests := []struct {
		name     string
		conf     fieldSpec
		advanced bool
		want     string
	}{
		{
			name: "list config renders one list item",
			conf: fieldSpec{Type: "object", Kind: "array", Children: []fieldSpec{
				str("check"),
				{Name: "processors", Type: "processor", Kind: "array", Default: json.RawMessage(`[]`)},
			}},
			want: "processor:\n  label: \"\"\n  x:\n    - check: \"\" # No default (required)\n      processors: []\n",
		},
		{
			name: "map of objects renders a placeholder key",
			conf: fieldSpec{Type: "object", Children: []fieldSpec{
				{Name: "branches", Type: "object", Kind: "map", Children: []fieldSpec{str("request_map"), str("result_map")}},
			}},
			want: "processor:\n  label: \"\"\n  x:\n    branches:\n      <name>:\n        request_map: \"\" # No default (required)\n        result_map: \"\" # No default (required)\n",
		},
		{
			name: "placeholders follow the type",
			conf: fieldSpec{Type: "object", Children: []fieldSpec{
				{Name: "headers", Type: "string", Kind: "map", IsOptional: true},
				{Name: "enabled", Type: "bool", Kind: "scalar"},
				{Name: "count", Type: "int", Kind: "scalar"},
				{Name: "ratio", Type: "float", Kind: "scalar"},
				{Name: "inner", Type: "output", Kind: "scalar"},
				{Name: "topics", Type: "string", Kind: "array"},
			}},
			want: "processor:\n  label: \"\"\n  x:\n    headers: {} # No default (optional)\n    enabled: false # No default (required)\n" +
				"    count: 0 # No default (required)\n    ratio: 0 # No default (required)\n    inner: {} # No default (required)\n    topics: [] # No default (required)\n",
		},
		{
			name: "an object whose fields are all advanced is never a bare key",
			conf: fieldSpec{Type: "object", Children: []fieldSpec{
				{Name: "aws", Type: "object", Children: []fieldSpec{{Name: "region", Type: "string", IsAdvanced: true}}},
			}},
			want: "processor:\n  label: \"\"\n  x:\n    aws: {}\n",
		},
		{
			name: "a null default renders inline",
			conf: fieldSpec{Type: "object", Children: []fieldSpec{
				{Name: "value", Type: "unknown", Default: json.RawMessage(`null`)},
			}},
			want: "processor:\n  label: \"\"\n  x:\n    value: null\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, buildConfigYAML("processor", "x", test.conf, test.advanced))
		})
	}
}

func TestRenderFieldDefaultEscapesControlCharacters(t *testing.T) {
	assert.Equal(t, "*Default*: `\"\\n\"`\n\n", renderFieldDefault("\n"))
	assert.Equal(t, "*Default*: `\"\\t\"`\n\n", renderFieldDefault("\t"))
	assert.Equal(t, "*Default*: `\" \"`\n\n", renderFieldDefault(" "))
	assert.Equal(t, "*Default*: `\"\"`\n\n", renderFieldDefault(""))
	assert.Equal(t, "*Default*: `null`\n\n", renderFieldDefault(nil))
	assert.Equal(t, "*Default*: `a,b`\n\n", renderFieldDefault("a,b"))
}

func TestFieldDisplayTypeShowsMapValueType(t *testing.T) {
	assert.Equal(t, "object<string>", fieldDisplayType(fieldSpec{Kind: "map", Type: "string"}))
	assert.Equal(t, "object<input>", fieldDisplayType(fieldSpec{Kind: "map", Type: "input"}))
	assert.Equal(t, "object", fieldDisplayType(fieldSpec{Kind: "map", Type: "unknown"}))
	assert.Equal(t, "object", fieldDisplayType(fieldSpec{Kind: "map", Type: "object"}))
}

func TestWithInterpolationNotice(t *testing.T) {
	link := "xref:configuration:interpolation.adoc#bloblang-queries"
	assert.Equal(t, "The table. Supports "+link+"[interpolation functions].",
		withInterpolationNotice("The table. Supports interpolation functions."))
	assert.Equal(t, "Pusher channel. "+link+"[Interpolation functions] can also be used",
		withInterpolationNotice("Pusher channel. Interpolation functions can also be used"))
	assert.Equal(t, "The path.\n\n"+interpolationNotice, withInterpolationNotice("The path."))
	assert.Equal(t, interpolationNotice, withInterpolationNotice(interpolationNotice))
}

func TestFieldPathsForListsAndListRoots(t *testing.T) {
	assert.Equal(t, "batches[][]", fieldName(fieldSpec{Name: "batches", Kind: "2darray"}))
	assert.Equal(t, "items[]", fieldName(fieldSpec{Name: "items", Kind: "array"}))
	assert.Equal(t, "plain", fieldName(fieldSpec{Name: "plain", Kind: "scalar"}))

	tests := fieldSpec{Name: "tests", Kind: "array", Children: []fieldSpec{
		{Name: "name", Kind: "scalar", Type: "string"},
		{Name: "input_batches", Kind: "2darray", Type: "object", Children: []fieldSpec{
			{Name: "content", Kind: "scalar", Type: "string"},
		}},
	}}
	out := renderFields(tests.Children, configObjectPrefix(tests))
	assert.Contains(t, out, "=== `tests[].name`")
	assert.Contains(t, out, "=== `tests[].input_batches[][]`")
	assert.Contains(t, out, "=== `tests[].input_batches[][].content`")

	redpanda := fieldSpec{Name: "redpanda", Kind: "scalar", Children: []fieldSpec{{Name: "acks", Kind: "scalar", Type: "string"}}}
	assert.Contains(t, renderFields(redpanda.Children, configObjectPrefix(redpanda)), "=== `acks`")
}
