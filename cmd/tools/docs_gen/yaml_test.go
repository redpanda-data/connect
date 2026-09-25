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

// The expected output of each case was produced by the yaml npm package
// (v2.8.2) with the options the published docs were generated with.
func TestYAMLStringifyMatchesNPMYAML(t *testing.T) {
	tests := []struct {
		name  string
		style yamlStringStyle
		json  string
		want  string
	}{
		{name: "nested map PLAIN", style: yamlPlain, json: `{"a":true,"b":{"c":"x","d":[1,2]}}`, want: "a: true\nb:\n  c: x\n  d:\n    - 1\n    - 2\n"},
		{name: "nested map QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":true,"b":{"c":"x","d":[1,2]}}`, want: "a: true\nb:\n  c: \"x\"\n  d:\n    - 1\n    - 2\n"},
		{name: "seq of maps PLAIN", style: yamlPlain, json: `[{"name":"a","value":"1"},{"name":"b"}]`, want: "- name: a\n  value: \"1\"\n- name: b\n"},
		{name: "seq of maps QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `[{"name":"a","value":"1"},{"name":"b"}]`, want: "- name: \"a\"\n  value: \"1\"\n- name: \"b\"\n"},
		{name: "seq of seqs PLAIN", style: yamlPlain, json: `[["a","b"],["c"]]`, want: "- - a\n  - b\n- - c\n"},
		{name: "seq of seqs QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `[["a","b"],["c"]]`, want: "- - \"a\"\n  - \"b\"\n- - \"c\"\n"},
		{name: "empty collections PLAIN", style: yamlPlain, json: `{"a":{},"b":[]}`, want: "a: {}\nb: []\n"},
		{name: "empty collections QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":{},"b":[]}`, want: "a: {}\nb: []\n"},
		{name: "reserved words PLAIN", style: yamlPlain, json: `{"a":"true","b":"null","c":"","d":"42","e":"0.5","f":"1e3"}`, want: "a: \"true\"\nb: \"null\"\nc: \"\"\nd: \"42\"\ne: \"0.5\"\nf: \"1e3\"\n"},
		{name: "reserved words QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":"true","b":"null","c":"","d":"42","e":"0.5","f":"1e3"}`, want: "a: \"true\"\nb: \"null\"\nc: \"\"\nd: \"42\"\ne: \"0.5\"\nf: \"1e3\"\n"},
		{name: "indicators PLAIN", style: yamlPlain, json: `{"@k":"v","a":"*x","b":"- x","c":"a: b","d":"x #y","e":"@service"}`, want: "\"@k\": v\na: \"*x\"\nb: \"- x\"\nc: \"a: b\"\nd: \"x #y\"\ne: \"@service\"\n"},
		{name: "indicators QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"@k":"v","a":"*x","b":"- x","c":"a: b","d":"x #y","e":"@service"}`, want: "\"@k\": \"v\"\na: \"*x\"\nb: \"- x\"\nc: \"a: b\"\nd: \"x #y\"\ne: \"@service\"\n"},
		{name: "quotes PLAIN", style: yamlPlain, json: `{"a":"say \"hi\"","b":"it's"}`, want: "a: say \"hi\"\nb: it's\n"},
		{name: "quotes QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":"say \"hi\"","b":"it's"}`, want: "a: \"say \\\"hi\\\"\"\nb: \"it's\"\n"},
		{name: "multiline PLAIN", style: yamlPlain, json: `{"a":"line1\nline2","b":"trail\n","c":"x\n\n"}`, want: "a: |-\n  line1\n  line2\nb: |\n  trail\nc: |+\n  x\n\n"},
		{name: "multiline QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":"line1\nline2","b":"trail\n","c":"x\n\n"}`, want: "a: \"line1\\nline2\"\nb: \"trail\\n\"\nc: \"x\\n\\n\"\n"},
		{name: "long multiline PLAIN", style: yamlPlain, json: `{"a":"this is a fairly long first line\nand a second line here"}`, want: "a: |-\n  this is a fairly long first line\n  and a second line here\n"},
		{name: "long multiline QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"a":"this is a fairly long first line\nand a second line here"}`, want: "a: \"this is a fairly long first line\n\n  and a second line here\"\n"},
		{name: "integer keys PLAIN", style: yamlPlain, json: `{"2":3,"10":2,"a":4,"b":1}`, want: "\"2\": 3\n\"10\": 2\na: 4\nb: 1\n"},
		{name: "integer keys QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `{"2":3,"10":2,"a":4,"b":1}`, want: "\"2\": 3\n\"10\": 2\na: 4\nb: 1\n"},
		{name: "scalar null PLAIN", style: yamlPlain, json: `null`, want: "null\n"},
		{name: "scalar null QUOTE_DOUBLE", style: yamlDoubleQuoted, json: `null`, want: "null\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, yamlStringify(decodeValue(json.RawMessage(test.json)), test.style))
		})
	}
}
