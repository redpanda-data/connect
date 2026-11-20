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

package lang

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// pruneEmptyMaps holds the recursive named maps that users write today. The
// native prune_empty method must give the same output. prune_empty_map is the
// default mode and prune_empty_strings_map is the empty_strings mode.
const pruneEmptyMaps = `map prune_empty_map {
  root = match {
    this.type() == "object" => this.map_each(kv -> kv.value.apply("prune_empty_map")).filter(kv -> kv.value != null && kv.value != [] && kv.value != {})
    this.type() == "array"  => this.map_each(ele -> ele.apply("prune_empty_map")).filter(ele -> ele != null && ele != [] && ele != {})
    _ => this
  }
}
map prune_empty_strings_map {
  root = match {
    this.type() == "object" => this.map_each(kv -> kv.value.apply("prune_empty_strings_map")).filter(kv -> kv.value != null && kv.value != [] && kv.value != {} && kv.value != "")
    this.type() == "array"  => this.map_each(ele -> ele.apply("prune_empty_strings_map")).filter(ele -> ele != null && ele != [] && ele != {} && ele != "")
    _ => this
  }
}
`

type pruneEmptyCase struct {
	name string
	// mapping uses the native method.
	mapping string
	// mapMapping is the same mapping with a map of pruneEmptyMaps in place of
	// the native method. Each case must set it, so that no case is left out
	// of TestPruneEmptyMatchesRecursiveMap.
	mapMapping string
	input      string
	// want is the exact output text: compact JSON with sorted keys.
	want string
}

var pruneEmptyCases = []pruneEmptyCase{
	{
		name:       "nulls and empty containers removed, zero values kept",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `{"a":null,"b":[],"c":{},"d":"x","e":0,"f":false,"g":""}`,
		want:       `{"d":"x","e":0,"f":false,"g":""}`,
	},
	{
		name:       "empties at several depths",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `{"a":{"b":{"c":{"d":null,"e":1},"f":[]},"g":{}},"h":[{"i":[{"j":null,"k":"v"}]}]}`,
		want:       `{"a":{"b":{"c":{"e":1}}},"h":[{"i":[{"k":"v"}]}]}`,
	},
	{
		name:       "containers that become empty are removed",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `{"a":{"b":{"c":null,"d":[{},[null]]}},"k":1}`,
		want:       `{"k":1}`,
	},
	{
		name:       "array elements removed and positions shift",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `[1,null,[],{},[[]],{"a":null},"",0,false,[null,2]]`,
		want:       `[1,"",0,false,[2]]`,
	},
	{
		name:       "root object becomes empty",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `{"a":null}`,
		want:       `{}`,
	},
	{
		name:       "root array becomes empty",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `[null,[],{}]`,
		want:       `[]`,
	},
	{
		name:       "root null",
		mapping:    `root.v = this.a.prune_empty()`,
		mapMapping: `root.v = this.a.apply("prune_empty_map")`,
		input:      `{"a":null}`,
		want:       `{"v":null}`,
	},
	{
		name:       "root scalar",
		mapping:    `root.v = this.a.prune_empty()`,
		mapMapping: `root.v = this.a.apply("prune_empty_map")`,
		input:      `{"a":"text"}`,
		want:       `{"v":"text"}`,
	},
	{
		name:       "empty_strings option keeps a root empty string",
		mapping:    `root.v = this.a.prune_empty(empty_strings: true)`,
		mapMapping: `root.v = this.a.apply("prune_empty_strings_map")`,
		input:      `{"a":""}`,
		want:       `{"v":""}`,
	},
	{
		name:       "json numbers keep their precision",
		mapping:    `root = this.prune_empty()`,
		mapMapping: `root = this.apply("prune_empty_map")`,
		input:      `{"n":12345678901234567890.123456789,"i":9007199254740993,"z":0,"x":null}`,
		want:       `{"i":9007199254740993,"n":12345678901234567890.123456789,"z":0}`,
	},
	{
		name:       "bytes are scalars",
		mapping:    `root.v = {"b": content().slice(0,0), "n": null}.prune_empty().keys()`,
		mapMapping: `root.v = {"b": content().slice(0,0), "n": null}.apply("prune_empty_map").keys()`,
		input:      `{}`,
		want:       `{"v":["b"]}`,
	},
	{
		name:       "empty_strings option removes empty strings",
		mapping:    `root = this.prune_empty(empty_strings: true)`,
		mapMapping: `root = this.apply("prune_empty_strings_map")`,
		input:      `{"a":"","b":[""],"c":"x","d":{"e":""},"f":[0,"",false]}`,
		want:       `{"c":"x","f":[0,false]}`,
	},
	{
		name:       "empty_strings option removes empty bytes",
		mapping:    `root.v = {"b": content().slice(0,0), "s": "", "k": "x"}.prune_empty(empty_strings: true).keys()`,
		mapMapping: `root.v = {"b": content().slice(0,0), "s": "", "k": "x"}.apply("prune_empty_strings_map").keys()`,
		input:      `{}`,
		want:       `{"v":["k"]}`,
	},
	{
		name:       "empty_strings option keeps bytes that are not empty",
		mapping:    `root.v = {"b": content().slice(0,1), "s": ""}.prune_empty(empty_strings: true).keys()`,
		mapMapping: `root.v = {"b": content().slice(0,1), "s": ""}.apply("prune_empty_strings_map").keys()`,
		input:      `{}`,
		want:       `{"v":["b"]}`,
	},
}

func runMapping(t testing.TB, mapping string, input []byte) []byte {
	t.Helper()
	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)
	res, err := service.NewMessage(input).BloblangQuery(exe)
	require.NoError(t, err)
	out, err := res.AsBytes()
	require.NoError(t, err)
	return out
}

func TestPruneEmpty(t *testing.T) {
	for _, tc := range pruneEmptyCases {
		t.Run(tc.name, func(t *testing.T) {
			got := runMapping(t, tc.mapping, []byte(tc.input))
			// Compare the raw text. JSONEq decodes numbers as float64, so it
			// does not see a loss of json.Number precision.
			assert.Equal(t, tc.want, string(got))
		})
	}
}

// TestPruneEmptyMatchesRecursiveMap checks that the native method gives the
// same output as the recursive named maps, in the two modes, on the table
// inputs and on large nested documents.
func TestPruneEmptyMatchesRecursiveMap(t *testing.T) {
	type sample struct{ name, mapping, mapMapping, input string }
	var samples []sample
	for _, tc := range pruneEmptyCases {
		samples = append(samples, sample{tc.name, tc.mapping, tc.mapMapping, tc.input})
	}
	for _, n := range []int{1, 7, 50} {
		doc := string(nestedDocument(n))
		samples = append(samples,
			sample{
				name:       fmt.Sprintf("nested document with %d results", n),
				mapping:    `root = this.prune_empty()`,
				mapMapping: `root = this.apply("prune_empty_map")`,
				input:      doc,
			},
			sample{
				name:       fmt.Sprintf("nested document with %d results, empty_strings", n),
				mapping:    `root = this.prune_empty(empty_strings: true)`,
				mapMapping: `root = this.apply("prune_empty_strings_map")`,
				input:      doc,
			},
		)
	}
	for _, s := range samples {
		t.Run(s.name, func(t *testing.T) {
			require.NotEmpty(t, s.mapMapping)
			want := runMapping(t, pruneEmptyMaps+s.mapMapping, []byte(s.input))
			got := runMapping(t, s.mapping, []byte(s.input))
			assert.Equal(t, string(want), string(got))
		})
	}
}

// TestPruneEmptyNoSharedContainers mutates every container of the result. The
// input must stay the same, in the two modes.
func TestPruneEmptyNoSharedContainers(t *testing.T) {
	const input = `{"a":{"b":[1,{"c":2,"d":null,"h":""}],"e":{}},"f":[[3],[]],"g":"x"}`

	for _, strs := range []bool{false, true} {
		var in any
		require.NoError(t, json.Unmarshal([]byte(input), &in))

		scribble(pruneEmpty(in, strs))

		after, err := json.Marshal(in)
		require.NoError(t, err)
		assert.JSONEq(t, input, string(after), "empty_strings=%t", strs)
	}
}

// scribble adds an entry to each object and replaces each array element, at
// all depths of v.
func scribble(v any) {
	switch c := v.(type) {
	case map[string]any:
		for _, child := range c {
			scribble(child)
		}
		c["x_scribble"] = true
	case []any:
		for i, child := range c {
			scribble(child)
			c[i] = "x_scribble"
		}
	}
}

// nestedDocument returns a JSON document shaped like a search API response.
// Each of the n results nests objects and arrays 3 to 5 levels deep, with
// nulls and empty containers at each level.
func nestedDocument(n int) []byte {
	results := make([]any, 0, n)
	for i := range n {
		var note any
		if i%3 != 0 {
			note = fmt.Sprintf("note %d", i)
		}
		results = append(results, map[string]any{
			"id":   i,
			"name": fmt.Sprintf("item-%d", i),
			"note": note,
			"tags": []any{"a", nil, []any{}},
			"meta": map[string]any{
				"owner":  map[string]any{"id": i, "email": nil, "roles": []any{}},
				"flags":  map[string]any{},
				"score":  float64(i) / 4,
				"active": i%2 == 0,
			},
			"lines": []any{
				map[string]any{
					"sku":       fmt.Sprintf("sku-%d", i),
					"qty":       0,
					"attrs":     map[string]any{"color": nil, "size": map[string]any{"eu": nil}},
					"discounts": []any{map[string]any{"code": nil}, map[string]any{}, map[string]any{"code": "x", "pct": 5}},
				},
				nil,
				[]any{},
			},
			"address": map[string]any{
				"geo":  map[string]any{"lat": nil, "lng": nil, "src": map[string]any{"kind": nil}},
				"city": "Paris",
				"zip":  "",
			},
		})
	}
	doc := map[string]any{
		"data": map[string]any{
			"results": results,
			"searchCriteria": map[string]any{
				"filters": map[string]any{
					"status":   []any{"open", nil},
					"owner":    nil,
					"range":    map[string]any{"from": nil, "to": map[string]any{}},
					"keywords": []any{},
				},
				"page": 1,
			},
		},
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// BenchmarkPruneEmpty compares the recursive named map and the native method
// on the same document of about 60 KiB.
func BenchmarkPruneEmpty(b *testing.B) {
	doc := nestedDocument(160)
	b.Logf("document size: %d bytes", len(doc))

	for _, bc := range []struct{ name, mapping string }{
		{name: "recursive_map", mapping: pruneEmptyMaps + `root = this.apply("prune_empty_map")`},
		{name: "native_method", mapping: `root = this.prune_empty()`},
	} {
		b.Run(bc.name, func(b *testing.B) {
			exe, err := bloblang.Parse(bc.mapping)
			require.NoError(b, err)
			msg := service.NewMessage(doc)
			// Parse the JSON once, outside of the timed loop.
			_, err = msg.AsStructured()
			require.NoError(b, err)

			b.ReportAllocs()
			b.SetBytes(int64(len(doc)))
			for b.Loop() {
				if _, err := msg.BloblangQuery(exe); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
