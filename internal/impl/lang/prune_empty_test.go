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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// pruneEmptyMap is the recursive named map that users write today. The
// native prune_empty method must give the same output.
const pruneEmptyMap = `map prune_empty_map {
  root = match {
    this.type() == "object" => this.map_each(kv -> kv.value.apply("prune_empty_map")).filter(kv -> kv.value != null && kv.value != [] && kv.value != {})
    this.type() == "array"  => this.map_each(ele -> ele.apply("prune_empty_map")).filter(ele -> ele != null && ele != [] && ele != {})
    _ => this
  }
}
`

type pruneEmptyCase struct {
	name    string
	mapping string
	input   string
	want    string
}

var pruneEmptyCases = []pruneEmptyCase{
	{
		name:    "nulls and empty containers removed, zero values kept",
		mapping: `root = this.prune_empty()`,
		input:   `{"a":null,"b":[],"c":{},"d":"x","e":0,"f":false,"g":""}`,
		want:    `{"d":"x","e":0,"f":false,"g":""}`,
	},
	{
		name:    "empties at several depths",
		mapping: `root = this.prune_empty()`,
		input:   `{"a":{"b":{"c":{"d":null,"e":1},"f":[]},"g":{}},"h":[{"i":[{"j":null,"k":"v"}]}]}`,
		want:    `{"a":{"b":{"c":{"e":1}}},"h":[{"i":[{"k":"v"}]}]}`,
	},
	{
		name:    "containers that become empty are removed",
		mapping: `root = this.prune_empty()`,
		input:   `{"a":{"b":{"c":null,"d":[{},[null]]}},"k":1}`,
		want:    `{"k":1}`,
	},
	{
		name:    "array elements removed and positions shift",
		mapping: `root = this.prune_empty()`,
		input:   `[1,null,[],{},[[]],{"a":null},"",0,false,[null,2]]`,
		want:    `[1,"",0,false,[2]]`,
	},
	{
		name:    "root object becomes empty",
		mapping: `root = this.prune_empty()`,
		input:   `{"a":null}`,
		want:    `{}`,
	},
	{
		name:    "root array becomes empty",
		mapping: `root = this.prune_empty()`,
		input:   `[null,[],{}]`,
		want:    `[]`,
	},
	{
		name:    "root null",
		mapping: `root.v = this.a.prune_empty()`,
		input:   `{"a":null}`,
		want:    `{"v":null}`,
	},
	{
		name:    "root scalar",
		mapping: `root.v = this.a.prune_empty()`,
		input:   `{"a":"text"}`,
		want:    `{"v":"text"}`,
	},
	{
		name:    "json numbers keep their precision",
		mapping: `root = this.prune_empty()`,
		input:   `{"n":12345678901234567890.123456789,"i":9007199254740993,"z":0,"x":null}`,
		want:    `{"i":9007199254740993,"n":12345678901234567890.123456789,"z":0}`,
	},
	{
		name:    "bytes are scalars",
		mapping: `root.v = {"b": content().slice(0,0), "n": null}.prune_empty().keys()`,
		input:   `{}`,
		want:    `{"v":["b"]}`,
	},
	{
		name:    "strings option removes empty strings",
		mapping: `root = this.prune_empty(strings: true)`,
		input:   `{"a":"","b":[""],"c":"x","d":{"e":""},"f":[0,"",false]}`,
		want:    `{"c":"x","f":[0,false]}`,
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
			// JSONEq decodes numbers as float64, so compare the raw text too
			// for the precision case.
			assert.JSONEq(t, tc.want, string(got))
			if strings.Contains(tc.name, "precision") {
				assert.Equal(t, tc.want, string(got))
			}
		})
	}
}

// TestPruneEmptyMatchesRecursiveMap checks that the native method gives the
// same output as the recursive named map, on the table inputs and on large
// nested documents.
func TestPruneEmptyMatchesRecursiveMap(t *testing.T) {
	type sample struct{ name, mapping, input string }
	var samples []sample
	for _, tc := range pruneEmptyCases {
		// The map has no strings option.
		if strings.Contains(tc.mapping, "prune_empty()") {
			samples = append(samples, sample{tc.name, tc.mapping, tc.input})
		}
	}
	for _, n := range []int{1, 7, 50} {
		samples = append(samples, sample{fmt.Sprintf("nested document with %d results", n), `root = this.prune_empty()`, string(nestedDocument(n))})
	}
	for _, s := range samples {
		t.Run(s.name, func(t *testing.T) {
			want := runMapping(t, pruneEmptyMap+strings.ReplaceAll(s.mapping, "prune_empty()", `apply("prune_empty_map")`), []byte(s.input))
			got := runMapping(t, s.mapping, []byte(s.input))
			assert.Equal(t, string(want), string(got))
		})
	}
}

// TestPruneEmptyNoSharedContainers mutates every container of the result. The
// input must stay the same.
func TestPruneEmptyNoSharedContainers(t *testing.T) {
	var in any
	require.NoError(t, json.Unmarshal([]byte(`{"a":{"b":[1,{"c":2,"d":null}],"e":{}},"f":[[3],[]],"g":"x"}`), &in))
	before, err := json.Marshal(in)
	require.NoError(t, err)

	var scribble func(v any)
	scribble = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for _, c := range t {
				scribble(c)
			}
			t["x_scribble"] = true
		case []any:
			for i, c := range t {
				scribble(c)
				t[i] = "x_scribble"
			}
		}
	}
	scribble(pruneEmpty(in, false))

	after, err := json.Marshal(in)
	require.NoError(t, err)
	assert.Equal(t, string(before), string(after))
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
		{name: "recursive_map", mapping: pruneEmptyMap + `root = this.apply("prune_empty_map")`},
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
