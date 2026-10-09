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
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// plainJSON converts a decoded JSON or YAML value to plain JSON types, so
// that a YAML int and a JSON number with the same value compare equal.
func plainJSON(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	var out any
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

// Every field example and every default the generator renders as YAML must
// parse back to the value in the spec. This runs over the whole Standard
// schema rather than fixtures, so a quoting or block-scalar bug in any real
// field fails here.
func TestRenderedValuesRoundTrip(t *testing.T) {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	require.NoError(t, err)
	full, err := parseFullSchema(raw)
	require.NoError(t, err)

	var examples, defaults int
	for _, g := range full.Groups {
		for _, c := range g.Components {
			where := g.Key + "/" + c.Name
			walkFields(c.Config.Children, func(path string, f fieldSpec) {
				if len(f.Examples) == 0 {
					return
				}
				block := renderFieldExamples(f)
				body := strings.TrimSuffix(strings.SplitN(block, "# Examples:\n", 2)[1], "----\n\n")
				parts := strings.Split(body, "\n# ---\n")
				require.Len(t, parts, len(f.Examples), "%s field %s", where, path)
				for i, part := range parts {
					var got map[string]any
					require.NoError(t, yaml.Unmarshal([]byte(part), &got), "%s field %s example %d:\n%s", where, path, i+1, part)
					want := plainJSON(t, decodeValue(f.Examples[i]))
					assert.Equal(t, want, plainJSON(t, got[f.Name]), "%s field %s example %d:\n%s", where, path, i+1, part)
					examples++
				}
			})

			if c.Config.Children == nil {
				continue
			}
			_, advanced := componentSnippets(c)
			root := snippetConfig(t, c, advanced)
			walkFields(c.Config.Children, func(path string, f fieldSpec) {
				// A map of objects shows one placeholder entry, not its
				// default.
				if !f.hasDefault() || (f.Kind == "map" && len(f.Children) > 0) {
					return
				}
				got, found := lookupPath(root, strings.Split(path, "."))
				if !found {
					// Fields under a list item or a map value aren't
					// reachable by a plain path.
					return
				}
				assert.Equal(t, plainJSON(t, f.defaultValue()), plainJSON(t, got), "%s default of %s:\n%s", where, path, advanced)
				defaults++
			})
		}
	}
	require.Greater(t, examples, 1000)
	require.Greater(t, defaults, 3000)
}

func TestListExamplesKeepQuotes(t *testing.T) {
	f := fieldSpec{Name: "tables", Kind: "array", Type: "string", Examples: []json.RawMessage{
		json.RawMessage(`["my_table_1", "\"MyCaseSensitiveTableNeedingQuotes\"", "'single'"]`),
	}}
	out := renderFieldExamples(f)
	assert.Contains(t, out, `  - '"MyCaseSensitiveTableNeedingQuotes"'`)
	body := strings.TrimSuffix(strings.SplitN(out, "# Examples:\n", 2)[1], "----\n\n")
	var got map[string][]string
	require.NoError(t, yaml.Unmarshal([]byte(body), &got), body)
	assert.Equal(t, []string{"my_table_1", `"MyCaseSensitiveTableNeedingQuotes"`, "'single'"}, got["tables"])
}

var fieldHeading = regexp.MustCompile("(?m)^=== `")

// Each top-level config object with fields gets a field reference that lists
// every non-deprecated field, so the hand-kept lists on the redpanda, logger,
// and http pages can be replaced by an include.
func TestTopLevelConfigObjects(t *testing.T) {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	require.NoError(t, err)
	objects, err := topLevelConfigObjects(raw)
	require.NoError(t, err)

	counts := map[string]int{}
	for _, f := range objects {
		var want int
		walkFields(f.Children, func(string, fieldSpec) { want++ })
		got := len(fieldHeading.FindAllString(renderFields(f.Children, ""), -1))
		assert.Equal(t, want, got, "config/%s field count", f.Name)
		counts[f.Name] = got
	}
	for _, name := range []string{"http", "logger", "redpanda", "error_handling", "pipeline", "tests"} {
		assert.Contains(t, counts, name)
	}
	// redpanda has 66 fields today, one of them deprecated.
	assert.GreaterOrEqual(t, counts["redpanda"], 65)
	for _, name := range []string{"input", "metrics", "shutdown_timeout"} {
		assert.NotContains(t, counts, name)
	}
}
