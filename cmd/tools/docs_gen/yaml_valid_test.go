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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// Every config snippet and field example the generator writes must be YAML a
// user can paste, and the Common snippet must not list advanced fields.
func TestGeneratedYAMLIsValid(t *testing.T) {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	require.NoError(t, err)
	full, err := parseFullSchema(raw)
	require.NoError(t, err)

	var checked int
	for _, g := range full.Groups {
		for _, c := range g.Components {
			where := g.Key + "/" + c.Name
			var common, advanced string
			if c.Config.Children != nil {
				common = buildConfigYAML(g.Key, c.Name, c.Config.Children, false)
				advanced = buildConfigYAML(g.Key, c.Name, c.Config.Children, true)
			} else {
				common = buildValueConfigYAML(g.Key, c.Name, c.Config)
				advanced = common
			}
			for kind, snippet := range map[string]string{"common": common, "advanced": advanced} {
				var v any
				require.NoError(t, yaml.Unmarshal([]byte(snippet), &v), "%s %s snippet:\n%s", where, kind, snippet)
				checked++
			}
			assertNoAdvancedFields(t, where, c.Config.Children, common)

			walkFields(c.Config.Children, func(path string, f fieldSpec) {
				if len(f.Examples) == 0 {
					return
				}
				block := renderFieldExamples(f)
				body := strings.TrimSuffix(strings.SplitN(block, "# Examples:\n", 2)[1], "----\n\n")
				for i, ex := range strings.Split(body, "\n# ---\n") {
					var v any
					require.NoError(t, yaml.Unmarshal([]byte(ex), &v), "%s field %s example %d:\n%s", where, path, i+1, ex)
					checked++
				}
			})
		}
	}
	require.Greater(t, checked, 1000)
}

func walkFields(fields []fieldSpec, fn func(path string, f fieldSpec)) {
	var walk func(fs []fieldSpec, prefix string)
	walk = func(fs []fieldSpec, prefix string) {
		for _, f := range fs {
			if f.IsDeprecated {
				continue
			}
			p := f.Name
			if prefix != "" {
				p = prefix + "." + f.Name
			}
			fn(p, f)
			walk(f.Children, p)
		}
	}
	walk(fields, "")
}

// assertNoAdvancedFields checks that no advanced field, at any depth, appears
// in the Common snippet.
func assertNoAdvancedFields(t *testing.T, where string, fields []fieldSpec, common string) {
	t.Helper()
	var v map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(common), &v))
	var inCommon func(node any, path []string) bool
	inCommon = func(node any, path []string) bool {
		if len(path) == 0 {
			return true
		}
		m, ok := node.(map[string]any)
		if !ok {
			return false
		}
		child, ok := m[path[0]]
		return ok && inCommon(child, path[1:])
	}
	var root any
	for _, top := range v {
		if m, ok := top.(map[string]any); ok {
			for k, cfg := range m {
				if k != "label" {
					root = cfg
				}
			}
		}
	}
	walkFields(fields, func(path string, f fieldSpec) {
		if f.IsAdvanced && inCommon(root, strings.Split(path, ".")) {
			t.Errorf("%s: advanced field %s appears in the Common snippet", where, path)
		}
	})
}
