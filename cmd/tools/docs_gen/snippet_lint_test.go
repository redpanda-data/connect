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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/public/schema"
)

// structuralLints are the lint types that mean a snippet has the wrong shape:
// an unknown root or field, a value of the wrong kind, or an unknown
// component. The other types (required fields, enum options, custom rules,
// Bloblang) fire on the empty placeholders a snippet shows for fields with no
// default, so the test allows them.
var structuralLints = map[service.LintType]bool{
	service.LintFailedRead:        true,
	service.LintUnknown:           true,
	service.LintExpectedArray:     true,
	service.LintExpectedObject:    true,
	service.LintExpectedScalar:    true,
	service.LintComponentMissing:  true,
	service.LintComponentNotFound: true,
	service.LintDeprecated:        true,
	service.LintShouldOmit:        true,
	service.LintBadLabel:          true,
	service.LintDuplicateLabel:    true,
	service.LintMissingLabel:      true,
}

// lintableConfig wraps a snippet in the smallest stream config that has a
// place for its component type. Inputs, outputs, buffers, metrics, tracers,
// and top-level config objects are already config roots.
func lintableConfig(root, snippet string) string {
	body := strings.Split(strings.TrimSuffix(snippet, "\n"), "\n")[1:]
	item := func(prefix string) string {
		var b strings.Builder
		for i, l := range body {
			l = strings.TrimPrefix(l, "  ")
			if i == 0 {
				b.WriteString(prefix + "- " + l + "\n")
			} else {
				b.WriteString(prefix + "  " + l + "\n")
			}
		}
		return b.String()
	}
	// Resource labels must be unique and not empty.
	resourceLabel := func(s string) string {
		return strings.Replace(s, `- label: ""`, "- label: snippet_resource", 1)
	}
	switch root {
	case "processor":
		return "pipeline:\n  processors:\n" + item("    ")
	case "cache":
		return resourceLabel("cache_resources:\n" + item("  "))
	case "rate_limit":
		return resourceLabel("rate_limit_resources:\n" + item("  "))
	case "scanner":
		return "input:\n  stdin:\n    scanner:\n" + indentLines(strings.Join(body, "\n"), "    ") + "\n"
	}
	return snippet
}

// resourceLabelLint is the lint for a label on a resource reference. The
// benthos linter also raises it for any component with a string field named
// resource, such as the jira input, so the snippet test skips it for those.
const resourceLabelLint = "label field should be omitted when pointing to a resource"

func structuralErrors(t *testing.T, linter *service.StreamConfigLinter, conf string, skip ...string) []string {
	t.Helper()
	lints, err := linter.LintYAML([]byte(conf))
	require.NoError(t, err, "config:\n%s", conf)
	var errs []string
	for _, l := range lints {
		skipped := false
		for _, s := range skip {
			skipped = skipped || l.What == s
		}
		if structuralLints[l.Type] && !skipped {
			errs = append(errs, l.Error())
		}
	}
	return errs
}

func newSnippetLinter() *service.StreamConfigLinter {
	return schema.Standard("", "").NewStreamConfigLinter().SetSkipEnvVarCheck(true)
}

// Every Common and Advanced snippet must be config the linter accepts in its
// place in a stream config: the right root, known fields, and values of the
// right kind. Run with -tags x_benthos_extra to cover the cgo components too,
// as the docs task does.
func TestSnippetsLint(t *testing.T) {
	raw, err := schema.Standard("", "").MarshalJSONV0()
	require.NoError(t, err)
	full, err := parseFullSchema(raw)
	require.NoError(t, err)
	linter := newSnippetLinter()

	var checked int
	for _, g := range full.Groups {
		for _, c := range g.Components {
			where := g.Key + "/" + c.Name
			common, advanced := componentSnippets(c)
			for kind, snippet := range map[string]string{"common": common, "advanced": advanced} {
				require.True(t, strings.HasPrefix(snippet, c.Type+":\n"), "%s %s snippet must start with %s:", where, kind, c.Type)
				conf := lintableConfig(c.Type, snippet)
				var skip []string
				if hasField(c.Config.Children, "resource") {
					skip = append(skip, resourceLabelLint)
				}
				for _, e := range structuralErrors(t, linter, conf, skip...) {
					t.Errorf("%s %s snippet: %s\n%s", where, kind, e, conf)
				}
				checked++
			}
			assertNoAdvancedFields(t, where, c, common)
		}
	}

	objects, err := topLevelConfigObjects(raw)
	require.NoError(t, err)
	for _, f := range objects {
		for _, adv := range []bool{false, true} {
			snippet := buildTopLevelConfigYAML(f, adv)
			for _, e := range structuralErrors(t, linter, snippet) {
				t.Errorf("config/%s snippet (advanced=%v): %s\n%s", f.Name, adv, e, snippet)
			}
			checked++
		}
	}
	require.Greater(t, checked, 600)
}

// The lint check must reject the snippet shapes the generator used to write.
func TestSnippetLintCatchesBadShapes(t *testing.T) {
	linter := newSnippetLinter()
	bad := map[string]string{
		"plural root key":        "inputs:\n  label: \"\"\n  generate:\n    mapping: \"\"\n",
		"list config as object":  lintableConfig("processor", "processor:\n  label: \"\"\n  switch:\n    check: \"\"\n    processors: []\n"),
		"map rendered as fields": lintableConfig("processor", "processor:\n  label: \"\"\n  workflow:\n    branches:\n      request_map: \"\"\n      processors: []\n"),
		"string for a map":       "output:\n  label: \"\"\n  kafka:\n    addresses: []\n    topic: \"\"\n    static_headers: \"\"\n",
		"bare key reads as null": "output:\n  label: \"\"\n  kafka:\n    addresses: []\n    topic: \"\"\n    sasl:\n    static_headers: {}\n    tls:\n",
	}
	for name, conf := range bad {
		t.Run(name, func(t *testing.T) {
			assert.NotEmpty(t, structuralErrors(t, linter, conf), "the linter accepted:\n%s", conf)
		})
	}
	good := lintableConfig("processor", "processor:\n  label: \"\"\n  switch:\n    - check: \"\"\n      processors: []\n")
	assert.Empty(t, structuralErrors(t, linter, good), good)
}

func hasField(fields []fieldSpec, name string) bool {
	for _, f := range fields {
		if f.Name == name {
			return true
		}
	}
	return false
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

// snippetConfig returns the component's config from a parsed snippet: the
// value under the component name, or the first item when the config is a
// list.
func snippetConfig(t *testing.T, c componentSpec, snippet string) any {
	t.Helper()
	var v map[string]map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(snippet), &v), snippet)
	conf := v[c.Type][c.Name]
	if list, ok := conf.([]any); ok && len(list) > 0 {
		return list[0]
	}
	return conf
}

// lookupPath follows a dotted field path through plain objects.
func lookupPath(node any, path []string) (any, bool) {
	for _, p := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return nil, false
		}
		if node, ok = m[p]; !ok {
			return nil, false
		}
	}
	return node, true
}

// assertNoAdvancedFields checks that no advanced field, at any depth, appears
// in the Common snippet.
func assertNoAdvancedFields(t *testing.T, where string, c componentSpec, common string) {
	t.Helper()
	root := snippetConfig(t, c, common)
	walkFields(c.Config.Children, func(path string, f fieldSpec) {
		if _, found := lookupPath(root, strings.Split(path, ".")); f.IsAdvanced && found {
			t.Errorf("%s: advanced field %s appears in the Common snippet", where, path)
		}
	})
}
