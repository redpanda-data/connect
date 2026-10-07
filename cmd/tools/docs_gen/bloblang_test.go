// Copyright 2024 Redpanda Data, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func TestFunctionExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp(t.TempDir(), "benthos_bloblang_functions_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`{"foo":"bar"}`)
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_FILE"
	t.Setenv(key, tmpJSONFile.Name())

	env := bloblang.GlobalEnvironment()
	env.WalkFunctions(func(name string, view *bloblang.FunctionView) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			spec := view.TemplateData()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}

				m, err := env.Parse(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := service.NewMessage([]byte(io[0]))
					textMap := propagation.MapCarrier{
						"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					}
					otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))

					textProp := otel.GetTextMapPropagator()
					otelCtx := textProp.Extract(msg.Context(), textMap)
					pCtx, _ := noop.NewTracerProvider().Tracer("blobby").Start(otelCtx, "test")
					msg = msg.WithContext(pCtx)

					p, err := msg.BloblangQuery(m)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else {
						require.NoError(t, err)

						pBytes, err := p.AsBytes()
						require.NoError(t, err)

						assertEqualOrJSON(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
		})
	})
}

func TestMethodExamples(t *testing.T) {
	tmpJSONFile, err := os.CreateTemp(t.TempDir(), "benthos_bloblang_methods_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Remove(tmpJSONFile.Name())
	})

	_, err = tmpJSONFile.WriteString(`
{
  "type":"object",
  "properties":{
    "foo":{
      "type":"string"
    }
  }
}`)
	require.NoError(t, err)

	key := "BENTHOS_TEST_BLOBLANG_SCHEMA_FILE"
	t.Setenv(key, tmpJSONFile.Name())

	env := bloblang.GlobalEnvironment()
	env.WalkMethods(func(_ string, view *bloblang.MethodView) {
		spec := view.TemplateData()
		t.Run(spec.Name, func(t *testing.T) {
			t.Parallel()
			for i, e := range spec.Examples {
				if e.SkipTesting {
					continue
				}

				m, err := env.Parse(e.Mapping)
				require.NoError(t, err)

				for j, io := range e.Results {
					msg := service.NewMessage([]byte(io[0]))
					p, err := msg.BloblangQuery(m)
					exp := io[1]
					if strings.HasPrefix(exp, "Error(") {
						exp = exp[7 : len(exp)-2]
						require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
					} else if exp == "<Message deleted>" {
						require.NoError(t, err)
						require.Nil(t, p)
					} else {
						require.NoError(t, err)

						pBytes, err := p.AsBytes()
						require.NoError(t, err)

						assertEqualOrJSON(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
					}
				}
			}
			for _, target := range spec.Categories {
				for i, e := range target.Examples {
					if e.SkipTesting {
						continue
					}

					m, err := env.Parse(e.Mapping)
					require.NoError(t, err)

					for j, io := range e.Results {
						msg := service.NewMessage([]byte(io[0]))
						p, err := msg.BloblangQuery(m)
						exp := io[1]
						if strings.HasPrefix(exp, "Error(") {
							exp = exp[7 : len(exp)-2]
							require.EqualError(t, err, exp, fmt.Sprintf("%v-%v", i, j))
						} else if exp == "<Message deleted>" {
							require.NoError(t, err)
							require.Nil(t, p)
						} else {
							require.NoError(t, err)

							pBytes, err := p.AsBytes()
							require.NoError(t, err)

							assertEqualOrJSON(t, exp, string(pBytes), fmt.Sprintf("%v-%v", i, j))
						}
					}
				}
			}
		})
	})
}

// assertEqualOrJSON compares two strings, attempting JSON semantic comparison
// if both are valid JSON. Falls back to string comparison if either string is
// not valid JSON.
func assertEqualOrJSON(t *testing.T, expected, actual string, msgAndArgs ...any) bool {
	t.Helper()

	// Try to parse both as JSON and fallback to string comparison if either is
	// not valid JSON
	var a, b any
	if err := json.Unmarshal([]byte(expected), &a); err != nil {
		return assert.Equal(t, expected, actual, msgAndArgs...)
	}
	if err := json.Unmarshal([]byte(actual), &b); err != nil {
		return assert.Equal(t, expected, actual, msgAndArgs...)
	}

	return assert.Equal(t, a, b, msgAndArgs...)
}

func TestBloblangListsHideSelfManagedOnlyInCloud(t *testing.T) {
	fns := []bloblangSpec{{Name: "now"}, {Name: "env"}}
	got := renderFunctionsList(fns, map[string]bool{"now": true})
	want := generatedBanner + "\n" +
		"\nifndef::env-cloud[]\ninclude::connect:components:partial$bloblang-functions/env.adoc[leveloffset=+1]\nendif::[]\n" +
		"\ninclude::connect:components:partial$bloblang-functions/now.adoc[leveloffset=+1]\n"
	if got != want {
		t.Errorf("functions list:\n%s\nwant:\n%s", got, want)
	}

	cat := func(c string) []bloblangCategory { return []bloblangCategory{{Category: c}} }
	methods := []bloblangSpec{
		{Name: "uppercase", Categories: cat("String Manipulation")},
		{Name: "read_file", Categories: cat("Environment")},
	}
	got = renderMethodsList(methods, map[string]bool{"uppercase": true})
	want = generatedBanner + "\n" +
		"\nifndef::env-cloud[]\n== Environment\n" +
		"\nifndef::env-cloud[]\ninclude::connect:components:partial$bloblang-methods/read_file.adoc[leveloffset=+2]\nendif::[]\n" +
		"endif::[]\n" +
		"\n== String manipulation\n" +
		"\ninclude::connect:components:partial$bloblang-methods/uppercase.adoc[leveloffset=+2]\n"
	if got != want {
		t.Errorf("methods list:\n%s\nwant:\n%s", got, want)
	}
}

func TestRenderBloblangSpecParams(t *testing.T) {
	var spec bloblangSpec
	require.NoError(t, json.Unmarshal([]byte(`{
		"name": "format_json", "status": "beta", "description": "Formats a value as JSON",
		"params": {"named": [
			{"name": "indent", "type": "string", "description": "Indentation for each level.", "default": "    "},
			{"name": "no_indent", "type": "bool", "description": "Disables indentation.", "default": false},
			{"name": "seed", "type": "timestamp", "description": "A seed.", "default": {"Value": 0}},
			{"name": "path", "type": "string", "description": "A path."},
			{"name": "extra", "type": "string", "description": "Extra.", "is_optional": true}
		]}
	}`), &spec))

	got := renderBloblangSpec(spec, "method")
	assert.Contains(t, got, "[CAUTION]\n====\nThis method is in beta.")
	assert.Contains(t, got, "| `indent` (optional)\n| `string`\n| Indentation for each level. Default: `+\"    \"+`.\n")
	assert.Contains(t, got, "| `no_indent` (optional)\n| `bool`\n| Disables indentation. Default: `+false+`.\n")
	assert.Contains(t, got, "| `seed` (optional)\n| `timestamp`\n| A seed.\n", "object defaults built in Go aren't shown")
	assert.Contains(t, got, "| `path`\n| `string`\n| A path.\n", "a param with no default stays required")
	assert.Contains(t, got, "| `extra` (optional)\n")

	variadic := bloblangSpec{Name: "concat", Status: "stable"}
	require.NoError(t, json.Unmarshal([]byte(`{"variadic": true}`), &variadic.Params))
	got = renderBloblangSpec(variadic, "method")
	assert.Contains(t, got, "== Parameters\n\nThis method accepts any number of arguments.\n")
	assert.NotContains(t, got, "CAUTION")

	assert.Contains(t, renderBloblangSpec(bloblangSpec{Name: "counter", Status: "experimental"}, "function"),
		"[CAUTION]\n====\nThis function is experimental.")
	assert.Contains(t, renderBloblangSpec(bloblangSpec{Name: "old", Status: "deprecated"}, "function"),
		"[WARNING]\n====\nThis function is deprecated")
}

func TestVisibleBloblang(t *testing.T) {
	got := visibleBloblang([]bloblangSpec{{Name: "now", Status: "stable"}, {Name: "var", Status: "hidden"}, {Name: ""}, {Name: "counter", Status: "experimental"}})
	var names []string
	for _, s := range got {
		names = append(names, s.Name)
	}
	assert.Equal(t, []string{"now", "counter"}, names)
}

func TestBloblangListsLeaveOutHiddenSpecs(t *testing.T) {
	fns := []bloblangSpec{{Name: "now"}, {Name: "var", Status: "hidden"}}
	all := map[string]bool{"now": true, "var": true}
	assert.NotContains(t, renderFunctionsList(fns, all), "var.adoc")

	cat := []bloblangCategory{{Category: "General"}}
	methods := []bloblangSpec{{Name: "apply", Categories: cat}, {Name: "secret", Status: "hidden", Categories: cat}}
	assert.NotContains(t, renderMethodsList(methods, all), "secret.adoc")
}

func TestWithCategoryText(t *testing.T) {
	base := bloblangExample{Mapping: `root = this.contains("foo")`, Results: [][2]string{{`"foo bar"`, `true`}}}
	cat := bloblangExample{Mapping: `root = this.contains(1)`, Results: [][2]string{{`[1,2]`, `true`}}}
	spec := bloblangSpec{
		Name:        "contains",
		Description: "Short.",
		Examples:    []bloblangExample{base, cat},
		Categories: []bloblangCategory{
			{Category: "Object & Array Manipulation", Description: "  The category text.  ", Examples: []bloblangExample{cat}},
			{Category: "String Manipulation", Description: "Other category text."},
		},
	}
	got := withCategoryText(spec)
	assert.Equal(t, "The category text.", got.Description, "the first category description replaces the method description")
	assert.Equal(t, []bloblangExample{cat, base}, got.Examples, "category examples come first, and no method example is lost or repeated")

	plain := bloblangSpec{Name: "x", Description: "Kept.", Examples: []bloblangExample{base}, Categories: []bloblangCategory{{Category: "General"}}}
	assert.Equal(t, plain, withCategoryText(plain), "a method whose categories have no text is unchanged")
}

func TestTemplateFieldsCoverTheTemplateSchema(t *testing.T) {
	raw, err := schema.Standard("", "").Environment().TemplateSchema("", "").MarshalJSONV0()
	require.NoError(t, err)
	dir := t.TempDir()
	w := writer{root: dir}
	w.templateFields(raw)
	b, err := os.ReadFile(filepath.Join(dir, "partials/fields/config/templates.adoc"))
	require.NoError(t, err)
	for _, heading := range []string{"=== `name`", "=== `mapping`", "=== `fields[].name`", "=== `tests[].expected`"} {
		assert.Contains(t, string(b), heading)
	}
}

func TestCategoryVariantsRenderEachCategorysText(t *testing.T) {
	str := bloblangExample{Mapping: `root = this.length()`, Results: [][2]string{{`"foo"`, `3`}}}
	arr := bloblangExample{Mapping: `root = this.length()`, Results: [][2]string{{`[1,2]`, `2`}}}
	spec := bloblangSpec{
		Name:     "length",
		Examples: []bloblangExample{str, arr},
		Categories: []bloblangCategory{
			{Category: "String Manipulation", Description: "Returns the character count of a string.", Examples: []bloblangExample{str}},
			{Category: "Object & Array Manipulation", Description: "Returns the number of items in an array or object.", Examples: []bloblangExample{arr}},
		},
	}
	variants := categoryVariants(spec)
	require.Len(t, variants, 2)
	assert.Equal(t, "Returns the character count of a string.", variants["String Manipulation"].Description)
	assert.Equal(t, []bloblangExample{arr}, variants["Object & Array Manipulation"].Examples)
	assert.Equal(t, "length-object_array_manipulation", categoryVariantName("length", "Object & Array Manipulation"))

	list := renderMethodsList([]bloblangSpec{spec}, map[string]bool{"length": true})
	assert.Contains(t, list, "partial$bloblang-methods/length-string_manipulation.adoc[")
	assert.Contains(t, list, "partial$bloblang-methods/length-object_array_manipulation.adoc[")
	assert.NotContains(t, list, "partial$bloblang-methods/length.adoc[", "each category includes its own variant")

	same := bloblangSpec{Name: "upper", Categories: []bloblangCategory{{Category: "String Manipulation", Description: "Upper."}}}
	assert.Nil(t, categoryVariants(same), "a method with one text needs no variants")
	assert.Contains(t, renderMethodsList([]bloblangSpec{same}, map[string]bool{"upper": true}), "partial$bloblang-methods/upper.adoc[")
}
