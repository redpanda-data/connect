package docs_test

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestInference(t *testing.T) {
	for _, t := range docs.Types() {
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testfoo%v", string(t)),
			Type: t,
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testbar%v", string(t)),
			Type: t,
		})
	}

	type testCase struct {
		inputType    docs.Type
		inputConf    interface{}
		inputDefault string

		res string
		err string
	}

	tests := []testCase{
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"processors": "yep",
			},
			inputDefault: "testfooinput",
			res:          "testfooinput",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]interface{}{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer output type, candidates were: [bar foo]",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"foo":        "yep",
				"bar":        "yep",
				"processors": "yep",
			},
			err: "unable to infer input type, candidates were: [bar foo]",
		},
		{
			inputType: docs.TypeTracer,
			inputConf: map[string]interface{}{
				"testbartracer": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbartracer",
		},
		{
			inputType: docs.TypeRateLimit,
			inputConf: map[string]interface{}{
				"testbarrate_limit": "baz",
				"testbarbuffer":     "baz",
			},
			res: "testbarrate_limit",
		},
		{
			inputType: docs.TypeProcessor,
			inputConf: map[string]interface{}{
				"testbarprocessor": "baz",
				"testbarbuffer":    "baz",
			},
			res: "testbarprocessor",
		},
		{
			inputType: docs.TypeOutput,
			inputConf: map[string]interface{}{
				"testbaroutput": "baz",
				"testbarbuffer": "baz",
			},
			res: "testbaroutput",
		},
		{
			inputType: docs.TypeMetrics,
			inputConf: map[string]interface{}{
				"testfoometrics": "baz",
				"testbarbuffer":  "baz",
			},
			res: "testfoometrics",
		},
		{
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"testfooinput":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfooinput",
		},
		{
			inputType: docs.TypeCache,
			inputConf: map[string]interface{}{
				"testfoocache":  "baz",
				"testbarbuffer": "baz",
			},
			res: "testfoocache",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"testfoobuffer": "baz",
				"testbarbuffer": "baz",
			},
			err: "unable to infer buffer type, multiple candidates 'testbarbuffer' and 'testfoobuffer'",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"testfoobuffer": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"type":   "testfoobuffer",
				"foobar": "baz",
			},
			res: "testfoobuffer",
		},
		{
			inputType: docs.TypeBuffer,
			inputConf: map[string]interface{}{
				"type":   "notreal",
				"foobar": "baz",
			},
			err: "buffer type 'notreal' was not recognised",
		},
		{
			inputType: docs.TypeBuffer,
			err:       "invalid config value <nil>, expected object",
		},
	}

	for i, test := range tests {
		res, spec, err := docs.GetInferenceCandidate(test.inputType, test.inputDefault, test.inputConf)
		if len(test.err) > 0 {
			assert.EqualError(t, err, test.err, "test: %v", i)
		} else {
			assert.Equal(t, test.res, spec.Name)
			assert.Equal(t, test.inputType, spec.Type)
			assert.NoError(t, err)
			assert.Equal(t, test.res, res, "test: %v", i)
		}
	}
}

func TestSanitation(t *testing.T) {
	for _, t := range docs.Types() {
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testsanitfoo%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldCommon("foo1", ""),
				docs.FieldAdvanced("foo2", ""),
				docs.FieldCommon("foo3", "").HasType(docs.FieldProcessor),
				docs.FieldAdvanced("foo4", "").Array().HasType(docs.FieldProcessor),
				docs.FieldCommon("foo5", "").Map().HasType(docs.FieldProcessor),
				docs.FieldDeprecated("foo6"),
			),
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testsanitbar%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().Array().WithChildren(
				docs.FieldCommon("bar1", ""),
				docs.FieldAdvanced("bar2", ""),
				docs.FieldCommon("bar3", "").HasType(docs.FieldProcessor),
			),
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testsanitbaz%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().Map().WithChildren(
				docs.FieldCommon("baz1", ""),
				docs.FieldAdvanced("baz2", ""),
				docs.FieldCommon("baz3", "").HasType(docs.FieldProcessor),
			),
		})
	}

	type testCase struct {
		name        string
		inputType   docs.Type
		inputConf   interface{}
		inputFilter func(f docs.FieldSpec) bool

		res interface{}
		err string
	}

	tests := []testCase{
		{
			name:      "input with processors",
			inputType: docs.TypeInput,
			inputConf: map[string]interface{}{
				"testsanitfooinput": map[string]interface{}{
					"foo1": "simple field",
					"foo2": "advanced field",
					"foo6": "deprecated field",
				},
				"someotherinput": map[string]interface{}{
					"ignore": "me please",
				},
				"processors": []interface{}{
					map[string]interface{}{
						"testsanitbarprocessor": map[string]interface{}{
							"bar1": "bar value",
							"bar5": "undocumented field",
						},
						"someotherprocessor": map[string]interface{}{
							"ignore": "me please",
						},
					},
				},
			},
			res: map[string]interface{}{
				"testsanitfooinput": map[string]interface{}{
					"foo1": "simple field",
					"foo2": "advanced field",
					"foo6": "deprecated field",
				},
				"processors": []interface{}{
					map[string]interface{}{
						"testsanitbarprocessor": map[string]interface{}{
							"bar1": "bar value",
							"bar5": "undocumented field",
						},
					},
				},
			},
		},
		{
			name:      "output array with nested map processor",
			inputType: docs.TypeOutput,
			inputConf: map[string]interface{}{
				"testsanitbaroutput": []interface{}{
					map[string]interface{}{
						"bar1": "simple field",
						"bar3": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
							"someotherprocessor": map[string]interface{}{
								"ignore": "me please",
							},
						},
					},
					map[string]interface{}{
						"bar2": "advanced field",
					},
				},
			},
			res: map[string]interface{}{
				"testsanitbaroutput": []interface{}{
					map[string]interface{}{
						"bar1": "simple field",
						"bar3": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
						},
					},
					map[string]interface{}{
						"bar2": "advanced field",
					},
				},
			},
		},
		{
			name:      "metrics map with nested map processor",
			inputType: docs.TypeMetrics,
			inputConf: map[string]interface{}{
				"testsanitbazmetrics": map[string]interface{}{
					"customkey1": map[string]interface{}{
						"baz1": "simple field",
						"baz3": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
							"someotherprocessor": map[string]interface{}{
								"ignore": "me please",
							},
						},
					},
					"customkey2": map[string]interface{}{
						"baz2": "advanced field",
					},
				},
			},
			res: map[string]interface{}{
				"testsanitbazmetrics": map[string]interface{}{
					"customkey1": map[string]interface{}{
						"baz1": "simple field",
						"baz3": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
						},
					},
					"customkey2": map[string]interface{}{
						"baz2": "advanced field",
					},
				},
			},
		},
		{
			name:      "ratelimit with array field processor",
			inputType: docs.TypeRateLimit,
			inputConf: map[string]interface{}{
				"testsanitfoorate_limit": map[string]interface{}{
					"foo1": "simple field",
					"foo4": []interface{}{
						map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
							"someotherprocessor": map[string]interface{}{
								"ignore": "me please",
							},
						},
					},
				},
			},
			res: map[string]interface{}{
				"testsanitfoorate_limit": map[string]interface{}{
					"foo1": "simple field",
					"foo4": []interface{}{
						map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
						},
					},
				},
			},
		},
		{
			name:      "ratelimit with map field processor",
			inputType: docs.TypeRateLimit,
			inputConf: map[string]interface{}{
				"testsanitfoorate_limit": map[string]interface{}{
					"foo1": "simple field",
					"foo5": map[string]interface{}{
						"customkey1": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
							"someotherprocessor": map[string]interface{}{
								"ignore": "me please",
							},
						},
					},
				},
			},
			res: map[string]interface{}{
				"testsanitfoorate_limit": map[string]interface{}{
					"foo1": "simple field",
					"foo5": map[string]interface{}{
						"customkey1": map[string]interface{}{
							"testsanitbazprocessor": map[string]interface{}{
								"customkey1": map[string]interface{}{
									"baz1": "simple field",
								},
							},
						},
					},
				},
			},
		},
		{
			name:        "input with processors no deprecated",
			inputType:   docs.TypeInput,
			inputFilter: docs.ShouldDropDeprecated(true),
			inputConf: map[string]interface{}{
				"testsanitfooinput": map[string]interface{}{
					"foo1": "simple field",
					"foo2": "advanced field",
					"foo6": "deprecated field",
				},
				"someotherinput": map[string]interface{}{
					"ignore": "me please",
				},
				"processors": []interface{}{
					map[string]interface{}{
						"testsanitfooprocessor": map[string]interface{}{
							"foo1": "simple field",
							"foo2": "advanced field",
							"foo6": "deprecated field",
						},
						"someotherprocessor": map[string]interface{}{
							"ignore": "me please",
						},
					},
				},
			},
			res: map[string]interface{}{
				"testsanitfooinput": map[string]interface{}{
					"foo1": "simple field",
					"foo2": "advanced field",
				},
				"processors": []interface{}{
					map[string]interface{}{
						"testsanitfooprocessor": map[string]interface{}{
							"foo1": "simple field",
							"foo2": "advanced field",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := docs.SanitiseComponentConfig(test.inputType, test.inputConf, test.inputFilter)
			if len(test.err) > 0 {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.res, test.inputConf)
			}
		})
	}
}

func TestLinting(t *testing.T) {
	for _, t := range docs.Types() {
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testlintfoo%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldCommon("foo1", "").Linter(func(v interface{}) []docs.Lint {
					if v == "lint me please" {
						return []docs.Lint{
							docs.NewLintError(0, "this is a custom lint"),
						}
					}
					return nil
				}),
				docs.FieldAdvanced("foo2", "").OmitWhen(func(field, parent interface{}) (string, bool) {
					if field == "drop me" {
						return "because foo", true
					}
					return "", false
				}),
				docs.FieldCommon("foo3", "").HasType(docs.FieldProcessor),
				docs.FieldAdvanced("foo4", "").Array().HasType(docs.FieldProcessor),
				docs.FieldCommon("foo5", "").Map().HasType(docs.FieldProcessor),
				docs.FieldDeprecated("foo6"),
				docs.FieldAdvanced("foo7", "").Array().WithChildren(
					docs.FieldCommon("foochild1", ""),
				),
				docs.FieldAdvanced("foo8", "").Map().WithChildren(
					docs.FieldCommon("foochild1", ""),
				),
			),
		})
	}

	type testCase struct {
		name      string
		inputType docs.Type
		inputConf string

		res []docs.Lint
	}

	tests := []testCase{
		{
			name:      "ignores comments",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  # comment here
  foo1: hello world # And what's this?`,
		},
		{
			name:      "allows anchors",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput: &test-anchor
  foo1: hello world
processors:
  - testlintfooprocessor: *test-anchor`,
		},
		{
			name:      "lints through anchors",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput: &test-anchor
  foo1: hello world
  nope: bad field
processors:
  - testlintfooprocessor: *test-anchor`,
			res: []docs.Lint{
				docs.NewLintError(4, "field nope not recognised"),
			},
		},
		{
			name:      "unknown fields",
			inputType: docs.TypeInput,
			inputConf: `
type: testlintfooinput
testlintfooinput:
  not_recognised: yuh
  foo1: hello world
  also_not_recognised: nah
definitely_not_recognised: huh`,
			res: []docs.Lint{
				docs.NewLintError(4, "field not_recognised not recognised"),
				docs.NewLintError(6, "field also_not_recognised not recognised"),
				docs.NewLintError(7, "field definitely_not_recognised is invalid when the component type is testlintfooinput"),
			},
		},
		{
			name:      "reserved field unknown fields",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  not_recognised: yuh
  foo1: hello world
processors:
  - testlintfooprocessor:
      also_not_recognised: nah`,
			res: []docs.Lint{
				docs.NewLintError(3, "field not_recognised not recognised"),
				docs.NewLintError(7, "field also_not_recognised not recognised"),
			},
		},
		{
			name:      "empty processors",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: hello world
processors: []`,
			res: []docs.Lint{
				docs.NewLintError(4, "field processors is empty and can be removed"),
			},
		},
		{
			name:      "custom omit func",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: hello world
  foo2: drop me`,
			res: []docs.Lint{
				docs.NewLintError(4, "because foo"),
			},
		},
		{
			name:      "nested array not an array",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo4:
    key1:
      testlintfooprocessor:
        foo1: somevalue
        not_recognised: nah`,
			res: []docs.Lint{
				docs.NewLintError(4, "expected array value"),
			},
		},
		{
			name:      "nested fields",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo3:
    testlintfooprocessor:
      foo1: somevalue
      not_recognised: nah`,
			res: []docs.Lint{
				docs.NewLintError(6, "field not_recognised not recognised"),
			},
		},
		{
			name:      "nested map fields",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo5:
    key1:
      testlintfooprocessor:
        foo1: somevalue
        not_recognised: nah`,
			res: []docs.Lint{
				docs.NewLintError(7, "field not_recognised not recognised"),
			},
		},
		{
			name:      "nested map not a map",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo5:
    - testlintfooprocessor:
        foo1: somevalue
        not_recognised: nah`,
			res: []docs.Lint{
				docs.NewLintError(4, "expected object value"),
			},
		},
		{
			name:      "array field",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo7:
   - foochild1: yep`,
		},
		{
			name:      "array field bad",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo7:
   - wat: no`,
			res: []docs.Lint{
				docs.NewLintError(4, "field wat not recognised"),
			},
		},
		{
			name:      "array field not array",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo7:
    key1:
      wat: no`,
			res: []docs.Lint{
				docs.NewLintError(4, "expected array value"),
			},
		},
		{
			name:      "map field",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo8:
    key1:
      foochild1: yep`,
		},
		{
			name:      "map field bad",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo8:
    key1:
      wat: nope`,
			res: []docs.Lint{
				docs.NewLintError(5, "field wat not recognised"),
			},
		},
		{
			name:      "map field not map",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo8:
    - wat: nope`,
			res: []docs.Lint{
				docs.NewLintError(4, "expected object value"),
			},
		},
		{
			name:      "custom lint",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: lint me please`,
			res: []docs.Lint{
				docs.NewLintError(3, "this is a custom lint"),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))
			lints := docs.LintNode(test.inputType, node.Content[0])
			assert.Equal(t, test.res, lints)
		})
	}
}
