package docs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

func TestFieldsFromNode(t *testing.T) {
	tests := []struct {
		name   string
		yaml   string
		fields docs.FieldSpecs
	}{
		{
			name: "flat object",
			yaml: `a: foo
b: bar
c: 21`,
			fields: docs.FieldSpecs{
				docs.FieldString("a", "").HasDefault("foo"),
				docs.FieldString("b", "").HasDefault("bar"),
				docs.FieldInt("c", "").HasDefault(int64(21)),
			},
		},
		{
			name: "nested object",
			yaml: `a: foo
b:
  d: bar
  e: 22
c: true`,
			fields: docs.FieldSpecs{
				docs.FieldString("a", "").HasDefault("foo"),
				docs.FieldObject("b", "").WithChildren(
					docs.FieldString("d", "").HasDefault("bar"),
					docs.FieldInt("e", "").HasDefault(int64(22)),
				),
				docs.FieldBool("c", "").HasDefault(true),
			},
		},
		{
			name: "array of strings",
			yaml: `a:
- foo`,
			fields: docs.FieldSpecs{
				docs.FieldString("a", "").Array().HasDefault([]string{"foo"}),
			},
		},
		{
			name: "array of ints",
			yaml: `a:
- 5
- 8`,
			fields: docs.FieldSpecs{
				docs.FieldInt("a", "").Array().HasDefault([]int64{5, 8}),
			},
		},
		{
			name: "nested array of strings",
			yaml: `a:
  b:
    - foo
    - bar`,
			fields: docs.FieldSpecs{
				docs.FieldObject("a", "").WithChildren(
					docs.FieldString("b", "").Array().HasDefault([]string{"foo", "bar"}),
				),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.yaml)

			var node yaml.Node
			require.NoError(t, yaml.Unmarshal(confBytes, &node))

			assert.Equal(t, test.fields, docs.FieldsFromYAML(&node))
		})
	}
}

func TestFieldsNodeToMap(t *testing.T) {
	spec := docs.FieldSpecs{
		docs.FieldString("a", ""),
		docs.FieldInt("b", "").HasDefault(11),
		docs.FieldObject("c", "").WithChildren(
			docs.FieldBool("d", "").HasDefault(true),
			docs.FieldString("e", "").HasDefault("evalue"),
			docs.FieldObject("f", "").WithChildren(
				docs.FieldInt("g", "").HasDefault(12),
				docs.FieldString("h", ""),
				docs.FieldFloat("i", "").HasDefault(13.0),
			),
		),
	}

	var node yaml.Node
	err := yaml.Unmarshal([]byte(`
a: setavalue
c:
  f:
    g: 22
    h: sethvalue
    i: 23.1
`), &node)
	require.NoError(t, err)

	generic, err := spec.YAMLToMap(&node, docs.ToValueConfig{})
	require.NoError(t, err)

	assert.Equal(t, map[string]interface{}{
		"a": "setavalue",
		"b": 11,
		"c": map[string]interface{}{
			"d": true,
			"e": "evalue",
			"f": map[string]interface{}{
				"g": 22,
				"h": "sethvalue",
				"i": 23.1,
			},
		},
	}, generic)
}

func TestFieldsNodeToMapTypeCoercion(t *testing.T) {
	tests := []struct {
		name   string
		spec   docs.FieldSpecs
		yaml   string
		result interface{}
	}{
		{
			name: "string fields",
			spec: docs.FieldSpecs{
				docs.FieldString("a", ""),
				docs.FieldString("b", ""),
				docs.FieldString("c", ""),
				docs.FieldString("d", ""),
				docs.FieldString("e", "").Array(),
				docs.FieldString("f", "").Map(),
			},
			yaml: `
a: no
b: false
c: 10
d: 30.4
e:
 - no
 - false
 - 10
f:
 "1": no
 "2": false
 "3": 10
`,
			result: map[string]interface{}{
				"a": "no",
				"b": "false",
				"c": "10",
				"d": "30.4",
				"e": []interface{}{
					"no", "false", "10",
				},
				"f": map[string]interface{}{
					"1": "no", "2": "false", "3": "10",
				},
			},
		},
		{
			name: "bool fields",
			spec: docs.FieldSpecs{
				docs.FieldBool("a", ""),
				docs.FieldBool("b", ""),
				docs.FieldBool("c", ""),
				docs.FieldBool("d", "").Array(),
				docs.FieldBool("e", "").Map(),
			},
			yaml: `
a: no
b: false
c: true
d:
 - no
 - false
 - true
e:
 "1": no
 "2": false
 "3": true
`,
			result: map[string]interface{}{
				"a": false,
				"b": false,
				"c": true,
				"d": []interface{}{
					false, false, true,
				},
				"e": map[string]interface{}{
					"1": false, "2": false, "3": true,
				},
			},
		},
		{
			name: "int fields",
			spec: docs.FieldSpecs{
				docs.FieldInt("a", ""),
				docs.FieldInt("b", ""),
				docs.FieldInt("c", ""),
				docs.FieldInt("d", "").Array(),
				docs.FieldInt("e", "").Map(),
			},
			yaml: `
a: 11
b: -12
c: 13.4
d:
 - 11
 - -12
 - 13.4
e:
 "1": 11
 "2": -12
 "3": 13.4
`,
			result: map[string]interface{}{
				"a": 11,
				"b": -12,
				"c": 13,
				"d": []interface{}{
					11, -12, 13,
				},
				"e": map[string]interface{}{
					"1": 11, "2": -12, "3": 13,
				},
			},
		},
		{
			name: "float fields",
			spec: docs.FieldSpecs{
				docs.FieldFloat("a", ""),
				docs.FieldFloat("b", ""),
				docs.FieldFloat("c", ""),
				docs.FieldFloat("d", "").Array(),
				docs.FieldFloat("e", "").Map(),
			},
			yaml: `
a: 11
b: -12
c: 13.4
d:
 - 11
 - -12
 - 13.4
e:
 "1": 11
 "2": -12
 "3": 13.4
`,
			result: map[string]interface{}{
				"a": 11.0,
				"b": -12.0,
				"c": 13.4,
				"d": []interface{}{
					11.0, -12.0, 13.4,
				},
				"e": map[string]interface{}{
					"1": 11.0, "2": -12.0, "3": 13.4,
				},
			},
		},
		{
			name: "recurse array of objects",
			spec: docs.FieldSpecs{
				docs.FieldObject("foo", "").WithChildren(
					docs.FieldObject("eles", "").Array().WithChildren(
						docs.FieldString("bar", "").HasDefault("default"),
					),
				),
			},
			yaml: `
foo:
  eles:
    - bar: bar1
    - bar: bar2
`,
			result: map[string]interface{}{
				"foo": map[string]interface{}{
					"eles": []interface{}{
						map[string]interface{}{
							"bar": "bar1",
						},
						map[string]interface{}{
							"bar": "bar2",
						},
					},
				},
			},
		},
		{
			name: "recurse map of objects",
			spec: docs.FieldSpecs{
				docs.FieldObject("foo", "").WithChildren(
					docs.FieldObject("eles", "").Map().WithChildren(
						docs.FieldString("bar", "").HasDefault("default"),
					),
				),
			},
			yaml: `
foo:
  eles:
    first:
      bar: bar1
    second:
      bar: bar2
`,
			result: map[string]interface{}{
				"foo": map[string]interface{}{
					"eles": map[string]interface{}{
						"first": map[string]interface{}{
							"bar": "bar1",
						},
						"second": map[string]interface{}{
							"bar": "bar2",
						},
					},
				},
			},
		},
		{
			name: "component field",
			spec: docs.FieldSpecs{
				docs.FieldString("a", "").HasDefault("adefault"),
				docs.FieldProcessor("b", ""),
				docs.FieldBool("c", ""),
			},
			yaml: `
b:
  bloblang: 'root = "hello world"'
c: true
`,
			result: map[string]interface{}{
				"a": "adefault",
				"b": &yaml.Node{
					Kind:   yaml.MappingNode,
					Tag:    "!!map",
					Line:   3,
					Column: 3,
					Content: []*yaml.Node{
						{
							Kind:   yaml.ScalarNode,
							Tag:    "!!str",
							Value:  "bloblang",
							Line:   3,
							Column: 3,
						},
						{
							Kind:   yaml.ScalarNode,
							Style:  yaml.SingleQuotedStyle,
							Tag:    "!!str",
							Value:  `root = "hello world"`,
							Line:   3,
							Column: 13,
						},
					},
				},
				"c": true,
			},
		},
		{
			name: "component field in array",
			spec: docs.FieldSpecs{
				docs.FieldString("a", "").HasDefault("adefault"),
				docs.FieldProcessor("b", "").Array(),
				docs.FieldBool("c", ""),
			},
			yaml: `
b:
  - bloblang: 'root = "hello world"'
c: true
`,
			result: map[string]interface{}{
				"a": "adefault",
				"b": []interface{}{
					&yaml.Node{
						Kind:   yaml.MappingNode,
						Tag:    "!!map",
						Line:   3,
						Column: 5,
						Content: []*yaml.Node{
							{
								Kind:   yaml.ScalarNode,
								Tag:    "!!str",
								Value:  "bloblang",
								Line:   3,
								Column: 5,
							},
							{
								Kind:   yaml.ScalarNode,
								Style:  yaml.SingleQuotedStyle,
								Tag:    "!!str",
								Value:  `root = "hello world"`,
								Line:   3,
								Column: 15,
							},
						},
					},
				},
				"c": true,
			},
		},
		{
			name: "component field in map",
			spec: docs.FieldSpecs{
				docs.FieldString("a", "").HasDefault("adefault"),
				docs.FieldProcessor("b", "").Map(),
				docs.FieldBool("c", ""),
			},
			yaml: `
b:
  foo:
    bloblang: 'root = "hello world"'
c: true
`,
			result: map[string]interface{}{
				"a": "adefault",
				"b": map[string]interface{}{
					"foo": &yaml.Node{
						Kind:   yaml.MappingNode,
						Tag:    "!!map",
						Line:   4,
						Column: 5,
						Content: []*yaml.Node{
							{
								Kind:   yaml.ScalarNode,
								Tag:    "!!str",
								Value:  "bloblang",
								Line:   4,
								Column: 5,
							},
							{
								Kind:   yaml.ScalarNode,
								Style:  yaml.SingleQuotedStyle,
								Tag:    "!!str",
								Value:  `root = "hello world"`,
								Line:   4,
								Column: 15,
							},
						},
					},
				},
				"c": true,
			},
		},
		{
			name: "array of array of string",
			spec: docs.FieldSpecs{
				docs.FieldString("foo", "").ArrayOfArrays(),
			},
			yaml: `
foo:
  -
    - bar1
    - bar2
  -
    - bar3
`,
			result: map[string]interface{}{
				"foo": []interface{}{
					[]interface{}{"bar1", "bar2"},
					[]interface{}{"bar3"},
				},
			},
		},
		{
			name: "array of array of int, float and bool",
			spec: docs.FieldSpecs{
				docs.FieldInt("foo", "").ArrayOfArrays(),
				docs.FieldFloat("bar", "").ArrayOfArrays(),
				docs.FieldBool("baz", "").ArrayOfArrays(),
			},
			yaml: `
foo: [[3,4],[5]]
bar: [[3.3,4.4],[5.5]]
baz: [[true,false],[true]]
`,
			result: map[string]interface{}{
				"foo": []interface{}{
					[]interface{}{3, 4}, []interface{}{5},
				},
				"bar": []interface{}{
					[]interface{}{3.3, 4.4}, []interface{}{5.5},
				},
				"baz": []interface{}{
					[]interface{}{true, false}, []interface{}{true},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			err := yaml.Unmarshal([]byte(test.yaml), &node)
			require.NoError(t, err)

			generic, err := test.spec.YAMLToMap(&node, docs.ToValueConfig{})
			require.NoError(t, err)

			assert.Equal(t, test.result, generic)
		})
	}
}

func TestFieldToNode(t *testing.T) {
	tests := []struct {
		name     string
		spec     docs.FieldSpec
		recurse  bool
		expected string
	}{
		{
			name: "no recurse single node null",
			spec: docs.FieldObject("foo", ""),
			expected: `null
`,
		},
		{
			name: "no recurse with children",
			spec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
				docs.FieldString("baz", ""),
			),
			expected: `{}
`,
		},
		{
			name: "no recurse map",
			spec: docs.FieldString("foo", "").Map(),
			expected: `{}
`,
		},
		{
			name: "recurse with children",
			spec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
				docs.FieldString("baz", "").HasDefault("baz default"),
				docs.FieldInt("buz", ""),
				docs.FieldFloat("bev", ""),
				docs.FieldBool("bun", ""),
				docs.FieldString("bud", "").Array(),
			),
			recurse: true,
			expected: `bar: ""
baz: baz default
buz: 0
bev: 0
bun: false
bud: []
`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			n, err := test.spec.ToYAML(test.recurse)
			require.NoError(t, err)

			b, err := yaml.Marshal(n)
			require.NoError(t, err)

			assert.Equal(t, test.expected, string(b))
		})
	}
}

func TestYAMLComponentLinting(t *testing.T) {
	for _, t := range docs.Types() {
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testlintfoo%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldString("foo1", "").LinterFunc(func(ctx docs.LintContext, line, col int, v interface{}) []docs.Lint {
					if v == "lint me please" {
						return []docs.Lint{
							docs.NewLintError(line, "this is a custom lint"),
						}
					}
					return nil
				}).Optional(),
				docs.FieldString("foo2", "").Advanced().OmitWhen(func(field, parent interface{}) (string, bool) {
					if field == "drop me" {
						return "because foo", true
					}
					return "", false
				}).Optional(),
				docs.FieldProcessor("foo3", "").Optional(),
				docs.FieldProcessor("foo4", "").Array().Advanced().Optional(),
				docs.FieldProcessor("foo5", "").Map().Optional(),
				docs.FieldString("foo6", "").Optional().Deprecated(),
				docs.FieldObject("foo7", "").Array().WithChildren(
					docs.FieldString("foochild1", "").Optional(),
				).Optional().Advanced(),
				docs.FieldObject("foo8", "").Map().WithChildren(
					docs.FieldInt("foochild1", "").Optional(),
				).Optional().Advanced(),
			),
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name:   fmt.Sprintf("testlintbar%v", string(t)),
			Type:   t,
			Status: docs.StatusDeprecated,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldString("bar1", "").Optional(),
			),
		})
	}

	type testCase struct {
		name             string
		inputType        docs.Type
		inputConf        string
		rejectDeprecated bool

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
			name:      "no problem with deprecated component",
			inputType: docs.TypeInput,
			inputConf: `
testlintbarinput:
  bar1: hello world`,
		},
		{
			name:      "no problem with deprecated fields",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: hello world
  foo6: hello world`,
		},
		{
			name:      "reject deprecated component",
			inputType: docs.TypeInput,
			inputConf: `
testlintbarinput:
  bar1: hello world`,
			rejectDeprecated: true,
			res: []docs.Lint{
				docs.NewLintError(2, "component testlintbarinput is deprecated"),
			},
		},
		{
			name:      "reject deprecated fields",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: hello world
  foo6: hello world`,
			rejectDeprecated: true,
			res: []docs.Lint{
				docs.NewLintError(4, "field foo6 is deprecated"),
			},
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
				docs.NewLintError(7, "field definitely_not_recognised is invalid when the component type is testlintfooinput (input)"),
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
			name:      "collision of labels",
			inputType: docs.TypeInput,
			inputConf: `
label: foo
testlintfooinput:
  foo1: hello world
processors:
  - label: bar
    testlintfooprocessor: {}
  - label: foo
    testlintfooprocessor: {}`,
			res: []docs.Lint{
				docs.NewLintError(8, "Label 'foo' collides with a previously defined label at line 2"),
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
			name:      "array for string",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo3:
    testlintfooprocessor:
      foo1: [ somevalue ]
`,
			res: []docs.Lint{
				docs.NewLintError(5, "expected string value"),
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
      foochild1: 10`,
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
			lintCtx := docs.NewLintContext()
			lintCtx.RejectDeprecated = test.rejectDeprecated

			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))
			lints := docs.LintYAML(lintCtx, test.inputType, &node)
			assert.Equal(t, test.res, lints)
		})
	}
}

func TestYAMLLinting(t *testing.T) {
	type testCase struct {
		name      string
		inputSpec docs.FieldSpec
		inputConf string

		res []docs.Lint
	}

	tests := []testCase{
		{
			name:      "expected string got array",
			inputSpec: docs.FieldString("foo", ""),
			inputConf: `["foo","bar"]`,
			res: []docs.Lint{
				docs.NewLintError(1, "expected string value"),
			},
		},
		{
			name:      "expected array got string",
			inputSpec: docs.FieldString("foo", "").Array(),
			inputConf: `"foo"`,
			res: []docs.Lint{
				docs.NewLintError(1, "expected array value"),
			},
		},
		{
			name: "expected object got string",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
			),
			inputConf: `"foo"`,
			res: []docs.Lint{
				docs.NewLintError(1, "expected object value"),
			},
		},
		{
			name: "expected string got object",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
			),
			inputConf: `bar: {}`,
			res: []docs.Lint{
				docs.NewLintError(1, "expected string value"),
			},
		},
		{
			name: "expected string got object nested",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldObject("bar", "").WithChildren(
					docs.FieldString("baz", ""),
				),
			),
			inputConf: `bar:
  baz: {}`,
			res: []docs.Lint{
				docs.NewLintError(2, "expected string value"),
			},
		},
		{
			name: "missing non-optional field",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", "").HasDefault("barv"),
				docs.FieldString("baz", ""),
				docs.FieldString("buz", "").Optional(),
				docs.FieldString("bev", ""),
			),
			inputConf: `bev: hello world`,
			res: []docs.Lint{
				docs.NewLintError(1, "field baz is required"),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))

			lints := test.inputSpec.LintYAML(docs.NewLintContext(), &node)
			assert.Equal(t, test.res, lints)
		})
	}
}

func TestYAMLSanitation(t *testing.T) {
	for _, t := range docs.Types() {
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testyamlsanitfoo%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldString("foo1", ""),
				docs.FieldString("foo2", "").Advanced(),
				docs.FieldProcessor("foo3", ""),
				docs.FieldProcessor("foo4", "").Array().Advanced(),
				docs.FieldProcessor("foo5", "").Map(),
				docs.FieldString("foo6", "").Deprecated(),
			),
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testyamlsanitbar%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().Array().WithChildren(
				docs.FieldString("bar1", ""),
				docs.FieldString("bar2", "").Advanced(),
				docs.FieldProcessor("bar3", ""),
			),
		})
		docs.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testyamlsanitbaz%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().Map().WithChildren(
				docs.FieldString("baz1", ""),
				docs.FieldString("baz2", "").Advanced(),
				docs.FieldProcessor("baz3", ""),
			),
		})
	}

	type testCase struct {
		name        string
		inputType   docs.Type
		inputConf   string
		inputFilter func(f docs.FieldSpec) bool

		res string
		err string
	}

	tests := []testCase{
		{
			name:      "input with processors",
			inputType: docs.TypeInput,
			inputConf: `testyamlsanitfooinput:
  foo1: simple field
  foo2: advanced field
  foo6: deprecated field
someotherinput:
  ignore: me please
processors:
  - testyamlsanitbarprocessor:
      bar1: bar value
      bar5: undocumented field
    someotherprocessor:
      ignore: me please
`,
			res: `testyamlsanitfooinput:
    foo1: simple field
    foo2: advanced field
    foo6: deprecated field
processors:
    - testyamlsanitbarprocessor:
        bar1: bar value
        bar5: undocumented field
`,
		},
		{
			name:      "output array with nested map processor",
			inputType: docs.TypeOutput,
			inputConf: `testyamlsanitbaroutput:
    - bar1: simple field
      bar3:
          testyamlsanitbazprocessor:
              customkey1:
                  baz1: simple field
          someotherprocessor:
             ignore: me please
    - bar2: advanced field
`,
			res: `testyamlsanitbaroutput:
    - bar1: simple field
      bar3:
        testyamlsanitbazprocessor:
            customkey1:
                baz1: simple field
    - bar2: advanced field
`,
		},
		{
			name:      "output with empty processors",
			inputType: docs.TypeOutput,
			inputConf: `testyamlsanitbaroutput:
    - bar1: simple field
processors: []
`,
			res: `testyamlsanitbaroutput:
    - bar1: simple field
`,
		},
		{
			name:      "metrics map with nested map processor",
			inputType: docs.TypeMetrics,
			inputConf: `testyamlsanitbazmetrics:
  customkey1:
    baz1: simple field
    baz3:
      testyamlsanitbazprocessor:
        customkey1:
          baz1: simple field
      someotherprocessor:
        ignore: me please
  customkey2:
    baz2: advanced field
`,
			res: `testyamlsanitbazmetrics:
    customkey1:
        baz1: simple field
        baz3:
            testyamlsanitbazprocessor:
                customkey1:
                    baz1: simple field
    customkey2:
        baz2: advanced field
`,
		},
		{
			name:      "ratelimit with array field processor",
			inputType: docs.TypeRateLimit,
			inputConf: `testyamlsanitfoorate_limit:
    foo1: simple field
    foo4:
      - testyamlsanitbazprocessor:
            customkey1:
                baz1: simple field
        someotherprocessor:
            ignore: me please
`,
			res: `testyamlsanitfoorate_limit:
    foo1: simple field
    foo4:
        - testyamlsanitbazprocessor:
            customkey1:
                baz1: simple field
`,
		},
		{
			name:      "ratelimit with map field processor",
			inputType: docs.TypeRateLimit,
			inputConf: `testyamlsanitfoorate_limit:
    foo1: simple field
    foo5:
        customkey1:
            testyamlsanitbazprocessor:
                customkey1:
                    baz1: simple field
            someotherprocessor:
                ignore: me please
`,
			res: `testyamlsanitfoorate_limit:
    foo1: simple field
    foo5:
        customkey1:
            testyamlsanitbazprocessor:
                customkey1:
                    baz1: simple field
`,
		},
		{
			name:        "input with processors no deprecated",
			inputType:   docs.TypeInput,
			inputFilter: docs.ShouldDropDeprecated(true),
			inputConf: `testyamlsanitfooinput:
    foo1: simple field
    foo2: advanced field
    foo6: deprecated field
someotherinput:
    ignore: me please
processors:
    - testyamlsanitfooprocessor:
        foo1: simple field
        foo2: advanced field
        foo6: deprecated field
      someotherprocessor:
        ignore: me please
`,
			res: `testyamlsanitfooinput:
    foo1: simple field
    foo2: advanced field
processors:
    - testyamlsanitfooprocessor:
        foo1: simple field
        foo2: advanced field
`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))
			err := docs.SanitiseYAML(test.inputType, &node, docs.SanitiseConfig{
				RemoveTypeField:  true,
				Filter:           test.inputFilter,
				RemoveDeprecated: false,
			})
			if len(test.err) > 0 {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)

				resBytes, err := yaml.Marshal(node.Content[0])
				require.NoError(t, err)
				assert.Equal(t, test.res, string(resBytes))
			}
		})
	}
}
