package docs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

func TestSecretScrubbingYAML(t *testing.T) {
	fields := docs.FieldSpecs{
		docs.FieldString("foo", "").Secret(),
		docs.FieldString("bar", ""),
		docs.FieldString("bazes", "").Secret().Array(),
		docs.FieldString("buzes", "").Secret().ArrayOfArrays(),
		docs.FieldString("bevers", "").Secret().Map(),
	}

	tests := []struct {
		name   string
		input  string
		output string
	}{
		{
			name: "all env vars",
			input: `foo: "${foo_value}"
bar: "${bar_value}"
bazes: [ "${baz_value_one}", "${baz_value_two}" ]
buzes: [ [ "${buz_value_one}" ], [ "${buz_value_two}", "${buz_value_three}" ] ]
bevers:
  first: "${bev_value_one}"
  second: "${bev_value_one}"
`,
			output: `foo: ${foo_value}
bar: "${bar_value}"
bazes: ['${baz_value_one}', '${baz_value_two}']
buzes: [['${buz_value_one}'], ['${buz_value_two}', '${buz_value_three}']]
bevers:
    first: ${bev_value_one}
    second: ${bev_value_one}
`,
		},
		{
			name: "all real secrets",
			input: `foo: dont print me!"
bar: you can print me
bazes: [ 'dont print me either', 'nor me' ]
buzes: [ [ 'and definitely' ], [ 'not any', 'of these' ] ]
bevers:
  first: dont even think
  second: about showing these ones
`,
			output: `foo: '!!!SECRET_SCRUBBED!!!'
bar: you can print me
bazes: ['!!!SECRET_SCRUBBED!!!', '!!!SECRET_SCRUBBED!!!']
buzes: [['!!!SECRET_SCRUBBED!!!'], ['!!!SECRET_SCRUBBED!!!', '!!!SECRET_SCRUBBED!!!']]
bevers:
    first: '!!!SECRET_SCRUBBED!!!'
    second: '!!!SECRET_SCRUBBED!!!'
`,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.input), &node))

			sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
			sanitConf.DocsProvider = docs.NewMappedDocsProvider()

			require.NoError(t, fields.SanitiseYAML(&node, sanitConf))

			resBytes, err := yaml.Marshal(node.Content[0])
			require.NoError(t, err)
			assert.Equal(t, test.output, string(resBytes))
		})
	}
}

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

	assert.Equal(t, map[string]any{
		"a": "setavalue",
		"b": 11,
		"c": map[string]any{
			"d": true,
			"e": "evalue",
			"f": map[string]any{
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
		result any
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
			result: map[string]any{
				"a": "no",
				"b": "false",
				"c": "10",
				"d": "30.4",
				"e": []any{
					"no", "false", "10",
				},
				"f": map[string]any{
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
			result: map[string]any{
				"a": false,
				"b": false,
				"c": true,
				"d": []any{
					false, false, true,
				},
				"e": map[string]any{
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
			result: map[string]any{
				"a": 11,
				"b": -12,
				"c": 13,
				"d": []any{
					11, -12, 13,
				},
				"e": map[string]any{
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
			result: map[string]any{
				"a": 11.0,
				"b": -12.0,
				"c": 13.4,
				"d": []any{
					11.0, -12.0, 13.4,
				},
				"e": map[string]any{
					"1": 11.0, "2": -12.0, "3": 13.4,
				},
			},
		},
		{
			name: "recurse array of objects",
			spec: docs.FieldSpecs{
				docs.FieldObject("foo", "").WithChildren(
					docs.FieldObject("eels", "").Array().WithChildren(
						docs.FieldString("bar", "").HasDefault("default"),
					),
				),
			},
			yaml: `
foo:
  eels:
    - bar: bar1
    - bar: bar2
`,
			result: map[string]any{
				"foo": map[string]any{
					"eels": []any{
						map[string]any{
							"bar": "bar1",
						},
						map[string]any{
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
					docs.FieldObject("eels", "").Map().WithChildren(
						docs.FieldString("bar", "").HasDefault("default"),
					),
				),
			},
			yaml: `
foo:
  eels:
    first:
      bar: bar1
    second:
      bar: bar2
`,
			result: map[string]any{
				"foo": map[string]any{
					"eels": map[string]any{
						"first": map[string]any{
							"bar": "bar1",
						},
						"second": map[string]any{
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
			result: map[string]any{
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
			result: map[string]any{
				"a": "adefault",
				"b": []any{
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
			result: map[string]any{
				"a": "adefault",
				"b": map[string]any{
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
			result: map[string]any{
				"foo": []any{
					[]any{"bar1", "bar2"},
					[]any{"bar3"},
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
			result: map[string]any{
				"foo": []any{
					[]any{3, 4}, []any{5},
				},
				"bar": []any{
					[]any{3.3, 4.4}, []any{5.5},
				},
				"baz": []any{
					[]any{true, false}, []any{true},
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
			expected: `null # No default (required)
`,
		},
		{
			name: "no recurse with children",
			spec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
				docs.FieldString("baz", ""),
			),
			expected: `{} # No default (required)
`,
		},
		{
			name: "no recurse map",
			spec: docs.FieldString("foo", "").Map(),
			expected: `{} # No default (required)
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
			expected: `bar: "" # No default (required)
baz: baz default
buz: 0 # No default (required)
bev: 0 # No default (required)
bun: false # No default (required)
bud: [] # No default (required)
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
	prov := docs.NewMappedDocsProvider()

	for _, t := range docs.Types() {
		prov.RegisterDocs(docs.ComponentSpec{
			Name:   "resource",
			Type:   t,
			Config: docs.FieldString("", ""),
		})
		prov.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testlintfoo%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldString("foo1", "").LinterFunc(func(ctx docs.LintContext, line, col int, v any) []docs.Lint {
					if v == "lint me please" {
						return []docs.Lint{
							docs.NewLintError(line, docs.LintCustom, errors.New("this is a custom lint")),
						}
					}
					return nil
				}).Optional(),
				docs.FieldString("foo2", "").Advanced().OmitWhen(func(field, parent any) (string, bool) {
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
		prov.RegisterDocs(docs.ComponentSpec{
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
		requireLabels    bool

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
				docs.NewLintError(2, docs.LintDeprecated, errors.New("component testlintbarinput is deprecated")),
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
				docs.NewLintError(4, docs.LintDeprecated, errors.New("field foo6 is deprecated")),
			},
		},
		{
			name:      "require label",
			inputType: docs.TypeInput,
			inputConf: `
testlintbarinput:
  bar1: hello world`,
			requireLabels: true,
			res: []docs.Lint{
				docs.NewLintError(2, docs.LintMissingLabel, errors.New("label is required for testlintbarinput")),
			},
		},
		{
			name:      "do not require label for resource",
			inputType: docs.TypeInput,
			inputConf: `
resource: something`,
			requireLabels: true,
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
				docs.NewLintError(4, docs.LintUnknown, errors.New("field nope not recognised")),
				docs.NewLintError(4, docs.LintUnknown, errors.New("field nope not recognised")),
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
				docs.NewLintError(4, docs.LintUnknown, errors.New("field not_recognised not recognised")),
				docs.NewLintError(6, docs.LintUnknown, errors.New("field also_not_recognised not recognised")),
				docs.NewLintError(7, docs.LintUnknown, errors.New("field definitely_not_recognised is invalid when the component type is testlintfooinput (input)")),
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
				docs.NewLintError(3, docs.LintUnknown, errors.New("field not_recognised not recognised")),
				docs.NewLintError(7, docs.LintUnknown, errors.New("field also_not_recognised not recognised")),
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
				docs.NewLintError(8, docs.LintDuplicateLabel, errors.New("label 'foo' collides with a previously defined label at line 2")),
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
				docs.NewLintError(4, docs.LintShouldOmit, errors.New("field processors is empty and can be removed")),
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
				docs.NewLintError(4, docs.LintShouldOmit, errors.New("because foo")),
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
				docs.NewLintError(4, docs.LintExpectedArray, errors.New("expected array value")),
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
				docs.NewLintError(6, docs.LintUnknown, errors.New("field not_recognised not recognised")),
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
				docs.NewLintError(5, docs.LintExpectedScalar, errors.New("expected string value")),
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
				docs.NewLintError(7, docs.LintUnknown, errors.New("field not_recognised not recognised")),
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
				docs.NewLintError(4, docs.LintExpectedObject, errors.New("expected object value, got !!seq")),
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
				docs.NewLintError(4, docs.LintUnknown, errors.New("field wat not recognised")),
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
				docs.NewLintError(4, docs.LintExpectedArray, errors.New("expected array value")),
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
				docs.NewLintError(5, docs.LintUnknown, errors.New("field wat not recognised")),
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
				docs.NewLintError(4, docs.LintExpectedObject, errors.New("expected object value, got !!seq")),
			},
		},
		{
			name:      "custom lint",
			inputType: docs.TypeInput,
			inputConf: `
testlintfooinput:
  foo1: lint me please`,
			res: []docs.Lint{
				docs.NewLintError(3, docs.LintCustom, errors.New("this is a custom lint")),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			lConf := docs.NewLintConfig(bundle.GlobalEnvironment)
			lConf.RejectDeprecated = test.rejectDeprecated
			lConf.RequireLabels = test.requireLabels
			lConf.DocsProvider = prov

			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))
			lints := docs.LintYAML(docs.NewLintContext(lConf), test.inputType, &node)
			assert.Equal(t, test.res, lints)
		})
	}
}

func TestYAMLLintYAMLMerge(t *testing.T) {
	prov := docs.NewMappedDocsProvider()
	prov.RegisterDocs(docs.ComponentSpec{
		Name: "meowthing",
		Type: docs.TypeInput,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("foo", ""),
			docs.FieldString("bar", "").Optional(),
		),
	})

	lConf := docs.NewLintConfig(bundle.GlobalEnvironment)
	lConf.DocsProvider = prov

	lintConf := func(t *testing.T, name, conf string, expected []docs.Lint) {
		t.Run(name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(conf), &node))
			lints := docs.FieldInput("root", "").Map().LintYAML(docs.NewLintContext(lConf), &node)
			assert.Equal(t, expected, lints)
		})
	}

	lintConf(t, "no lint errors", `
first:
  meowthing: &a
    foo: one
second:
  meowthing:
    <<: *a
    bar: two
`, nil)

	lintConf(t, "unknown field from", `
first:
  meowthing: &a
    foo: one
    baz: three
second:
  meowthing:
    <<: *a
    bar: two
`, []docs.Lint{
		{Line: 5, Column: 1, Level: docs.LintError, Type: docs.LintUnknown, What: "field baz not recognised"},
		{Line: 5, Column: 1, Level: docs.LintError, Type: docs.LintUnknown, What: "field baz not recognised"},
	})

	lintConf(t, "unknown field into", `
first:
  meowthing: &a
    foo: one
    bar: two
second:
  meowthing:
    <<: *a
    baz: three
`, []docs.Lint{
		{Line: 9, Column: 1, Level: docs.LintError, Type: docs.LintUnknown, What: "field baz not recognised"},
	})
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
				docs.NewLintError(1, docs.LintExpectedScalar, errors.New("expected string value")),
			},
		},
		{
			name:      "expected array got string",
			inputSpec: docs.FieldString("foo", "").Array(),
			inputConf: `"foo"`,
			res: []docs.Lint{
				docs.NewLintError(1, docs.LintExpectedArray, errors.New("expected array value")),
			},
		},
		{
			name: "expected object got string",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
			),
			inputConf: `"foo"`,
			res: []docs.Lint{
				docs.NewLintError(1, docs.LintExpectedObject, errors.New("expected object value, got !!str")),
			},
		},
		{
			name: "expected string got object",
			inputSpec: docs.FieldObject("foo", "").WithChildren(
				docs.FieldString("bar", ""),
			),
			inputConf: `bar: {}`,
			res: []docs.Lint{
				docs.NewLintError(1, docs.LintExpectedScalar, errors.New("expected string value")),
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
				docs.NewLintError(2, docs.LintExpectedScalar, errors.New("expected string value")),
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
				docs.NewLintError(1, docs.LintMissing, errors.New("field baz is required")),
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			require.NoError(t, yaml.Unmarshal([]byte(test.inputConf), &node))

			lints := test.inputSpec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), &node)
			assert.Equal(t, test.res, lints)
		})
	}
}

func TestYAMLSanitation(t *testing.T) {
	prov := docs.NewMappedDocsProvider()

	for _, t := range docs.Types() {
		prov.RegisterDocs(docs.ComponentSpec{
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
		prov.RegisterDocs(docs.ComponentSpec{
			Name: fmt.Sprintf("testyamlsanitbar%v", string(t)),
			Type: t,
			Config: docs.FieldComponent().Array().WithChildren(
				docs.FieldString("bar1", ""),
				docs.FieldString("bar2", "").Advanced(),
				docs.FieldProcessor("bar3", ""),
			),
		})
		prov.RegisterDocs(docs.ComponentSpec{
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
		inputFilter func(f docs.FieldSpec, v any) bool

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

			sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
			sanitConf.DocsProvider = prov
			sanitConf.RemoveTypeField = true
			sanitConf.Filter = test.inputFilter
			sanitConf.RemoveDeprecated = false

			err := docs.SanitiseYAML(test.inputType, &node, sanitConf)
			if test.err != "" {
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
