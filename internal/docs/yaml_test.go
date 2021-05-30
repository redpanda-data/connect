package docs_test

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
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
				docs.FieldCommon("a", ""),
				docs.FieldCommon("b", ""),
				docs.FieldCommon("c", ""),
			},
		},
		{
			name: "nested object",
			yaml: `a: foo
b:
  d: bar
  e: 22
c: 21`,
			fields: docs.FieldSpecs{
				docs.FieldCommon("a", ""),
				docs.FieldCommon("b", "").WithChildren(
					docs.FieldCommon("d", ""),
					docs.FieldCommon("e", ""),
				),
				docs.FieldCommon("c", ""),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.yaml)

			var node yaml.Node
			require.NoError(t, yaml.Unmarshal(confBytes, &node))

			if node.Kind == yaml.DocumentNode && node.Content[0].Kind == yaml.MappingNode {
				node = *node.Content[0]
			}

			assert.Equal(t, test.fields, docs.FieldsFromNode(&node))
		})
	}
}

func TestFieldsNodeToMap(t *testing.T) {
	spec := docs.FieldSpecs{
		docs.FieldCommon("a", ""),
		docs.FieldCommon("b", "").HasDefault(11),
		docs.FieldCommon("c", "").WithChildren(
			docs.FieldCommon("d", "").HasDefault(true),
			docs.FieldCommon("e", "").HasDefault("evalue"),
			docs.FieldCommon("f", "").WithChildren(
				docs.FieldCommon("g", "").HasDefault(12),
				docs.FieldCommon("h", ""),
				docs.FieldCommon("i", "").HasDefault(13),
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

	if node.Kind == yaml.DocumentNode && node.Content[0].Kind == yaml.MappingNode {
		node = *node.Content[0]
	}

	generic, err := spec.NodeToMap(&node)
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
				docs.FieldCommon("a", "").HasType("string"),
				docs.FieldCommon("b", "").HasType("string"),
				docs.FieldCommon("c", "").HasType("string"),
				docs.FieldCommon("d", "").HasType("string"),
				docs.FieldCommon("e", "").HasType("string").Array(),
				docs.FieldCommon("f", "").HasType("string").Map(),
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
				docs.FieldCommon("a", "").HasType("bool"),
				docs.FieldCommon("b", "").HasType("bool"),
				docs.FieldCommon("c", "").HasType("bool"),
				docs.FieldCommon("d", "").HasType("bool").Array(),
				docs.FieldCommon("e", "").HasType("bool").Map(),
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
				docs.FieldCommon("a", "").HasType("int"),
				docs.FieldCommon("b", "").HasType("int"),
				docs.FieldCommon("c", "").HasType("int"),
				docs.FieldCommon("d", "").HasType("int").Array(),
				docs.FieldCommon("e", "").HasType("int").Map(),
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
				docs.FieldCommon("a", "").HasType("float"),
				docs.FieldCommon("b", "").HasType("float"),
				docs.FieldCommon("c", "").HasType("float"),
				docs.FieldCommon("d", "").HasType("float").Array(),
				docs.FieldCommon("e", "").HasType("float").Map(),
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var node yaml.Node
			err := yaml.Unmarshal([]byte(test.yaml), &node)
			require.NoError(t, err)

			if node.Kind == yaml.DocumentNode && node.Content[0].Kind == yaml.MappingNode {
				node = *node.Content[0]
			}

			generic, err := test.spec.NodeToMap(&node)
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
			spec: docs.FieldCommon("foo", ""),
			expected: `null
`,
		},
		{
			name: "no recurse with children",
			spec: docs.FieldCommon("foo", "").WithChildren(
				docs.FieldCommon("bar", ""),
				docs.FieldCommon("baz", ""),
			),
			expected: `{}
`,
		},
		{
			name: "no recurse map",
			spec: docs.FieldCommon("foo", "").Map(),
			expected: `{}
`,
		},
		{
			name: "recurse with children",
			spec: docs.FieldCommon("foo", "").WithChildren(
				docs.FieldCommon("bar", "").HasType(docs.FieldString),
				docs.FieldCommon("baz", "").HasType(docs.FieldString).HasDefault("baz default"),
				docs.FieldCommon("buz", "").HasType(docs.FieldInt),
				docs.FieldCommon("bev", "").HasType(docs.FieldFloat),
				docs.FieldCommon("bun", "").HasType(docs.FieldBool),
				docs.FieldCommon("bud", "").Array(),
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
			n, err := test.spec.ToNode(test.recurse)
			require.NoError(t, err)

			b, err := yaml.Marshal(n)
			require.NoError(t, err)

			assert.Equal(t, test.expected, string(b))
		})
	}
}
