package docs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestFieldsFromNode(t *testing.T) {
	tests := []struct {
		name   string
		yaml   string
		fields FieldSpecs
	}{
		{
			name: "flat object",
			yaml: `a: foo
b: bar
c: 21`,
			fields: FieldSpecs{
				FieldCommon("a", ""),
				FieldCommon("b", ""),
				FieldCommon("c", ""),
			},
		},
		{
			name: "nested object",
			yaml: `a: foo
b:
  d: bar
  e: 22
c: 21`,
			fields: FieldSpecs{
				FieldCommon("a", ""),
				FieldCommon("b", "").WithChildren(
					FieldCommon("d", ""),
					FieldCommon("e", ""),
				),
				FieldCommon("c", ""),
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

			assert.Equal(t, test.fields, FieldsFromNode(&node))
		})
	}
}

func TestFieldsNodeToMap(t *testing.T) {
	spec := FieldSpecs{
		FieldCommon("a", ""),
		FieldCommon("b", "").HasDefault(11),
		FieldCommon("c", "").WithChildren(
			FieldCommon("d", "").HasDefault(true),
			FieldCommon("e", "").HasDefault("evalue"),
			FieldCommon("f", "").WithChildren(
				FieldCommon("g", "").HasDefault(12),
				FieldCommon("h", ""),
				FieldCommon("i", "").HasDefault(13),
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
		spec   FieldSpecs
		yaml   string
		result interface{}
	}{
		{
			name: "string fields",
			spec: FieldSpecs{
				FieldCommon("a", "").HasType("string"),
				FieldCommon("b", "").HasType("string"),
				FieldCommon("c", "").HasType("string"),
				FieldCommon("d", "").HasType("string"),
				FieldCommon("e", "").HasType("string").Array(),
				FieldCommon("f", "").HasType("string").Map(),
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
			spec: FieldSpecs{
				FieldCommon("a", "").HasType("bool"),
				FieldCommon("b", "").HasType("bool"),
				FieldCommon("c", "").HasType("bool"),
				FieldCommon("d", "").HasType("bool").Array(),
				FieldCommon("e", "").HasType("bool").Map(),
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
			spec: FieldSpecs{
				FieldCommon("a", "").HasType("int"),
				FieldCommon("b", "").HasType("int"),
				FieldCommon("c", "").HasType("int"),
				FieldCommon("d", "").HasType("int").Array(),
				FieldCommon("e", "").HasType("int").Map(),
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
			spec: FieldSpecs{
				FieldCommon("a", "").HasType("float"),
				FieldCommon("b", "").HasType("float"),
				FieldCommon("c", "").HasType("float"),
				FieldCommon("d", "").HasType("float").Array(),
				FieldCommon("e", "").HasType("float").Map(),
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
