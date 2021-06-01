package service

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfigFromStructYAML(t *testing.T) {
	type confNestedType struct {
		D bool   `yaml:"d"`
		E string `yaml:"e"`
	}
	type confType struct {
		A string         `yaml:"a"`
		B int            `yaml:"b"`
		C confNestedType `yaml:"c"`
	}

	spec, err := NewStructConfigSpec(func() interface{} {
		return &confType{
			A: "avalue",
			B: 11,
			C: confNestedType{
				D: true,
				E: "evalue",
			},
		}
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		config    string
		lints     []docs.Lint
		sanitized string
	}{
		{
			name:   "no fields",
			config: "{}",
			sanitized: `a: avalue
b: 11
c:
    d: true
    e: evalue
`,
		},
		{
			name: "fields set",
			config: `a: newavalue
c:
  d: false
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
		},
		{
			name: "fields set unrecognized field",
			config: `a: newavalue
not_real: this doesnt exist in the spec
c:
  d: false
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
			lints: []docs.Lint{
				docs.NewLintError(2, "field not_real not recognised"),
			},
		},
		{
			name: "fields set nested unrecognized field",
			config: `a: newavalue
c:
  d: false
  not_real: this doesnt exist in the spec
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
			lints: []docs.Lint{
				docs.NewLintError(4, "field not_real not recognised"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.config)

			node, err := getYAMLNode(confBytes)
			require.NoError(t, err)

			assert.Equal(t, test.lints, spec.component.Config.Children.LintNode(docs.NewLintContext(), node))

			pConf, err := spec.configFromNode(node)
			require.NoError(t, err)

			var sanitNode yaml.Node
			require.NoError(t, sanitNode.Encode(pConf.Root()))

			require.NoError(t, spec.component.Config.Children.SanitiseNode(&sanitNode, docs.SanitiseConfig{
				RemoveTypeField:  true,
				RemoveDeprecated: true,
			}))

			sanitConfOutBytes, err := yaml.Marshal(sanitNode)
			require.NoError(t, err)
			assert.Equal(t, test.sanitized, string(sanitConfOutBytes))
		})
	}
}

func TestConfigGeneric(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewStringField("a")).
		Field(NewIntField("b").Default(11)).
		Field(NewObjectField("c",
			NewBoolField("d").Default(true),
			NewStringField("e").Default("evalue"),
		))

	tests := []struct {
		name      string
		config    string
		lints     []docs.Lint
		sanitized string
	}{
		{
			name:   "no fields except mandatory",
			config: `a: foovalue`,
			sanitized: `a: foovalue
b: 11
c:
    d: true
    e: evalue
`,
		},
		{
			name: "fields set",
			config: `a: newavalue
c:
  d: false
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
		},
		{
			name: "fields set unrecognized field",
			config: `a: newavalue
not_real: this doesnt exist in the spec
c:
  d: false
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
			lints: []docs.Lint{
				docs.NewLintError(2, "field not_real not recognised"),
			},
		},
		{
			name: "fields set nested unrecognized field",
			config: `a: newavalue
c:
  d: false
  not_real: this doesnt exist in the spec
`,
			sanitized: `a: newavalue
b: 11
c:
    d: false
    e: evalue
`,
			lints: []docs.Lint{
				docs.NewLintError(4, "field not_real not recognised"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.config)

			node, err := getYAMLNode(confBytes)
			require.NoError(t, err)

			assert.Equal(t, test.lints, spec.component.Config.Children.LintNode(docs.NewLintContext(), node))

			pConf, err := spec.configFromNode(node)
			require.NoError(t, err)

			var sanitNode yaml.Node
			require.NoError(t, sanitNode.Encode(pConf.Root()))

			require.NoError(t, spec.component.Config.Children.SanitiseNode(&sanitNode, docs.SanitiseConfig{
				RemoveTypeField:  true,
				RemoveDeprecated: true,
			}))

			sanitConfOutBytes, err := yaml.Marshal(sanitNode)
			require.NoError(t, err)
			assert.Equal(t, test.sanitized, string(sanitConfOutBytes))
		})
	}
}

func TestConfigTypedFields(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewStringField("a")).
		Field(NewIntField("b").Default(11)).
		Field(NewObjectField("c",
			NewBoolField("d").Default(true),
			NewStringField("e").Default("evalue"),
			NewObjectField("f",
				NewIntField("g").Default(12),
				NewStringField("h"),
				NewFloatField("i").Default(13.0),
				NewStringListField("j"),
			),
		))

	node, err := getYAMLNode([]byte(`
a: setavalue
c:
  f:
    g: 22
    h: sethvalue
    i: 23.1
    j:
      - first in list
      - second in list
`))
	require.NoError(t, err)

	parsedConfig, err := spec.configFromNode(node)
	require.NoError(t, err)

	s, err := parsedConfig.FieldString("a")
	assert.NoError(t, err)
	assert.Equal(t, "setavalue", s)

	_, err = parsedConfig.FieldString("z")
	assert.Error(t, err)

	_, err = parsedConfig.FieldInt("c", "z")
	assert.Error(t, err)

	_, err = parsedConfig.FieldFloat("c", "d", "z")
	assert.Error(t, err)

	_, err = parsedConfig.FieldBool("c", "z")
	assert.Error(t, err)

	i, err := parsedConfig.FieldInt("b")
	assert.NoError(t, err)
	assert.Equal(t, 11, i)

	b, err := parsedConfig.FieldBool("c", "d")
	assert.NoError(t, err)
	assert.Equal(t, true, b)

	i, err = parsedConfig.FieldInt("c", "f", "g")
	assert.NoError(t, err)
	assert.Equal(t, 22, i)

	f, err := parsedConfig.FieldFloat("c", "f", "i")
	assert.NoError(t, err)
	assert.Equal(t, 23.1, f)

	ll, err := parsedConfig.FieldStringList("c", "f", "j")
	assert.NoError(t, err)
	assert.Equal(t, []string{"first in list", "second in list"}, ll)
}
