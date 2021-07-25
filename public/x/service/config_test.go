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

			assert.Equal(t, test.lints, spec.component.Config.Children.LintYAML(docs.NewLintContext(), node))

			pConf, err := spec.configFromNode(NewEnvironment(), nil, node)
			require.NoError(t, err)

			var sanitNode yaml.Node
			require.NoError(t, sanitNode.Encode(pConf.AsStruct()))

			require.NoError(t, spec.component.Config.Children.SanitiseYAML(&sanitNode, docs.SanitiseConfig{
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

			assert.Equal(t, test.lints, spec.component.Config.Children.LintYAML(docs.NewLintContext(), node))

			pConf, err := spec.configFromNode(NewEnvironment(), nil, node)
			require.NoError(t, err)

			var sanitNode yaml.Node
			require.NoError(t, sanitNode.Encode(pConf.generic))

			require.NoError(t, spec.component.Config.Children.SanitiseYAML(&sanitNode, docs.SanitiseConfig{
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

	parsedConfig, err := spec.configFromNode(NewEnvironment(), nil, node)
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

func TestConfigBatching(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBatchPolicyField("a"))

	node, err := getYAMLNode([]byte(`
a:
  count: 20
  period: 5s
  processors:
    - bloblang: 'root = content().uppercase()'
`))
	require.NoError(t, err)

	parsedConfig, err := spec.configFromNode(NewEnvironment(), nil, node)
	require.NoError(t, err)

	_, err = parsedConfig.FieldTLS("b")
	require.Error(t, err)

	bConf, err := parsedConfig.FieldBatchPolicy("a")
	require.NoError(t, err)

	assert.Equal(t, 20, bConf.Count)
	assert.Equal(t, "5s", bConf.Period)
	require.Len(t, bConf.procs, 1)
	assert.Equal(t, "bloblang", bConf.procs[0].Type)
	assert.Equal(t, "root = content().uppercase()", string(bConf.procs[0].Bloblang))
}

func TestConfigTLS(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTLSField("a")).
		Field(NewStringField("b"))

	node, err := getYAMLNode([]byte(`
a:
  skip_cert_verify: true
b: and this
`))
	require.NoError(t, err)

	parsedConfig, err := spec.configFromNode(NewEnvironment(), nil, node)
	require.NoError(t, err)

	_, err = parsedConfig.FieldTLS("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldTLS("c")
	require.Error(t, err)

	tConf, err := parsedConfig.FieldTLS("a")
	require.NoError(t, err)

	assert.True(t, tConf.InsecureSkipVerify)
}

func TestConfigInterpolatedString(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewInterpolatedStringField("a")).
		Field(NewStringField("b"))

	node, err := getYAMLNode([]byte(`
a: foo ${! content() } bar
b: this is ${! json } an invalid interp string
`))
	require.NoError(t, err)

	parsedConfig, err := spec.configFromNode(NewEnvironment(), nil, node)
	require.NoError(t, err)

	_, err = parsedConfig.FieldInterpolatedString("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldInterpolatedString("c")
	require.Error(t, err)

	iConf, err := parsedConfig.FieldInterpolatedString("a")
	require.NoError(t, err)

	res := iConf.String(NewMessage([]byte("hello world")))
	assert.Equal(t, "foo hello world bar", res)
}

func TestConfigBloblang(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewBloblangField("a")).
		Field(NewStringField("b"))

	node, err := getYAMLNode([]byte(`
a: 'root = this.uppercase()'
b: 'root = this.filter('
`))
	require.NoError(t, err)

	parsedConfig, err := spec.configFromNode(NewEnvironment(), nil, node)
	require.NoError(t, err)

	_, err = parsedConfig.FieldBloblang("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldBloblang("c")
	require.Error(t, err)

	exec, err := parsedConfig.FieldBloblang("a")
	require.NoError(t, err)

	res, err := exec.Query("hello world")
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", res)
}
