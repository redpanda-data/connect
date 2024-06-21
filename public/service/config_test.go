package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

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
				docs.NewLintError(2, docs.LintUnknown, errors.New("field not_real not recognised")),
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
				docs.NewLintError(4, docs.LintUnknown, errors.New("field not_real not recognised")),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confBytes := []byte(test.config)

			node, err := NewStreamBuilder().getYAMLNode(confBytes)
			require.NoError(t, err)

			assert.Equal(t, test.lints, spec.component.Config.Children.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node))

			pConf, err := spec.configFromAny(nil, node)
			require.NoError(t, err)

			a, err := pConf.FieldAny()
			require.NoError(t, err)

			var sanitNode yaml.Node
			require.NoError(t, sanitNode.Encode(a))

			sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
			sanitConf.RemoveTypeField = true
			sanitConf.RemoveDeprecated = true

			require.NoError(t, spec.component.Config.Children.SanitiseYAML(&sanitNode, sanitConf))

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
				NewStringMapField("k"),
				NewIntListField("l"),
				NewIntMapField("m"),
			),
		))

	parsedConfig, err := spec.ParseYAML(`
a: setavalue
c:
  f:
    g: 22
    h: sethvalue
    i: 23.1
    j:
      - first in list
      - second in list
    k:
      first: one
      second: two
    l:
      - 11
      - 12
    m:
      first: 21
      second: 22
`, nil)
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
	assert.True(t, b)

	i, err = parsedConfig.FieldInt("c", "f", "g")
	assert.NoError(t, err)
	assert.Equal(t, 22, i)

	f, err := parsedConfig.FieldFloat("c", "f", "i")
	assert.NoError(t, err)
	assert.Equal(t, 23.1, f)

	ll, err := parsedConfig.FieldStringList("c", "f", "j")
	assert.NoError(t, err)
	assert.Equal(t, []string{"first in list", "second in list"}, ll)

	sm, err := parsedConfig.FieldStringMap("c", "f", "k")
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"first": "one", "second": "two"}, sm)

	il, err := parsedConfig.FieldIntList("c", "f", "l")
	assert.NoError(t, err)
	assert.Equal(t, []int{11, 12}, il)

	im, err := parsedConfig.FieldIntMap("c", "f", "m")
	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"first": 21, "second": 22}, im)

	// Testing namespaces
	nsC := parsedConfig.Namespace("c")
	nsFOne := nsC.Namespace("f")
	nsFTwo := parsedConfig.Namespace("c", "f")

	b, err = nsC.FieldBool("d")
	assert.NoError(t, err)
	assert.True(t, b)

	i, err = nsFOne.FieldInt("g")
	assert.NoError(t, err)
	assert.Equal(t, 22, i)

	f, err = nsFTwo.FieldFloat("i")
	assert.NoError(t, err)
	assert.Equal(t, 23.1, f)
}

func TestConfigRootString(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewStringField(""))

	parsedConfig, err := spec.ParseYAML(`"hello world"`, nil)
	require.NoError(t, err)

	v, err := parsedConfig.FieldString()
	require.NoError(t, err)

	assert.Equal(t, "hello world", v)
}

func TestConfigListOfObjects(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewObjectListField("objects",
			NewStringField("foo"),
			NewStringField("bar").Default("bar value"),
			NewIntField("baz"),
		))

	_, err := spec.ParseYAML(`objects:
- foo: "foo value 1"
  bar: "bar value 1"
`, nil)
	require.Error(t, err)

	_, err = spec.ParseYAML(`objects:
- bar: "bar value 1"
  baz: 11
`, nil)
	require.Error(t, err)

	_, err = spec.ParseYAML(`objects: []`, nil)
	require.NoError(t, err)

	parsedConfig, err := spec.ParseYAML(`objects:
- foo: "foo value 1"
  bar: "bar value 1"
  baz: 11

- foo: "foo value 2"
  bar: "bar value 2"
  baz: 12

- foo: "foo value 3"
  baz: 13
`, nil)
	require.NoError(t, err)

	objs, err := parsedConfig.FieldObjectList("objects")
	require.NoError(t, err)
	require.Len(t, objs, 3)

	strValue, err := objs[0].FieldString("foo")
	require.NoError(t, err)
	assert.Equal(t, "foo value 1", strValue)

	strValue, err = objs[0].FieldString("bar")
	require.NoError(t, err)
	assert.Equal(t, "bar value 1", strValue)

	intValue, err := objs[0].FieldInt("baz")
	require.NoError(t, err)
	assert.Equal(t, 11, intValue)

	strValue, err = objs[1].FieldString("foo")
	require.NoError(t, err)
	assert.Equal(t, "foo value 2", strValue)

	strValue, err = objs[1].FieldString("bar")
	require.NoError(t, err)
	assert.Equal(t, "bar value 2", strValue)

	intValue, err = objs[1].FieldInt("baz")
	require.NoError(t, err)
	assert.Equal(t, 12, intValue)

	strValue, err = objs[2].FieldString("foo")
	require.NoError(t, err)
	assert.Equal(t, "foo value 3", strValue)

	strValue, err = objs[2].FieldString("bar")
	require.NoError(t, err)
	assert.Equal(t, "bar value", strValue)

	intValue, err = objs[2].FieldInt("baz")
	require.NoError(t, err)
	assert.Equal(t, 13, intValue)
}

func TestConfigTLS(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewTLSField("a")).
		Field(NewStringField("b"))

	parsedConfig, err := spec.ParseYAML(`
a:
  skip_cert_verify: true
b: and this
`, nil)
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

	parsedConfig, err := spec.ParseYAML(`
a: foo ${! content() } bar
b: this is ${! json( } an invalid interp string
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldInterpolatedString("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldInterpolatedString("c")
	require.Error(t, err)

	iConf, err := parsedConfig.FieldInterpolatedString("a")
	require.NoError(t, err)

	res, err := iConf.TryString(NewMessage([]byte("hello world")))
	require.NoError(t, err)
	assert.Equal(t, "foo hello world bar", res)
}

func TestConfigInterpolatedStringMap(t *testing.T) {
	spec := NewConfigSpec().
		Field(NewInterpolatedStringMapField("a")).
		Field(NewStringMapField("b"))

	parsedConfig, err := spec.ParseYAML(`
a:
  c: foo ${! content() } bar
  d: xyzzy ${! content() } baz
b:
  e: this is ${! json( } an invalid interp string
  f: this is another invalid interp string
`, nil)
	require.NoError(t, err)

	_, err = parsedConfig.FieldInterpolatedStringMap("b")
	require.Error(t, err)

	_, err = parsedConfig.FieldInterpolatedStringMap("g")
	require.Error(t, err)

	iConf, err := parsedConfig.FieldInterpolatedStringMap("a")
	require.NoError(t, err)

	res, err := iConf["c"].TryString(NewMessage([]byte("hello world")))
	require.NoError(t, err)
	assert.Equal(t, "foo hello world bar", res)

	res, err = iConf["d"].TryString(NewMessage([]byte("hello world")))
	require.NoError(t, err)
	assert.Equal(t, "xyzzy hello world baz", res)
}

func TestConfigFields(t *testing.T) {
	spec := NewConfigSpec().
		Fields(
			NewStringField("a"),
			NewIntField("b").Default(11),
			NewObjectField("c",
				NewBoolField("d").Default(true),
				NewStringField("e").Default("evalue"),
			),
		)

	parsed, err := spec.ParseYAML(`
      a: sample value
      c:
        d: false
    `, nil)
	require.NoError(t, err)

	a, err := parsed.FieldString("a")
	require.NoError(t, err)
	assert.Equal(t, "sample value", a)

	b, err := parsed.FieldInt("b")
	require.NoError(t, err)
	assert.Equal(t, 11, b)

	c := parsed.Namespace("c")

	d, err := c.FieldBool("d")
	require.NoError(t, err)
	assert.False(t, d)

	e, err := c.FieldString("e")
	require.NoError(t, err)
	assert.Equal(t, "evalue", e)
}
