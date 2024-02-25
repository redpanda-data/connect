package config_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func testConfToAny(t testing.TB, conf any) any {
	var node yaml.Node
	err := node.Encode(conf)
	require.NoError(t, err)

	sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
	sanitConf.RemoveTypeField = true
	sanitConf.ScrubSecrets = true
	err = config.Spec().SanitiseYAML(&node, sanitConf)
	require.NoError(t, err)

	var v any
	require.NoError(t, node.Decode(&v))
	return v
}

func TestSetOverridesOnNothing(t *testing.T) {
	rdr := config.NewReader("", nil, config.OptAddOverrides(
		"input.type=generate",
		"input.generate.mapping=this.foo",
		"output.type=drop",
	))

	conf, lints, err := rdr.Read()
	require.NoError(t, err)
	assert.Empty(t, lints)

	v := gabs.Wrap(testConfToAny(t, conf))

	assert.Equal(t, "this.foo", v.S("input", "generate", "mapping").Data())
	assert.Equal(t, map[string]any{}, v.S("output", "drop").Data())
}

func TestSetOverrideErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		err   string
	}{
		{
			name:  "no value",
			input: "input.type=",
			err:   "invalid set expression 'input.type='",
		},
		{
			name:  "no equals",
			input: "input.type",
			err:   "invalid set expression 'input.type'",
		},
		{
			name:  "completely empty",
			input: "",
			err:   "invalid set expression ''",
		},
		{
			name:  "cant set that",
			input: "input=meow",
			err:   "invalid type !!str, expected object",
		},
	}

	for _, test := range tests {
		rdr := config.NewReader("", nil, config.OptAddOverrides(test.input))

		_, _, err := rdr.Read()
		assert.Contains(t, err.Error(), test.err)
	}
}

func TestSetOverridesOfFile(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  generate:
    count: 10
    mapping: 'root = "meow"'
`), 0o644))

	rdr := config.NewReader(fullPath, nil, config.OptAddOverrides(
		"input.generate.count=5",
		"input.generate.interval=10s",
		"output.type=drop",
	))

	conf, lints, err := rdr.Read()
	require.NoError(t, err)
	assert.Empty(t, lints)

	v := gabs.Wrap(testConfToAny(t, conf))

	assert.Equal(t, `root = "meow"`, v.S("input", "generate", "mapping").Data())
	assert.Equal(t, `10s`, v.S("input", "generate", "interval").Data())
	assert.Equal(t, 5, v.S("input", "generate", "count").Data())

	oMap := v.S("output").ChildrenMap()
	assert.Len(t, oMap, 2)
	assert.Contains(t, oMap, "drop")
	assert.Contains(t, oMap, "label")
}

func TestResources(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  generate:
    count: 5
    mapping: 'root = "meow"'
output:
  drop: {}
`), 0o644))

	resourceOnePath := filepath.Join(dir, "res1.yaml")
	require.NoError(t, os.WriteFile(resourceOnePath, []byte(`
cache_resources:
  - label: foo
    memory:
      default_ttl: 12s

tests:
  - name: huh
`), 0o644))

	resourceTwoPath := filepath.Join(dir, "res2.yaml")
	require.NoError(t, os.WriteFile(resourceTwoPath, []byte(`
cache_resources:
  - label: bar
    memory:
      default_ttl: 13s
`), 0o644))

	resourceThreePath := filepath.Join(dir, "res3.yaml")
	require.NoError(t, os.WriteFile(resourceThreePath, []byte(`
tests:
  - name: whut
`), 0o644))

	rdr := config.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath, resourceThreePath})

	conf, lints, err := rdr.Read()
	require.NoError(t, err)
	assert.Empty(t, lints)

	v := gabs.Wrap(testConfToAny(t, conf))

	assert.Equal(t, `root = "meow"`, v.S("input", "generate", "mapping").Data())

	require.Len(t, v.S("cache_resources").Data(), 2)

	assert.Equal(t, "foo", v.S("cache_resources", "0", "label").Data())
	assert.Equal(t, "12s", v.S("cache_resources", "0", "memory", "default_ttl").Data())

	assert.Equal(t, "bar", v.S("cache_resources", "1", "label").Data())
	assert.Equal(t, "13s", v.S("cache_resources", "1", "memory", "default_ttl").Data())
}

func TestLints(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  meow1: not this
  generate:
    count: 5
    mapping: 'root = "meow"'

output:
  drop: {}
`), 0o644))

	resourceOnePath := filepath.Join(dir, "res1.yaml")
	require.NoError(t, os.WriteFile(resourceOnePath, []byte(`
cache_resources:
  - label: foo
    memory:
      meow2: or this
      default_ttl: 12s
`), 0o644))

	resourceTwoPath := filepath.Join(dir, "res2.yaml")
	require.NoError(t, os.WriteFile(resourceTwoPath, []byte(`
cache_resources:
  - label: bar
    memory:
      meow3: or also this
      default_ttl: 13s
`), 0o644))

	rdr := config.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath})

	conf, lints, err := rdr.Read()
	require.NoError(t, err)
	require.Len(t, lints, 3)
	assert.Contains(t, lints[0], "/main.yaml(3,1) field meow1 ")
	assert.Contains(t, lints[1], "/res1.yaml(5,1) field meow2 ")
	assert.Contains(t, lints[2], "/res2.yaml(5,1) field meow3 ")

	v := gabs.Wrap(testConfToAny(t, conf))

	assert.Equal(t, `root = "meow"`, v.S("input", "generate", "mapping").Data())

	require.Len(t, v.S("cache_resources").Data(), 2)

	assert.Equal(t, "foo", v.S("cache_resources", "0", "label").Data())
	assert.Equal(t, "12s", v.S("cache_resources", "0", "memory", "default_ttl").Data())

	assert.Equal(t, "bar", v.S("cache_resources", "1", "label").Data())
	assert.Equal(t, "13s", v.S("cache_resources", "1", "memory", "default_ttl").Data())
}

func TestDefaultBasedOverridesWithYAML(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "foo.txt")

	var v yaml.Node
	require.NoError(t, v.Encode(map[string]any{
		"file": map[string]any{
			"path": outFile,
		},
	}))

	spec := config.Spec()
	spec.SetDefault(&v, "output")

	pConf, err := spec.ParsedConfigFromAny(map[string]any{
		"input": map[string]any{
			"generate": map[string]any{
				"mapping":  `root.foo = "bar"`,
				"count":    1,
				"interval": "1us",
			},
		},
	})
	require.NoError(t, err)

	c, err := config.FromParsed(bundle.GlobalEnvironment, pConf, nil)
	require.NoError(t, err)

	s, err := stream.New(c.Config, mock.NewManager())
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		fBytes, _ := os.ReadFile(outFile)
		return bytes.Contains(fBytes, []byte(`{"foo":"bar"}`))
	}, time.Second, time.Millisecond*10)

	require.NoError(t, s.Stop(context.Background()))
}

func TestDefaultBasedOverridesWithAny(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "foo.txt")

	spec := config.Spec()
	spec.SetDefault(map[string]any{
		"file": map[string]any{
			"path": outFile,
		},
	}, "output")

	node, err := docs.UnmarshalYAML([]byte(`
input:
  generate:
    mapping: 'root.foo = "bar"'
    count: 1
    interval: 1us
`))
	require.NoError(t, err)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	c, err := config.FromParsed(bundle.GlobalEnvironment, pConf, nil)
	require.NoError(t, err)

	s, err := stream.New(c.Config, mock.NewManager())
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		fBytes, _ := os.ReadFile(outFile)
		return bytes.Contains(fBytes, []byte(`{"foo":"bar"}`))
	}, time.Second, time.Millisecond*10)

	require.NoError(t, s.Stop(context.Background()))
}

func TestDefaultBasedOverridesWithExplicit(t *testing.T) {
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "foo.txt")

	outConf := output.Config{
		Type: "file",
		Plugin: map[string]any{
			"path": outFile,
		},
	}

	spec := config.Spec()
	spec.SetDefault(outConf, "output")

	node, err := docs.UnmarshalYAML([]byte(`
input:
  generate:
    mapping: 'root.foo = "bar"'
    count: 1
    interval: 1us
`))
	require.NoError(t, err)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	c, err := config.FromParsed(bundle.GlobalEnvironment, pConf, nil)
	require.NoError(t, err)

	s, err := stream.New(c.Config, mock.NewManager())
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		fBytes, _ := os.ReadFile(outFile)
		return bytes.Contains(fBytes, []byte(`{"foo":"bar"}`))
	}, time.Second, time.Millisecond*10)

	require.NoError(t, s.Stop(context.Background()))
}
