package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/config"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestSetOverridesOnNothing(t *testing.T) {
	conf := config.New()
	rdr := config.NewReader("", nil, config.OptAddOverrides(
		"input.type=generate",
		"input.generate.mapping=this.foo",
		"output.type=drop",
	))

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "generate", conf.Input.Type)
	assert.Equal(t, "this.foo", conf.Input.Generate.Mapping)
	assert.Equal(t, "drop", conf.Output.Type)
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
			err:   "yaml: unmarshal errors",
		},
	}

	for _, test := range tests {
		conf := config.New()
		rdr := config.NewReader("", nil, config.OptAddOverrides(test.input))

		_, err := rdr.Read(&conf)
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

	conf := config.New()
	rdr := config.NewReader(fullPath, nil, config.OptAddOverrides(
		"input.generate.count=5",
		"input.generate.interval=10s",
		"output.type=drop",
	))

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "generate", conf.Input.Type)
	assert.Equal(t, `root = "meow"`, conf.Input.Generate.Mapping)
	assert.Equal(t, `10s`, conf.Input.Generate.Interval)
	assert.Equal(t, 5, conf.Input.Generate.Count)

	assert.Equal(t, "drop", conf.Output.Type)
}

func TestResources(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  generate:
    count: 5
    mapping: 'root = "meow"'
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

	conf := config.New()
	rdr := config.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath, resourceThreePath})

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "generate", conf.Input.Type)
	assert.Equal(t, `root = "meow"`, conf.Input.Generate.Mapping)

	require.Len(t, conf.ResourceCaches, 2)

	assert.Equal(t, "foo", conf.ResourceCaches[0].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[0].Type)

	assert.Equal(t, "bar", conf.ResourceCaches[1].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[1].Type)
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

	conf := config.New()
	rdr := config.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath})

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Len(t, lints, 3)
	assert.Contains(t, lints[0], "/main.yaml(3,1) field meow1 ")
	assert.Contains(t, lints[1], "/res1.yaml(5,1) field meow2 ")
	assert.Contains(t, lints[2], "/res2.yaml(5,1) field meow3 ")

	assert.Equal(t, "generate", conf.Input.Type)
	assert.Equal(t, `root = "meow"`, conf.Input.Generate.Mapping)

	require.Len(t, conf.ResourceCaches, 2)

	assert.Equal(t, "foo", conf.ResourceCaches[0].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[0].Type)

	assert.Equal(t, "bar", conf.ResourceCaches[1].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[1].Type)
}
