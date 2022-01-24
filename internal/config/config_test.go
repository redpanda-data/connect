package config_test

import (
	"os"
	"path/filepath"
	"testing"

	iconfig "github.com/Jeffail/benthos/v3/internal/config"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestSetOverridesOnNothing(t *testing.T) {
	conf := config.New()
	rdr := iconfig.NewReader("", nil, iconfig.OptAddOverrides(
		"input.type=kafka",
		"input.kafka.addresses=foobarbaz.com",
		"output.type=amqp_0_9",
	))

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "kafka", conf.Input.Type)
	assert.Equal(t, "foobarbaz.com", conf.Input.Kafka.Addresses[0])
	assert.Equal(t, "amqp_0_9", conf.Output.Type)
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
		rdr := iconfig.NewReader("", nil, iconfig.OptAddOverrides(test.input))

		_, err := rdr.Read(&conf)
		assert.Contains(t, err.Error(), test.err)
	}
}

func TestSetOverridesOfFile(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  kafka:
    addresses: [ foobar.com, barbaz.com ]
    topics: [ meow1, meow2 ]
`), 0o644))

	conf := config.New()
	rdr := iconfig.NewReader(fullPath, nil, iconfig.OptAddOverrides(
		"input.kafka.addresses.0=nope1.com",
		"input.kafka.addresses.1=nope2.com",
		"input.kafka.topics=justthis",
		"output.type=kafka",
		"output.kafka.addresses=nope3.com",
		"output.kafka.topic=foobar",
	))

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "kafka", conf.Input.Type)
	assert.Equal(t, []string{"nope1.com", "nope2.com"}, conf.Input.Kafka.Addresses)
	assert.Equal(t, []string{"justthis"}, conf.Input.Kafka.Topics)

	assert.Equal(t, "kafka", conf.Output.Type)
	assert.Equal(t, []string{"nope3.com"}, conf.Output.Kafka.Addresses)
	assert.Equal(t, "foobar", conf.Output.Kafka.Topic)
}

func TestResources(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  kafka:
    addresses: [ foobar.com, barbaz.com ]
    topics: [ meow1, meow2 ]
`), 0o644))

	resourceOnePath := filepath.Join(dir, "res1.yaml")
	require.NoError(t, os.WriteFile(resourceOnePath, []byte(`
cache_resources:
  - label: foo
    memory:
      ttl: 12

tests:
  - name: huh
`), 0o644))

	resourceTwoPath := filepath.Join(dir, "res2.yaml")
	require.NoError(t, os.WriteFile(resourceTwoPath, []byte(`
cache_resources:
  - label: bar
    memory:
      ttl: 13
`), 0o644))

	resourceThreePath := filepath.Join(dir, "res3.yaml")
	require.NoError(t, os.WriteFile(resourceThreePath, []byte(`
tests:
  - name: whut
`), 0o644))

	conf := config.New()
	rdr := iconfig.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath, resourceThreePath})

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	assert.Empty(t, lints)

	assert.Equal(t, "kafka", conf.Input.Type)
	assert.Equal(t, []string{"foobar.com", "barbaz.com"}, conf.Input.Kafka.Addresses)
	assert.Equal(t, []string{"meow1", "meow2"}, conf.Input.Kafka.Topics)

	require.Len(t, conf.ResourceCaches, 2)

	assert.Equal(t, "foo", conf.ResourceCaches[0].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[0].Type)
	assert.Equal(t, 12, conf.ResourceCaches[0].Memory.TTL)

	assert.Equal(t, "bar", conf.ResourceCaches[1].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[1].Type)
	assert.Equal(t, 13, conf.ResourceCaches[1].Memory.TTL)
}

func TestLints(t *testing.T) {
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(fullPath, []byte(`
input:
  meow1: not this
  kafka:
    addresses: [ foobar.com, barbaz.com ]
    topics: [ meow1, meow2 ]
`), 0o644))

	resourceOnePath := filepath.Join(dir, "res1.yaml")
	require.NoError(t, os.WriteFile(resourceOnePath, []byte(`
cache_resources:
  - label: foo
    memory:
      meow2: or this
      ttl: 12
`), 0o644))

	resourceTwoPath := filepath.Join(dir, "res2.yaml")
	require.NoError(t, os.WriteFile(resourceTwoPath, []byte(`
cache_resources:
  - label: bar
    memory:
      meow3: or also this
      ttl: 13
`), 0o644))

	conf := config.New()
	rdr := iconfig.NewReader(fullPath, []string{resourceOnePath, resourceTwoPath})

	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Len(t, lints, 3)
	assert.Contains(t, lints[0], "/main.yaml: line 3: field meow1 ")
	assert.Contains(t, lints[1], "/res1.yaml: line 5: field meow2 ")
	assert.Contains(t, lints[2], "/res2.yaml: line 5: field meow3 ")

	assert.Equal(t, "kafka", conf.Input.Type)
	assert.Equal(t, []string{"foobar.com", "barbaz.com"}, conf.Input.Kafka.Addresses)
	assert.Equal(t, []string{"meow1", "meow2"}, conf.Input.Kafka.Topics)

	require.Len(t, conf.ResourceCaches, 2)

	assert.Equal(t, "foo", conf.ResourceCaches[0].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[0].Type)
	assert.Equal(t, 12, conf.ResourceCaches[0].Memory.TTL)

	assert.Equal(t, "bar", conf.ResourceCaches[1].Label)
	assert.Equal(t, "memory", conf.ResourceCaches[1].Type)
	assert.Equal(t, 13, conf.ResourceCaches[1].Memory.TTL)
}
