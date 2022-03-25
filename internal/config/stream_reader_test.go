package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestStreamsLints(t *testing.T) {
	dir := t.TempDir()

	streamOnePath := filepath.Join(dir, "first.yaml")
	require.NoError(t, os.WriteFile(streamOnePath, []byte(`
input:
  meow1: not this
  kafka:
    addresses: [ foobar.com, barbaz.com ]
    topics: [ meow1, meow2 ]
`), 0o644))

	streamTwoPath := filepath.Join(dir, "second.yaml")
	require.NoError(t, os.WriteFile(streamTwoPath, []byte(`
pipeline:
  processors:
    - bloblang: 'root = this.lowercase()'

cache_resources:
  - label: this_shouldnt_be_here
    memory:
      ttl: 13
`), 0o644))

	rdr := config.NewReader("", nil, config.OptSetStreamPaths(streamOnePath, streamTwoPath))

	conf := config.New()
	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Len(t, lints, 0)

	streamConfs := map[string]stream.Config{}
	lints, err = rdr.ReadStreams(streamConfs)
	require.NoError(t, err)

	require.Len(t, lints, 2)
	assert.Contains(t, lints[0], "/first.yaml: line 3: field meow1 ")
	assert.Contains(t, lints[1], "/second.yaml: line 6: field cache_resources not recognised")

	require.Len(t, streamConfs, 2)

	assert.Equal(t, "kafka", streamConfs["first"].Input.Type)
	assert.Equal(t, []string{"foobar.com", "barbaz.com"}, streamConfs["first"].Input.Kafka.Addresses)
	assert.Equal(t, []string{"meow1", "meow2"}, streamConfs["first"].Input.Kafka.Topics)
}

func TestStreamsDirectoryWalk(t *testing.T) {
	dir := t.TempDir()

	streamOnePath := filepath.Join(dir, "first.yaml")
	require.NoError(t, os.WriteFile(streamOnePath, []byte(`
pipeline:
  processors:
    - bloblang: 'root = "first"'
`), 0o644))

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested", "inner"), 0o755))

	streamTwoPath := filepath.Join(dir, "nested", "inner", "second.yaml")
	require.NoError(t, os.WriteFile(streamTwoPath, []byte(`
pipeline:
  processors:
    - bloblang: 'root = "second"'
`), 0o644))

	streamThreePath := filepath.Join(dir, "nested", "inner", "third.yaml")
	require.NoError(t, os.WriteFile(streamThreePath, []byte(`
pipeline:
  processors:
    - bloblang: 'root = "third"'
`), 0o644))

	rdr := config.NewReader("", nil, config.OptSetStreamPaths(streamOnePath, filepath.Join(dir, "nested")))

	conf := config.New()
	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Len(t, lints, 0)

	streamConfs := map[string]stream.Config{}
	lints, err = rdr.ReadStreams(streamConfs)
	require.NoError(t, err)
	require.Len(t, lints, 0)

	require.Len(t, streamConfs, 3)
	require.Contains(t, streamConfs, "first")
	require.Contains(t, streamConfs, "inner_second")
	require.Contains(t, streamConfs, "inner_third")

	assert.Equal(t, `root = "first"`, string(streamConfs["first"].Pipeline.Processors[0].Bloblang))
	assert.Equal(t, `root = "second"`, string(streamConfs["inner_second"].Pipeline.Processors[0].Bloblang))
	assert.Equal(t, `root = "third"`, string(streamConfs["inner_third"].Pipeline.Processors[0].Bloblang))
}
