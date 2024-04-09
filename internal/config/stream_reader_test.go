package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestStreamsLints(t *testing.T) {
	dir := t.TempDir()

	generalConfPath := filepath.Join(dir, "main.yaml")
	require.NoError(t, os.WriteFile(generalConfPath, []byte(`
logger:
  level: ALL
`), 0o644))

	streamOnePath := filepath.Join(dir, "first.yaml")
	require.NoError(t, os.WriteFile(streamOnePath, []byte(`
input:
  meow1: not this
  generate:
    count: 10
    mapping: 'root = "meow"'

output:
  drop: {}
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

	rdr := config.NewReader(generalConfPath, nil, config.OptSetStreamPaths(streamOnePath, streamTwoPath))

	_, lints, err := rdr.Read()
	require.NoError(t, err)
	require.Empty(t, lints)

	streamConfs := map[string]stream.Config{}
	lints, err = rdr.ReadStreams(streamConfs)
	require.NoError(t, err)

	require.Len(t, lints, 2)
	assert.Contains(t, lints[0], "/first.yaml(3,1) field meow1 ")
	assert.Contains(t, lints[1], "/second.yaml(6,1) field cache_resources not recognised")

	require.Len(t, streamConfs, 2)

	firstAny := gabs.Wrap(testConfToAny(t, streamConfs["first"]))

	assert.Equal(t, "generate", streamConfs["first"].Input.Type)
	assert.Equal(t, `root = "meow"`, firstAny.S("input", "generate", "mapping").Data())
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

	_, lints, err := rdr.Read()
	require.NoError(t, err)
	require.Empty(t, lints)

	streamConfs := map[string]stream.Config{}
	lints, err = rdr.ReadStreams(streamConfs)
	require.NoError(t, err)
	require.Empty(t, lints)

	require.Len(t, streamConfs, 3)
	require.Contains(t, streamConfs, "first")
	require.Contains(t, streamConfs, "inner_second")
	require.Contains(t, streamConfs, "inner_third")

	assert.Equal(t, `root = "first"`, gabs.Wrap(testConfToAny(t, streamConfs["first"])).S("pipeline", "processors", "0", "bloblang").Data())
	assert.Equal(t, `root = "second"`, gabs.Wrap(testConfToAny(t, streamConfs["inner_second"])).S("pipeline", "processors", "0", "bloblang").Data())
	assert.Equal(t, `root = "third"`, gabs.Wrap(testConfToAny(t, streamConfs["inner_third"])).S("pipeline", "processors", "0", "bloblang").Data())
}
