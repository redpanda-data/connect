package config

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDummyReader(confFilePath string, resourcePaths []string, opts ...OptFunc) *Reader {
	rdr := NewReader(confFilePath, resourcePaths, opts...)
	rdr.changeDelayPeriod = 1 * time.Millisecond
	rdr.changeFlushPeriod = 1 * time.Millisecond
	rdr.filesRefreshPeriod = 1 * time.Millisecond
	return rdr
}

type testFS struct {
	m fstest.MapFS
}

func (fs testFS) Open(name string) (fs.File, error) {
	return fs.m.Open(name)
}

func (fs testFS) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return fs.m.Open(name)
}

func (fs testFS) Stat(name string) (fs.FileInfo, error) {
	return fs.m.Stat(name)
}

func (fs testFS) MkdirAll(name string, perm fs.FileMode) error {
	return errors.New("not implemented")
}

func (fs testFS) Remove(name string) error {
	return errors.New("not implemented")
}

func TestCustomFileSync(t *testing.T) {
	testFS := &testFS{m: fstest.MapFS{
		"foo_main.yaml": &fstest.MapFile{
			Data: []byte(`
input:
  label: fooin
  inproc: foo

output:
  label: fooout
  inproc: bar
`),
		},
		"a.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: a
    mapping: 'root = content() + " a1"'
  - label: b
    mapping: 'root = content() + " b1"'
`),
		},
		"b.yaml": &fstest.MapFile{
			Data: []byte(`
processor_resources:
  - label: c
    mapping: 'root = content() + " c1"'
  - label: d
    mapping: 'root = content() + " d1"'
`),
		},
	}}
	rdr := newDummyReader("foo_main.yaml", []string{"a.yaml", "b.yaml"}, OptUseFS(testFS))

	conf := New()
	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Empty(t, lints)

	assert.Equal(t, "fooin", conf.Input.Label)
	assert.Equal(t, "fooout", conf.Output.Label)

	assert.Len(t, conf.ResourceProcessors, 4)
	assert.Equal(t, "a", conf.ResourceProcessors[0].Label)
	assert.Equal(t, "b", conf.ResourceProcessors[1].Label)
	assert.Equal(t, "c", conf.ResourceProcessors[2].Label)
	assert.Equal(t, "d", conf.ResourceProcessors[3].Label)
}
