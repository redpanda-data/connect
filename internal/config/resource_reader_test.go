package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func TestReaderResourceFileReading(t *testing.T) {
	confDir := t.TempDir()

	mainFilePath := filepath.Join(confDir, "main.yaml")
	require.NoError(t, os.WriteFile(mainFilePath, []byte(`
input:
  inproc: meow

output:
  drop: {}
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(confDir, "a_res.yaml"), []byte(`
processor_resources:
  - label: fooproc
    mapping: |
      root = content().uppercase()
  - label: barproc
    mapping: |
      root = content() + " and bar"
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(confDir, "b_res.yaml"), []byte(`
processor_resources:
  - label: bazproc
    mapping: |
      root = content() + " and baz"
`), 0o644))

	rdr := NewReader(mainFilePath, []string{confDir + "/*_res.yaml"})
	rdr.changeDelayPeriod = 1 * time.Millisecond
	rdr.changeFlushPeriod = 1 * time.Millisecond

	conf := New()
	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Empty(t, lints)

	require.NoError(t, rdr.SubscribeConfigChanges(func(conf stream.Config) error {
		return nil
	}))

	// Watch for configuration changes.
	testMgr, err := manager.New(conf.ResourceConfig)
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testProc := func(name string, input, output string) {
		require.NoError(t, testMgr.AccessProcessor(tCtx, name, func(p processor.V1) {
			res, err := p.ProcessBatch(tCtx, message.Batch{
				message.NewPart([]byte(input)),
			})
			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Len(t, res[0], 1)
			assert.Equal(t, output, string(res[0][0].AsBytes()))
		}))
	}

	testProc("fooproc", "hello world", "HELLO WORLD")
	testProc("barproc", "hello world", "hello world and bar")
	testProc("bazproc", "hello world", "hello world and baz")

	// Update foo, remove bar.
	require.NoError(t, os.WriteFile(filepath.Join(confDir, "a_res.yaml"), []byte(`
processor_resources:
  - label: fooproc
    mapping: |
      root = content().uppercase() + "!!!"
`), 0o644))

	require.Eventually(t, func() bool {
		return testMgr.AccessProcessor(tCtx, "barproc", func(v processor.V1) {}) != nil
	}, time.Second, time.Millisecond*10)

	testProc("fooproc", "hello world", "HELLO WORLD!!!")
	require.EqualError(t, testMgr.AccessProcessor(tCtx, "barproc", func(v processor.V1) {}), "unable to locate resource: barproc")
	testProc("bazproc", "hello world", "hello world and baz")

	// Update baz, add new bar.
	require.NoError(t, os.WriteFile(filepath.Join(confDir, "b_res.yaml"), []byte(`
processor_resources:
  - label: bazproc
    mapping: |
      root = content() + " and a new baz"
  - label: barproc
    mapping: |
      root = content() + " and a replaced bar"
`), 0o644))

	require.Eventually(t, func() bool {
		return testMgr.AccessProcessor(tCtx, "barproc", func(v processor.V1) {}) == nil
	}, time.Second, time.Millisecond*10)

	testProc("fooproc", "hello world", "HELLO WORLD!!!")
	testProc("barproc", "hello world", "hello world and a replaced bar")
	testProc("bazproc", "hello world", "hello world and a new baz")
}

func TestReaderResourceMovedToNewFile(t *testing.T) {
	confDir := t.TempDir()

	mainFilePath := filepath.Join(confDir, "main.yaml")
	require.NoError(t, os.WriteFile(mainFilePath, []byte(`
input:
  inproc: meow

output:
  drop: {}
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(confDir, "a_res.yaml"), []byte(`
processor_resources:
  - label: fooproc
    mapping: |
      root = content().uppercase()
  - label: barproc
    mapping: |
      root = content() + " and bar"
`), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(confDir, "b_res.yaml"), []byte(`
processor_resources:
  - label: bazproc
    mapping: |
      root = content() + " and baz"
`), 0o644))

	rdr := NewReader(mainFilePath, []string{confDir + "/*_res.yaml"})
	rdr.changeDelayPeriod = 1 * time.Millisecond
	rdr.changeFlushPeriod = 1 * time.Millisecond

	conf := New()
	lints, err := rdr.Read(&conf)
	require.NoError(t, err)
	require.Empty(t, lints)

	require.NoError(t, rdr.SubscribeConfigChanges(func(conf stream.Config) error {
		return nil
	}))

	// Watch for configuration changes.
	testMgr, err := manager.New(conf.ResourceConfig)
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	testProc := func(name string, input, output string) {
		require.NoError(t, testMgr.AccessProcessor(tCtx, name, func(p processor.V1) {
			res, err := p.ProcessBatch(tCtx, message.Batch{
				message.NewPart([]byte(input)),
			})
			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Len(t, res[0], 1)
			assert.Equal(t, output, string(res[0][0].AsBytes()))
		}))
	}

	testProc("fooproc", "hello world", "HELLO WORLD")
	testProc("barproc", "hello world", "hello world and bar")
	testProc("bazproc", "hello world", "hello world and baz")

	// Update baz, add new bar.
	require.NoError(t, os.WriteFile(filepath.Join(confDir, "b_res.yaml"), []byte(`
processor_resources:
  - label: bazproc
    mapping: |
      root = content() + " and a new baz"
  - label: barproc
    mapping: |
      root = content() + " and a replaced bar"
`), 0o644))

	require.Eventually(t, func() (success bool) {
		_ = testMgr.AccessProcessor(tCtx, "barproc", func(p processor.V1) {
			res, _ := p.ProcessBatch(tCtx, message.Batch{
				message.NewPart([]byte("hello world")),
			})
			if len(res) != 1 || len(res[0]) != 1 {
				return
			}
			success = string(res[0][0].AsBytes()) == "hello world and a replaced bar"
		})
		return
	}, time.Second, time.Millisecond*10)

	testProc("fooproc", "hello world", "HELLO WORLD")
	testProc("barproc", "hello world", "hello world and a replaced bar")
	testProc("bazproc", "hello world", "hello world and a new baz")

	// Update foo, remove bar
	require.NoError(t, os.WriteFile(filepath.Join(confDir, "a_res.yaml"), []byte(`
processor_resources:
  - label: fooproc
    mapping: |
      root = content().uppercase() + "!!!"
`), 0o644))

	require.Eventually(t, func() (success bool) {
		_ = testMgr.AccessProcessor(tCtx, "fooproc", func(p processor.V1) {
			res, _ := p.ProcessBatch(tCtx, message.Batch{
				message.NewPart([]byte("hello world")),
			})
			if len(res) != 1 || len(res[0]) != 1 {
				return
			}
			success = string(res[0][0].AsBytes()) == "HELLO WORLD!!!"
		})
		return
	}, time.Second, time.Millisecond*10)

	// Bar should still exist because it was moved to a new file.
	testProc("fooproc", "hello world", "HELLO WORLD!!!")
	testProc("barproc", "hello world", "hello world and a replaced bar")
	testProc("bazproc", "hello world", "hello world and a new baz")
}
