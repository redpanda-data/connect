package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func newDummyReader(confFilePath string) *Reader {
	rdr := NewReader(confFilePath, nil)
	rdr.changeDelayPeriod = 1 * time.Millisecond
	rdr.changeFlushPeriod = 1 * time.Millisecond
	return rdr
}

func TestReaderFileWatching(t *testing.T) {
	dummyConfig := []byte(`
input:
  generate: {}
output:
  drop: {}
`)

	confDir := t.TempDir()

	// Create an empty config file in the config folder
	confFilePath := filepath.Join(confDir, "main.yaml")
	require.NoError(t, os.WriteFile(confFilePath, []byte{}, 0o644))

	rdr := newDummyReader(confFilePath)

	changeChan := make(chan struct{})
	var updatedConf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf stream.Config) bool {
		updatedConf = conf
		close(changeChan)
		return true
	}))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	// Overwrite original config
	require.NoError(t, os.WriteFile(confFilePath, dummyConfig, 0o644))

	// Wait for the config watcher to reload the config
	select {
	case <-changeChan:
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Expected a config change to be triggered")
	}

	assert.Equal(t, "generate", updatedConf.Input.Type)
	assert.Equal(t, "drop", updatedConf.Output.Type)
}

func TestReaderFileWatchingSymlinkReplace(t *testing.T) {
	dummyConfig := []byte(`
input:
  generate: {}
output:
  drop: {}
`)

	rootDir := t.TempDir()

	// Create a config folder
	confDir := filepath.Join(rootDir, "config")
	require.NoError(t, os.Mkdir(confDir, 0o755))

	// Create a symlink to the config folder
	confDirSymlink := filepath.Join(rootDir, "symlink")
	require.NoError(t, os.Symlink(confDir, confDirSymlink))

	// Create an empty config file in the config folder through the symlink
	confFilePath := filepath.Join(confDirSymlink, "main.yaml")
	require.NoError(t, os.WriteFile(confFilePath, []byte{}, 0o644))

	rdr := newDummyReader(confFilePath)

	changeChan := make(chan struct{})
	var updatedConf stream.Config
	require.NoError(t, rdr.SubscribeConfigChanges(func(conf stream.Config) bool {
		updatedConf = conf
		close(changeChan)
		return true
	}))

	// Watch for configuration changes
	testMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)
	require.NoError(t, rdr.BeginFileWatching(testMgr, true))

	// Create a new config folder and place in it a new copy of the config file
	newConfDir := filepath.Join(rootDir, "config_new")
	require.NoError(t, os.Mkdir(newConfDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(newConfDir, "main.yaml"), dummyConfig, 0o644))

	// Create a symlink to the new config folder
	newConfDirSymlink := filepath.Join(rootDir, "symlink_new")
	require.NoError(t, os.Symlink(newConfDir, newConfDirSymlink))

	// Overwrite the original symlink with the new symlink
	require.NoError(t, os.Rename(newConfDirSymlink, confDirSymlink))

	// Remove the original config folder to trigger a config refresh
	require.NoError(t, os.RemoveAll(confDir))

	// Wait for the config watcher to reload the config
	select {
	case <-changeChan:
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Expected a config change to be triggered")
	}

	assert.Equal(t, "generate", updatedConf.Input.Type)
	assert.Equal(t, "drop", updatedConf.Output.Type)
}
