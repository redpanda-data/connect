package manager_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/stream"
	"github.com/benthosdev/benthos/v4/internal/stream/manager"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestFromDirectory(t *testing.T) {
	testDir := t.TempDir()

	barDir := filepath.Join(testDir, "bar")
	if err := os.Mkdir(barDir, 0o777); err != nil {
		t.Fatal(err)
	}

	fooPath := filepath.Join(testDir, "foo.json")
	barPath := filepath.Join(barDir, "test.yaml")

	require.NoError(t, os.WriteFile(fooPath, []byte(`{"input":{"generate":{"mapping":"root = {}"}}}`), 0o666))
	require.NoError(t, os.WriteFile(barPath, []byte(`
input:
  inproc: meow
`), 0o666))

	var actConfs map[string]stream.Config
	actConfs, err := manager.LoadStreamConfigsFromDirectory(true, testDir)
	require.NoError(t, err)

	require.Contains(t, actConfs, "foo")
	require.Contains(t, actConfs, "bar_test")

	if exp, act := "generate", actConfs["foo"].Input.Type; exp != act {
		t.Errorf("Wrong value in loaded set: %v != %v", act, exp)
	}
	if exp, act := "inproc", actConfs["bar_test"].Input.Type; exp != act {
		t.Errorf("Wrong value in loaded set: %v != %v", act, exp)
	}
}
