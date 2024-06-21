package test_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func initTestFiles(t *testing.T, files map[string]string) (string, error) {
	testDir := t.TempDir()

	for k, v := range files {
		fp := filepath.Join(testDir, k)
		if err := os.MkdirAll(filepath.Dir(fp), 0o777); err != nil {
			return "", err
		}
		if err := os.WriteFile(fp, []byte(v), 0o777); err != nil {
			return "", err
		}
	}

	return testDir, nil
}

func TestProcessorsProviderErrors(t *testing.T) {
	files := map[string]string{
		"config1.yaml": `
this isnt valid yaml
		nah
		what is even happening here?`,
		"config2.yaml": `
pipeline:
  processors:
  - bloblang: 'root = this'`,
		"config3.yaml": `
pipeline:
  processors:
  - type: doesnotexist`,
	}

	testDir, err := initTestFiles(t, files)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	if _, err = test.NewProcessorsProvider(filepath.Join(testDir, "doesnotexist.yaml")).Provide("/pipeline/processors", nil, nil); err == nil {
		t.Error("Expected error from bad filepath")
	}
	if _, err = test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml")).Provide("/pipeline/processors", nil, nil); err == nil {
		t.Error("Expected error from bad config file")
	}
	if _, err = test.NewProcessorsProvider(filepath.Join(testDir, "config2.yaml")).Provide("/not/a/valid/path", nil, nil); err == nil {
		t.Error("Expected error from bad processors path")
	}
	if _, err = test.NewProcessorsProvider(filepath.Join(testDir, "config3.yaml")).Provide("/pipeline/processors", nil, nil); err == nil {
		t.Error("Expected error from bad processor type")
	}
}

func TestProcessorsProvider(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	files := map[string]string{
		"config1.yaml": `
cache_resources:
  - label: foocache
    memory: {}

pipeline:
  processors:
  - bloblang: 'meta foo = env("BAR_VAR").not_empty().catch("defaultvalue")'
  - cache:
      resource: foocache
      operator: set
      key: defaultkey
      value: ${! meta("foo") }
  - cache:
      resource: foocache
      operator: get
      key: defaultkey
  - bloblang: 'root = content().uppercase()'`,
	}

	testDir, err := initTestFiles(t, files)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	provider := test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("/pipeline/processors", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 4, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	msgs, res := processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("hello world")}))
	require.NoError(t, res)
	if exp, act := "DEFAULTVALUE", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	if procs, err = provider.Provide("/pipeline/processors", map[string]string{
		"BAR_VAR": "newvalue",
	}, nil); err != nil {
		t.Fatal(err)
	}
	if exp, act := 4, len(procs); exp != act {
		t.Fatalf("Unexpected processor count: %v != %v", act, exp)
	}
	msgs, res = processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("hello world")}))
	require.NoError(t, res)
	if exp, act := "NEWVALUE", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestProcessorsProviderLabel(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	files := map[string]string{
		"config1.yaml": `
pipeline:
  processors:
  - bloblang: 'meta foo = env("BAR_VAR").not_empty().catch("defaultvalue")'
  - label: fooproc
    bloblang: 'root = content().uppercase()'`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	provider := test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("fooproc", nil, nil)
	require.NoError(t, err)

	assert.Len(t, procs, 1)

	msgs, res := processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("hello world")}))
	require.NoError(t, res)
	if exp, act := "HELLO WORLD", string(msgs[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestProcessorsProviderMocks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	files := map[string]string{
		"config1.yaml": `
pipeline:
  processors:
    - http:
        url: http://example.com/foobar
        verb: POST
    - bloblang: 'root = content().string() + " first proc"'
    - http:
        url: http://example.com/barbaz
        verb: POST
    - bloblang: 'root = content().string() + " second proc"'
`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	mocks := map[string]any{}
	require.NoError(t, yaml.Unmarshal([]byte(`
"/pipeline/processors/0":
  bloblang: 'root = content().string() + " first mock"'
"/pipeline/processors/2":
  bloblang: 'root = content().string() + " second mock"'
`), &mocks))

	provider := test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("/pipeline/processors", nil, mocks)
	require.NoError(t, err)

	require.Len(t, procs, 4)

	msgs, res := processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("starts with")}))
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	require.Equal(t, 1, msgs[0].Len())

	assert.Equal(t, "starts with first mock first proc second mock second proc", string(msgs[0].Get(0).AsBytes()))
}

func TestProcessorsProviderMocksFromLabel(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	files := map[string]string{
		"config1.yaml": `
pipeline:
  processors:
    - label: first_http
      http:
        url: http://example.com/foobar
        verb: POST
    - bloblang: 'root = content().string() + " first proc"'
    - label: second_http
      http:
        url: http://example.com/barbaz
        verb: POST
    - bloblang: 'root = content().string() + " second proc"'
`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	mocks := map[string]any{}
	require.NoError(t, yaml.Unmarshal([]byte(`
"first_http":
  bloblang: 'root = content().string() + " first mock"'
"second_http":
  bloblang: 'root = content().string() + " second mock"'
`), &mocks))

	provider := test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("/pipeline/processors", nil, mocks)
	require.NoError(t, err)

	require.Len(t, procs, 4)

	msgs, res := processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("starts with")}))
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	require.Equal(t, 1, msgs[0].Len())

	assert.Equal(t, "starts with first mock first proc second mock second proc", string(msgs[0].Get(0).AsBytes()))
}

func TestProcessorsProviderMocksMixed(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	files := map[string]string{
		"config1.yaml": `
pipeline:
  processors:
    - label: first_http
      http:
        url: http://example.com/foobar
        verb: POST
    - bloblang: 'root = content().string() + " first proc"'
    - label: second_http
      http:
        url: http://example.com/barbaz
        verb: POST
    - bloblang: 'root = content().string() + " second proc"'
`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	mocks := map[string]any{}
	require.NoError(t, yaml.Unmarshal([]byte(`
"first_http":
  bloblang: 'root = content().string() + " first mock"'
"/pipeline/processors/2":
  bloblang: 'root = content().string() + " second mock"'
`), &mocks))

	provider := test.NewProcessorsProvider(filepath.Join(testDir, "config1.yaml"))
	procs, err := provider.Provide("/pipeline/processors", nil, mocks)
	require.NoError(t, err)

	require.Len(t, procs, 4)

	msgs, res := processor.ExecuteAll(tCtx, procs, message.QuickBatch([][]byte{[]byte("starts with")}))
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	require.Equal(t, 1, msgs[0].Len())

	assert.Equal(t, "starts with first mock first proc second mock second proc", string(msgs[0].Get(0).AsBytes()))
}

func TestProcessorsExtraResources(t *testing.T) {
	files := map[string]string{
		"resources1.yaml": `
cache_resources:
  - label: barcache
    memory: {}
`,
		"resources2.yaml": `
cache_resources:
  - label: bazcache
    memory: {}
`,
		"config1.yaml": `
cache_resources:
  - label: foocache
    memory: {}

pipeline:
  processors:
  - cache:
      resource: foocache
      operator: set
      key: defaultkey
      value: foo
  - cache:
      resource: barcache
      operator: set
      key: defaultkey
      value: bar
  - cache:
      resource: bazcache
      operator: set
      key: defaultkey
      value: bar
`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	provider := test.NewProcessorsProvider(
		filepath.Join(testDir, "config1.yaml"),
		test.OptAddResourcesPaths([]string{
			filepath.Join(testDir, "resources1.yaml"),
			filepath.Join(testDir, "resources2.yaml"),
		}),
	)
	procs, err := provider.Provide("/pipeline/processors", nil, nil)
	require.NoError(t, err)
	assert.Len(t, procs, 3)
}

func TestProcessorsExtraResourcesError(t *testing.T) {
	files := map[string]string{
		"resources1.yaml": `
cache_resources:
  - label: barcache
    memory: {}
`,
		"resources2.yaml": `
cache_resources:
  - label: barcache
    memory: {}
`,
		"config1.yaml": `
cache_resources:
  - label: foocache
    memory: {}

pipeline:
  processors:
  - cache:
      resource: foocache
      operator: set
      key: defaultkey
      value: foo
  - cache:
      resource: barcache
      operator: set
      key: defaultkey
      value: bar
`,
	}

	testDir, err := initTestFiles(t, files)
	require.NoError(t, err)
	defer os.RemoveAll(testDir)

	provider := test.NewProcessorsProvider(
		filepath.Join(testDir, "config1.yaml"),
		test.OptAddResourcesPaths([]string{
			filepath.Join(testDir, "resources1.yaml"),
			filepath.Join(testDir, "resources2.yaml"),
		}),
	)
	_, err = provider.Provide("/pipeline/processors", nil, nil)
	require.EqualError(t, err, "failed to initialise resources: cache resource label 'barcache' collides with a previously defined resource")
}
