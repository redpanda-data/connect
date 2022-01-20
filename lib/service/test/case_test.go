package test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

type mockProvider map[string][]types.Processor

func (m mockProvider) Provide(ptr string, env map[string]string) ([]types.Processor, error) {
	if procs, ok := m[ptr]; ok {
		return procs, nil
	}
	return nil, errors.New("processors not found")
}

func (m mockProvider) ProvideMocked(ptr string, env map[string]string, mocks map[string]yaml.Node) ([]types.Processor, error) {
	if procs, ok := m[ptr]; ok {
		return procs, nil
	}
	return nil, errors.New("processors not found")
}

func (m mockProvider) ProvideBloblang(name string) ([]types.Processor, error) {
	if procs, ok := m[name]; ok {
		return procs, nil
	}
	return nil, errors.New("mapping not found")
}

func TestCase(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}

	procConf := processor.NewConfig()
	procConf.Type = processor.TypeNoop
	proc, err := processor.New(procConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	provider["/pipeline/processors"] = []types.Processor{proc}

	procConf = processor.NewConfig()
	procConf.Type = processor.TypeBloblang
	procConf.Bloblang = `root = content().uppercase()`
	if proc, err = processor.New(procConf, nil, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/0/processors"] = []types.Processor{proc}

	procConf = processor.NewConfig()
	procConf.Type = processor.TypeBloblang
	procConf.Bloblang = `root = deleted()`
	if proc, err = processor.New(procConf, nil, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/1/processors"] = []types.Processor{proc}

	type testCase struct {
		name     string
		conf     string
		expected []CaseFailure
	}

	tests := []testCase{
		{
			name: "positive 1",
			conf: `
name: positive 1
input_batch:
- content: foo bar
output_batches:
-
  - content_equals: "foo bar"
`,
		},
		{
			name: "positive 2",
			conf: `
name: positive 2
target_processors: /input/broker/inputs/0/processors
input_batch:
- content: foo bar
output_batches:
-
  - content_equals: "FOO BAR"
`,
		},
		{
			name: "positive 3",
			conf: `
name: positive 3
target_processors: /input/broker/inputs/1/processors
input_batch:
- content: foo bar
output_batches: []`,
		},
		{
			name: "json positive 4",
			conf: `
name: json positive 4
input_batch:
- json_content:
    foo: bar
output_batches:
-
  - json_equals:
      foo: bar
`,
		},
		{
			name: "negative 1",
			conf: `
name: negative 1
input_batch:
- content: foo bar
output_batches:
-
  - content_equals: "foo baz"
`,
			expected: []CaseFailure{
				{
					Name:     "negative 1",
					TestLine: 2,
					Reason:   "batch 0 message 0: content_equals: content mismatch\n  expected: foo baz\n  received: foo bar",
				},
			},
		},
		{
			name: "negative 2",
			conf: `
name: negative 2
input_batch:
- content: foo bar
- content: foo baz
  metadata:
    foo: baz
output_batches:
-
  - content_equals: "foo bar"
  - content_equals: "bar baz"
    metadata_equals:
      foo: bar
`,
			expected: []CaseFailure{
				{
					Name:     "negative 2",
					TestLine: 2,
					Reason:   "batch 0 message 1: content_equals: content mismatch\n  expected: bar baz\n  received: foo baz",
				},
				{
					Name:     "negative 2",
					TestLine: 2,
					Reason:   "batch 0 message 1: metadata_equals: metadata key 'foo' mismatch\n  expected: bar\n  received: baz",
				},
			},
		},
		{
			name: "negative batches count 1",
			conf: `
name: negative batches count 1
input_batch:
- content: foo bar
output_batches:
-
  - content_equals: "foo bar"
-
  - content_equals: "foo bar"
`,
			expected: []CaseFailure{
				{
					Name:     "negative batches count 1",
					TestLine: 2,
					Reason:   "wrong batch count, expected 2, got 1",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			c := NewCase()
			if err = yaml.Unmarshal([]byte(test.conf), &c); err != nil {
				tt.Fatal(err)
			}
			fails, err := c.Execute(provider)
			if err != nil {
				tt.Fatal(err)
			}
			if exp, act := test.expected, fails; !reflect.DeepEqual(exp, act) {
				tt.Errorf("Wrong results: %v != %v", act, exp)
			}
		})
	}
}

func TestFileCaseInputs(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}
	procConf := processor.NewConfig()

	procConf.Type = processor.TypeBloblang
	procConf.Bloblang = processor.BloblangConfig(`root = "hello world " + content().string()`)
	proc, err := processor.New(procConf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	provider["/pipeline/processors"] = []types.Processor{proc}

	tmpDir, err := os.MkdirTemp("", "test_file_content")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	c := NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: uppercased
input_batch:
  - file_content: ./inner/uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`), &c))

	fails, err := c.executeFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []CaseFailure(nil), fails)

	c = NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: not uppercased
input_batch:
  - file_content: ./not_uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`), &c))

	fails, err = c.executeFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []CaseFailure{
		{
			Name:     "not uppercased",
			TestLine: 2,
			Reason:   "batch 0 message 0: content_equals: content mismatch\n  expected: hello world FOO BAR BAZ\n  received: hello world foo bar baz",
		},
	}, fails)
}

func TestFileCaseConditions(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}
	procConf := processor.NewConfig()

	procConf.Type = processor.TypeBloblang
	procConf.Bloblang = processor.BloblangConfig(`root = content().uppercase()`)
	proc, err := processor.New(procConf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	provider["/pipeline/processors"] = []types.Processor{proc}

	tmpDir, err := os.MkdirTemp("", "test_file_case")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	c := NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./inner/uppercased.txt"
`), &c))

	fails, err := c.executeFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []CaseFailure(nil), fails)

	c = NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: not uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./not_uppercased.txt"
`), &c))

	fails, err = c.executeFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []CaseFailure{
		{
			Name:     "not uppercased",
			TestLine: 2,
			Reason:   "batch 0 message 0: file_equals: content mismatch\n  expected: foo bar baz\n  received: FOO BAR BAZ",
		},
	}, fails)
}
