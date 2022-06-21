package test_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/cli/test"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

type mockProvider map[string][]processor.V1

func (m mockProvider) Provide(ptr string, env map[string]string, mocks map[string]yaml.Node) ([]processor.V1, error) {
	if procs, ok := m[ptr]; ok {
		return procs, nil
	}
	return nil, errors.New("processors not found")
}

func (m mockProvider) ProvideBloblang(name string) ([]processor.V1, error) {
	if procs, ok := m[name]; ok {
		return procs, nil
	}
	return nil, errors.New("mapping not found")
}

func TestCase(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}

	procConf := processor.NewConfig()
	procConf.Type = "noop"
	proc, err := mock.NewManager().NewProcessor(procConf)
	if err != nil {
		t.Fatal(err)
	}
	provider["/pipeline/processors"] = []processor.V1{proc}

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Bloblang = `root = content().uppercase()`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/0/processors"] = []processor.V1{proc}

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Bloblang = `root = deleted()`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/1/processors"] = []processor.V1{proc}

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Bloblang = `root = if batch_index() == 0 { count("batch_id") }`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/2/processors"] = []processor.V1{proc}

	type testCase struct {
		name     string
		conf     string
		expected []test.CaseFailure
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
			name: "positive 5",
			conf: `
name: positive 5
input_batches:
-
  - content: foo
-
  - content: bar
output_batches:
-
  - content_equals: "foo"
-
  - content_equals: "bar"
`,
		},
		{
			name: "batch id 1",
			conf: `
name: batch id 1
target_processors: /input/broker/inputs/2/processors
input_batches:
-
  - content: foo
-
  - content: foo
  - content: bar
output_batches:
-
  - content_equals: "1"
-
  - content_equals: "2"
  - content_equals: "bar"
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
			expected: []test.CaseFailure{
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
			expected: []test.CaseFailure{
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
			expected: []test.CaseFailure{
				{
					Name:     "negative batches count 1",
					TestLine: 2,
					Reason:   "wrong batch count, expected 2, got 1",
				},
			},
		},
		{
			name: "unexpected batch 1",
			conf: `
name: unexpected batch 1
input_batches:
-
  - content: foo bar
-
  - content: foo bar
-
  - content: foo bar
output_batches:
-
  - content_equals: "foo bar"
-
  - content_equals: "foo bar"
`,
			expected: []test.CaseFailure{
				{
					Name:     "unexpected batch 1",
					TestLine: 2,
					Reason:   "unexpected batch: [foo bar]",
				},
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(tt *testing.T) {
			c := test.NewCase()
			if err = yaml.Unmarshal([]byte(testCase.conf), &c); err != nil {
				tt.Fatal(err)
			}
			fails, err := c.ExecuteFrom("", provider)
			if err != nil {
				tt.Fatal(err)
			}
			if exp, act := testCase.expected, fails; !reflect.DeepEqual(exp, act) {
				tt.Errorf("Wrong results: %v != %v", act, exp)
			}
		})
	}
}

func TestFileCaseInputs(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}
	procConf := processor.NewConfig()

	procConf.Type = "bloblang"
	procConf.Bloblang = `root = "hello world " + content().string()`
	proc, err := mock.NewManager().NewProcessor(procConf)
	require.NoError(t, err)

	provider["/pipeline/processors"] = []processor.V1{proc}

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	c := test.NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: uppercased
input_batch:
  - file_content: ./inner/uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`), &c))

	fails, err := c.ExecuteFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []test.CaseFailure(nil), fails)

	c = test.NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: not uppercased
input_batch:
  - file_content: ./not_uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`), &c))

	fails, err = c.ExecuteFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []test.CaseFailure{
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

	procConf.Type = "bloblang"
	procConf.Bloblang = `root = content().uppercase()`
	proc, err := mock.NewManager().NewProcessor(procConf)
	require.NoError(t, err)

	provider["/pipeline/processors"] = []processor.V1{proc}

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	c := test.NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./inner/uppercased.txt"
`), &c))

	fails, err := c.ExecuteFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []test.CaseFailure(nil), fails)

	c = test.NewCase()
	require.NoError(t, yaml.Unmarshal([]byte(`
name: not uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./not_uppercased.txt"
`), &c))

	fails, err = c.ExecuteFrom(tmpDir, provider)
	require.NoError(t, err)

	assert.Equal(t, []test.CaseFailure{
		{
			Name:     "not uppercased",
			TestLine: 2,
			Reason:   "batch 0 message 0: file_equals: content mismatch\n  expected: foo bar baz\n  received: FOO BAR BAZ",
		},
	}, fails)
}
