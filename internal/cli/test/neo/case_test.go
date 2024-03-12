package neo_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/cli/test/neo"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	dtest "github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

type mockProvider map[string][]processor.V1

func (m mockProvider) Provide(ptr string, env map[string]string, mocks map[string]any) ([]processor.V1, error) {
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
	procConf.Plugin = `root = content().uppercase()`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/0/processors"] = []processor.V1{proc}

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Plugin = `root = deleted()`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/1/processors"] = []processor.V1{proc}

	procConf = processor.NewConfig()
	procConf.Type = "bloblang"
	procConf.Plugin = `root = if batch_index() == 0 { count("batch_id") }`
	if proc, err = mock.NewManager().NewProcessor(procConf); err != nil {
		t.Fatal(err)
	}
	provider["/input/broker/inputs/2/processors"] = []processor.V1{proc}

	type testCase struct {
		name     string
		conf     string
		expected neo.CaseExecution
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
			expected: neo.CaseExecution{
				Name:     "positive 1",
				TestLine: 2,
				Status:   neo.Passed,
			},
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
			expected: neo.CaseExecution{
				Name:     "positive 2",
				TestLine: 2,
				Status:   neo.Passed,
			},
		},
		{
			name: "positive 3",
			conf: `
name: positive 3
target_processors: /input/broker/inputs/1/processors
input_batch:
- content: foo bar
output_batches: []`,
			expected: neo.CaseExecution{
				Name:     "positive 3",
				TestLine: 2,
				Status:   neo.Passed,
			},
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
			expected: neo.CaseExecution{
				Name:     "json positive 4",
				TestLine: 2,
				Status:   neo.Passed,
			},
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
			expected: neo.CaseExecution{
				Name:     "positive 5",
				TestLine: 2,
				Status:   neo.Passed,
			},
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
			expected: neo.CaseExecution{
				Name:     "batch id 1",
				TestLine: 2,
				Status:   neo.Passed,
			},
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
			expected: neo.CaseExecution{
				Name:     "negative 1",
				TestLine: 2,
				Status:   neo.Failed,
				Failures: []string{
					"batch 0 message 0: content_equals: content mismatch\n  expected: foo baz\n  received: foo bar",
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
			expected: neo.CaseExecution{
				Name:     "negative 2",
				TestLine: 2,
				Status:   neo.Failed,
				Failures: []string{
					"batch 0 message 1: content_equals: content mismatch\n  expected: bar baz\n  received: foo baz",
					"batch 0 message 1: metadata_equals: metadata key 'foo' mismatch\n  expected: bar\n  received: baz",
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
			expected: neo.CaseExecution{
				Name:     "negative batches count 1",
				TestLine: 2,
				Status:   neo.Failed,
				Failures: []string{
					"wrong batch count, expected 2, got 1",
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
			expected: neo.CaseExecution{
				Name:     "unexpected batch 1",
				TestLine: 2,
				Status:   neo.Failed,
				Failures: []string{
					"unexpected batch: [foo bar]",
				},
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(tt *testing.T) {
			node, err := docs.UnmarshalYAML([]byte(testCase.conf))
			require.NoError(t, err)

			c, err := dtest.CaseFromAny(node)
			require.NoError(t, err)

			exec := neo.RunCase(ifs.OS(), "", c, provider)
			if exec.Status == neo.Errored {
				tt.Fatal(exec.Err)
			}
			if exp, act := testCase.expected, exec; !reflect.DeepEqual(exp, *act) {
				jexp, err := json.Marshal(exp)
				require.NoError(t, err)
				jact, err := json.Marshal(act)
				require.NoError(t, err)

				tt.Errorf("Wrong results: actual %s != %s", jact, jexp)
			}
		})
	}
}

func TestFileCaseInputs(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}
	procConf := processor.NewConfig()

	procConf.Type = "bloblang"
	procConf.Plugin = `root = "hello world " + content().string()`
	proc, err := mock.NewManager().NewProcessor(procConf)
	require.NoError(t, err)

	provider["/pipeline/processors"] = []processor.V1{proc}

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	node, err := docs.UnmarshalYAML([]byte(`
name: uppercased
input_batch:
  - file_content: ./inner/uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`))
	require.NoError(t, err)

	c, err := dtest.CaseFromAny(node)
	require.NoError(t, err)

	exec := neo.RunCase(ifs.OS(), tmpDir, c, provider)
	assert.Equal(t, exec.Status, neo.Passed)

	node, err = docs.UnmarshalYAML([]byte(`
name: not uppercased
input_batch:
  - file_content: ./not_uppercased.txt
output_batches:
-
  - content_equals: hello world FOO BAR BAZ
`))
	require.NoError(t, err)

	c, err = dtest.CaseFromAny(node)
	require.NoError(t, err)

	exec = neo.RunCase(ifs.OS(), tmpDir, c, provider)

	assert.Equal(t, exec.Status, neo.Failed)
	assert.Equal(t, []string{
		"batch 0 message 0: content_equals: content mismatch\n  expected: hello world FOO BAR BAZ\n  received: hello world foo bar baz",
	}, exec.Failures)
}

func TestFileCaseConditions(t *testing.T) {
	color.NoColor = true

	provider := mockProvider{}
	procConf := processor.NewConfig()

	procConf.Type = "bloblang"
	procConf.Plugin = `root = content().uppercase()`
	proc, err := mock.NewManager().NewProcessor(procConf)
	require.NoError(t, err)

	provider["/pipeline/processors"] = []processor.V1{proc}

	tmpDir := t.TempDir()

	uppercasedPath := filepath.Join(tmpDir, "inner", "uppercased.txt")
	notUppercasedPath := filepath.Join(tmpDir, "not_uppercased.txt")

	require.NoError(t, os.MkdirAll(filepath.Dir(uppercasedPath), 0o755))
	require.NoError(t, os.WriteFile(uppercasedPath, []byte(`FOO BAR BAZ`), 0o644))
	require.NoError(t, os.WriteFile(notUppercasedPath, []byte(`foo bar baz`), 0o644))

	node, err := docs.UnmarshalYAML([]byte(`
name: uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./inner/uppercased.txt"
`))
	require.NoError(t, err)

	c, err := dtest.CaseFromAny(node)
	require.NoError(t, err)

	exec := neo.RunCase(ifs.OS(), tmpDir, c, provider)
	assert.Equal(t, exec.Status, neo.Passed)

	node, err = docs.UnmarshalYAML([]byte(`
name: not uppercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./not_uppercased.txt"
`))
	require.NoError(t, err)

	c, err = dtest.CaseFromAny(node)
	require.NoError(t, err)

	exec = neo.RunCase(ifs.OS(), tmpDir, c, provider)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"batch 0 message 0: file_equals: content mismatch\n  expected: foo bar baz\n  received: FOO BAR BAZ",
	}, exec.Failures)
}
