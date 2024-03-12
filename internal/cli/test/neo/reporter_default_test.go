package neo_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/cli/test/neo"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

var (
	lowercased = func(t *testing.T) test.Case {
		return testCase(t, `
name: lowercased
input_batch:
  - content: foo bar baz
output_batches:
-
  - file_equals: "./inner/lowercased.txt"
`)
	}
	uppercased = func(t *testing.T) test.Case {
		return testCase(t, `
name: uppercased
input_batch:
    - content: foo bar baz
output_batches:
-
    - file_equals: "./inner/uppercased.txt"
`)
	}
	suiteOne = func(t *testing.T) *neo.Suite {
		return &neo.Suite{
			Name: "tests/one_benthos_test.yaml",
			Path: "tests/one_benthos_test.yaml",
			Cases: map[string]test.Case{
				"test_uppercase": lowercased(t),
				"test_lowercase": uppercased(t),
			},
		}
	}
	suiteTwo = func(t *testing.T) *neo.Suite {
		return &neo.Suite{
			Name: "tests/two_benthos_test.yaml",
			Path: "tests/two_benthos_test.yaml",
			Cases: map[string]test.Case{
				"test_uppercase": lowercased(t),
				"test_lowercase": uppercased(t),
			},
		}
	}
)

func TestDefaultReporter_Error(t *testing.T) {
	type fields struct {
		out string
		err string
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "no_error", fields: fields{out: "", err: ""}, args: args{err: nil}},
		{name: "error", fields: fields{out: "", err: "oh look, an error\n"}, args: args{err: errors.New("oh look, an error")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBufferString("")
			ew := bytes.NewBufferString("")

			l, err := neo.GetFormatter("default", w, ew)
			assert.NoError(t, err)

			l.Error(tt.args.err)
			assert.Equal(t, tt.fields.out, w.String())
			assert.Equal(t, tt.fields.err, ew.String())
		})
	}
}

func TestDefaultReporter_Render(t *testing.T) {
	type fields struct {
		out string
		err string
	}
	type args struct {
		executions map[string]*neo.SuiteExecution
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "errored",
			fields: fields{
				out: `Test 'tests/one_benthos_test.yaml' errored
Error: Failed to parse file

Test 'tests/two_benthos_test.yaml' errored
Error: Failed to parse file`,
				err: "",
			},
			args: args{executions: map[string]*neo.SuiteExecution{
				"tests/one_benthos_test.yaml": {
					Suite:  suiteOne(t),
					Status: neo.Errored,
					Err:    errors.New("Failed to parse file"),
				},
				"tests/two_benthos_test.yaml": {
					Suite:  suiteTwo(t),
					Status: neo.Errored,
					Err:    errors.New("Failed to parse file"),
				},
			}},
		},
		{
			name: "targets_errored",
			fields: fields{
				out: `Test 'tests/one_benthos_test.yaml' failed
  Case 'test_uppercase' [line 2] passed
  Case 'test_lowercase' [line 15] errored
    Error: processors resulted in error: hollabaloo

Test 'tests/two_benthos_test.yaml' failed
  Case 'deleted' [line 2] errored
    Error: processors resulted in error: hollabaloo`,
				err: "",
			},
			args: args{executions: map[string]*neo.SuiteExecution{
				"tests/one_benthos_test.yaml": {
					Suite:  suiteOne(t),
					Status: neo.Failed,
					Cases: map[string]*neo.CaseExecution{
						"test_uppercase": {Name: "test_uppercase", TestLine: 2, Status: neo.Passed, Failures: []string{}},
						"test_lowercase": {Name: "test_lowercase", TestLine: 15, Status: neo.Errored, Err: errors.New("processors resulted in error: hollabaloo"), Failures: []string{}},
					},
				},
				"tests/two_benthos_test.yaml": {
					Suite:  suiteTwo(t),
					Status: neo.Failed,
					Cases: map[string]*neo.CaseExecution{
						"deleted": {Name: "deleted", TestLine: 2, Status: neo.Errored, Err: errors.New("processors resulted in error: hollabaloo"), Failures: []string{}},
					},
				},
			}},
		},
		{
			name: "targets_failed",
			fields: fields{
				out: `Test 'tests/one_benthos_test.yaml' failed
  Case 'test_uppercase' [line 2] passed
  Case 'test_lowercase' [line 15] failed
    unexpected batch: foo bar baz

Test 'tests/two_benthos_test.yaml' failed
  Case 'deleted' [line 2] failed
    unexpected batch: foo bar baz`,
				err: "",
			},
			args: args{executions: map[string]*neo.SuiteExecution{
				"tests/one_benthos_test.yaml": {
					Suite:  suiteOne(t),
					Status: neo.Failed,
					Cases: map[string]*neo.CaseExecution{
						"test_uppercase": {Name: "test_uppercase", TestLine: 2, Status: neo.Passed, Failures: []string{}},
						"test_lowercase": {Name: "test_lowercase", TestLine: 15, Status: neo.Failed, Failures: []string{"unexpected batch: foo bar baz"}},
					},
				},
				"tests/two_benthos_test.yaml": {
					Suite:  suiteTwo(t),
					Status: neo.Failed,
					Cases: map[string]*neo.CaseExecution{
						"deleted": {Name: "deleted", TestLine: 2, Status: neo.Failed, Failures: []string{"unexpected batch: foo bar baz"}},
					},
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBufferString("")
			ew := bytes.NewBufferString("")

			l, err := neo.GetFormatter("default", w, ew)
			assert.NoError(t, err)

			l.Render(tt.args.executions)
			assert.Equal(t, tt.fields.out, w.String())
			assert.Equal(t, tt.fields.err, ew.String())
		})
	}
}

func TestDefaultReporter_Warn(t *testing.T) {
	type fields struct {
		out string
		err string
	}
	type args struct {
		msg string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "no_message", fields: fields{out: "", err: ""}, args: args{msg: ""}},
		{name: "message", fields: fields{out: neo.Yellow("what a wonderful world\n"), err: ""}, args: args{msg: "what a wonderful world"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBufferString("")
			ew := bytes.NewBufferString("")

			l, err := neo.GetFormatter("default", w, ew)
			assert.NoError(t, err)

			l.Warn(tt.args.msg)
			assert.Equal(t, tt.fields.out, w.String())
			assert.Equal(t, tt.fields.err, ew.String())
		})
	}
}

func testCase(t *testing.T, content string) test.Case {
	node, err := docs.UnmarshalYAML([]byte(content))
	require.NoError(t, err)

	c, err := test.CaseFromAny(node)
	require.NoError(t, err)

	return c
}
