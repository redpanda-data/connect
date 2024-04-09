package pure_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestJQAllParts(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
jq:
  query: .foo.bar
`)
	require.NoError(t, err)

	jSet, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"bar":0}}`),
		[]byte(`{"foo":{"bar":1}}`),
		[]byte(`{"foo":{"bar":2}}`),
	})
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, res)
	require.Len(t, msgs, 1)
	for i, part := range message.GetAllBytes(msgs[0]) {
		assert.Equal(t, strconv.Itoa(i), string(part))
	}
}

func TestJQValidation(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
jq:
  query: .foo.bar
`)
	require.NoError(t, err)

	jSet, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	msgIn := message.QuickBatch([][]byte{[]byte("this is bad json")})
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)

	require.NoError(t, res)
	require.Len(t, msgs, 1)

	assert.Equal(t, "this is bad json", string(message.GetAllBytes(msgs[0])[0]))
}

func TestJQMutation(t *testing.T) {
	conf, err := testutil.ProcessorFromYAML(`
jq:
  query: '{foo: .foo} | .foo.bar = "baz"'
`)
	require.NoError(t, err)

	jSet, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	ogObj := gabs.New()
	_, _ = ogObj.Set("is this", "foo", "original", "content")
	_, _ = ogObj.Set("remove this", "bar")
	ogExp := ogObj.String()

	msgIn := message.QuickBatch(make([][]byte, 1))
	msgIn.Get(0).SetStructured(ogObj.Data())
	msgs, res := jSet.ProcessBatch(context.Background(), msgIn)
	require.NoError(t, res)
	require.Len(t, msgs, 1)

	assert.Equal(t, `{"foo":{"bar":"baz","original":{"content":"is this"}}}`, string(message.GetAllBytes(msgs[0])[0]))
	assert.Equal(t, ogExp, ogObj.String())
}

func TestJQ(t *testing.T) {
	type jTest struct {
		name     string
		path     string
		inputStr string
		input    any
		output   string
		err      string
	}

	tests := []jTest{
		{
			name:     "select obj",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":{"baz":1}}}`,
			output:   `{"baz":1}`,
		},
		{
			name:     "select array",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":["baz","qux"]}}`,
			output:   `["baz","qux"]`,
		},
		{
			name:     "select obj as str",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":"{\"baz\":1}"}}`,
			output:   `"{\"baz\":1}"`,
		},
		{
			name:     "select str",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":"hello world"}}`,
			output:   `"hello world"`,
		},
		{
			name:     "select float",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":0.123}}`,
			output:   `0.123`,
		},
		{
			name:     "select int",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":123}}`,
			output:   `123`,
		},
		{
			name:     "select bool",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":true}}`,
			output:   `true`,
		},
		{
			name:     "null result",
			path:     ".baz.qux",
			inputStr: `{"foo":{"bar":true}}`,
			output:   `null`,
		},
		{
			name:     "empty string",
			path:     ".foo.bar",
			inputStr: `{"foo":{"bar":""}}`,
			output:   `""`,
		},
		{
			name:     "convert to csv",
			path:     "[.ts,.id,.msg] | @csv",
			inputStr: `{"id":"1054fe28","msg":"sample \"log\"","ts":1641393111}`,
			output:   `"1641393111,\"1054fe28\",\"sample \"\"log\"\"\""`,
		},
		{
			name: "invalid type",
			path: ".ts | length",
			input: map[string]any{
				"ts": time.Unix(1000, 0),
			},
			err: "invalid type: time.Time",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := testutil.ProcessorFromYAML(`
jq:
  query: '` + test.path + `'
`)
			require.NoError(t, err)

			jSet, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			var inMsg message.Batch
			if test.inputStr != "" {
				inMsg = append(inMsg, message.NewPart([]byte(test.inputStr)))
			} else {
				part := message.NewPart(nil)
				part.SetStructuredMut(test.input)
				inMsg = append(inMsg, part)
			}

			msgs, _ := jSet.ProcessBatch(context.Background(), inMsg)
			require.Len(t, msgs, 1)
			if test.err == "" {
				assert.Equal(t, test.output, string(message.GetAllBytes(msgs[0])[0]))
			} else {
				assert.Error(t, msgs[0][0].ErrorGet())
				assert.Contains(t, msgs[0][0].ErrorGet().Error(), test.err)
			}
		})
	}
}

func TestJQ_OutputRaw(t *testing.T) {
	type jTest struct {
		name   string
		path   string
		input  string
		output string
	}

	tests := []jTest{
		{
			name:   "select obj",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":{"baz":1}}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select array",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":["baz","qux"]}}`,
			output: `["baz","qux"]`,
		},
		{
			name:   "select obj as str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"{\"baz\":1}"}}`,
			output: `{"baz":1}`,
		},
		{
			name:   "select str",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":"hello world"}}`,
			output: `hello world`,
		},
		{
			name:   "select float",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":0.123}}`,
			output: `0.123`,
		},
		{
			name:   "select int",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":123}}`,
			output: `123`,
		},
		{
			name:   "select bool",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":true}}`,
			output: `true`,
		},
		{
			name:   "null result",
			path:   ".baz.qux",
			input:  `{"foo":{"bar":true}}`,
			output: `null`,
		},
		{
			name:   "empty string",
			path:   ".foo.bar",
			input:  `{"foo":{"bar":""}}`,
			output: ``,
		},
		{
			name:   "convert to csv",
			path:   "[.ts,.id,.msg] | @csv",
			input:  `{"id":"1054fe28","msg":"sample \"log\"","ts":1641393111}`,
			output: `1641393111,"1054fe28","sample ""log"""`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := testutil.ProcessorFromYAML(`
jq:
  query: '` + test.path + `'
  output_raw: true
`)
			require.NoError(t, err)

			jSet, err := mock.NewManager().NewProcessor(conf)
			require.NoError(t, err)

			inMsg := message.QuickBatch(
				[][]byte{
					[]byte(test.input),
				},
			)
			msgs, _ := jSet.ProcessBatch(context.Background(), inMsg)
			require.Len(t, msgs, 1)
			assert.Equal(t, test.output, string(message.GetAllBytes(msgs[0])[0]))
		})
	}
}
