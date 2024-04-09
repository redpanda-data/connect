package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func TestConfigParseYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errContains string
		validateFn  func(t testing.TB, v stream.Config)
	}{
		{
			name: "one of everything",
			input: `
input:
  label: a
  generate:
    count: 1
    mapping: 'root.id = "a"'
    interval: 1s

buffer:
  memory:
    limit: 456

pipeline:
  threads: 123

output:
  label: c
  reject: "c rejected"
`,
			validateFn: func(tb testing.TB, v stream.Config) {
				tb.Helper()

				assert.Equal(tb, "a", v.Input.Label)
				assert.Equal(tb, "generate", v.Input.Type)
				assert.Equal(tb, "memory", v.Buffer.Type)
				assert.Equal(tb, 123, v.Pipeline.Threads)
				assert.Equal(tb, "c", v.Output.Label)
				assert.Equal(tb, "reject", v.Output.Type)

			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf, err := testutil.StreamFromYAML(test.input)
			if test.errContains == "" {
				require.NoError(t, err)
				test.validateFn(t, conf)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}
