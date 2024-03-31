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
			validateFn: func(t testing.TB, v stream.Config) {
				assert.Equal(t, "a", v.Input.Label)
				assert.Equal(t, "generate", v.Input.Type)
				assert.Equal(t, "memory", v.Buffer.Type)
				assert.Equal(t, 123, v.Pipeline.Threads)
				assert.Equal(t, "c", v.Output.Label)
				assert.Equal(t, "reject", v.Output.Type)
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
