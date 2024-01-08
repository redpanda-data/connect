package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
				assert.Equal(t, v.Input.Label, "a")
				assert.Equal(t, v.Input.Type, "generate")
				assert.Equal(t, v.Buffer.Type, "memory")
				assert.Equal(t, v.Pipeline.Threads, 123)
				assert.Equal(t, v.Output.Label, "c")
				assert.Equal(t, v.Output.Type, "reject")
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf, err := stream.FromYAML(test.input)
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
