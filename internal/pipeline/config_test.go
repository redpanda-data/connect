package pipeline_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

func TestConfigParseYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errContains string
		validateFn  func(t testing.TB, v pipeline.Config)
	}{
		{
			name: "basic config",
			input: `
threads: 123
processors:
  - label: a
    mapping: 'root = "a"'
  - label: b
    mapping: 'root = "b"'
`,
			validateFn: func(t testing.TB, v pipeline.Config) {
				assert.Equal(t, v.Threads, 123)
				require.Len(t, v.Processors, 2)
				assert.Equal(t, v.Processors[0].Label, "a")
				assert.Equal(t, v.Processors[0].Type, "mapping")
				assert.Equal(t, v.Processors[1].Label, "b")
				assert.Equal(t, v.Processors[1].Type, "mapping")
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			n, err := docs.UnmarshalYAML([]byte(test.input))
			require.NoError(t, err)

			conf, err := pipeline.FromAny(docs.DeprecatedProvider, n)
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
