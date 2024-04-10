package manager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager"
)

func TestConfigParseYAML(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		errContains string
		validateFn  func(t testing.TB, v manager.ResourceConfig)
	}{
		{
			name: "one of everything",
			input: `
input_resources:
  - label: a
    generate:
      count: 1
      mapping: 'root.id = "a"'
      interval: 1s
processor_resources:
  - label: b
    mapping: 'root.id = "b"'
output_resources:
  - label: c
    reject: "c rejected"
cache_resources:
  - label: d
    memory:
      init_values:
        static: "d value"
rate_limit_resources:
  - label: e
    local:
      count: 123
      interval: 100ms
`,
			validateFn: func(t testing.TB, v manager.ResourceConfig) {
				require.Len(t, v.ResourceCaches, 1)
				require.Len(t, v.ResourceRateLimits, 1)
				require.Len(t, v.ResourceInputs, 1)
				require.Len(t, v.ResourceOutputs, 1)
				require.Len(t, v.ResourceProcessors, 1)

				assert.Equal(t, "a", v.ResourceInputs[0].Label)
				assert.Equal(t, "generate", v.ResourceInputs[0].Type)

				assert.Equal(t, "b", v.ResourceProcessors[0].Label)
				assert.Equal(t, "mapping", v.ResourceProcessors[0].Type)

				assert.Equal(t, "c", v.ResourceOutputs[0].Label)
				assert.Equal(t, "reject", v.ResourceOutputs[0].Type)

				assert.Equal(t, "d", v.ResourceCaches[0].Label)
				assert.Equal(t, "memory", v.ResourceCaches[0].Type)

				assert.Equal(t, "e", v.ResourceRateLimits[0].Label)
				assert.Equal(t, "local", v.ResourceRateLimits[0].Type)
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			conf, err := testutil.ManagerFromYAML(test.input)
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
