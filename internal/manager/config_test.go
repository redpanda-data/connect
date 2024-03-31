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
			validateFn: func(tb testing.TB, v manager.ResourceConfig) {
				tb.Helper()

				require.Len(tb, v.ResourceCaches, 1)
				require.Len(tb, v.ResourceRateLimits, 1)
				require.Len(tb, v.ResourceInputs, 1)
				require.Len(tb, v.ResourceOutputs, 1)
				require.Len(tb, v.ResourceProcessors, 1)

				assert.Equal(tb, "a", v.ResourceInputs[0].Label)
				assert.Equal(tb, "generate", v.ResourceInputs[0].Type)

				assert.Equal(tb, "b", v.ResourceProcessors[0].Label)
				assert.Equal(tb, "mapping", v.ResourceProcessors[0].Type)

				assert.Equal(tb, "c", v.ResourceOutputs[0].Label)
				assert.Equal(tb, "reject", v.ResourceOutputs[0].Type)

				assert.Equal(tb, "d", v.ResourceCaches[0].Label)
				assert.Equal(tb, "memory", v.ResourceCaches[0].Type)

				assert.Equal(tb, "e", v.ResourceRateLimits[0].Label)
				assert.Equal(tb, "local", v.ResourceRateLimits[0].Type)
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
