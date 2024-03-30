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

				assert.Equal(tb, v.ResourceInputs[0].Label, "a")
				assert.Equal(tb, v.ResourceInputs[0].Type, "generate")

				assert.Equal(tb, v.ResourceProcessors[0].Label, "b")
				assert.Equal(tb, v.ResourceProcessors[0].Type, "mapping")

				assert.Equal(tb, v.ResourceOutputs[0].Label, "c")
				assert.Equal(tb, v.ResourceOutputs[0].Type, "reject")

				assert.Equal(tb, v.ResourceCaches[0].Label, "d")
				assert.Equal(tb, v.ResourceCaches[0].Type, "memory")

				assert.Equal(tb, v.ResourceRateLimits[0].Label, "e")
				assert.Equal(tb, v.ResourceRateLimits[0].Type, "local")
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
