package pure

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestConcatMethod(t *testing.T) {
	testCases := []struct {
		name    string
		mapping string
		input   any
		output  any
		execErr string
	}{
		{
			name:    "nested arrays",
			mapping: `root.foo = this.foo.concat(this.bar, this.baz)`,
			input: map[string]any{
				"foo": []any{[]any{"first"}},
				"bar": []any{
					[]any{"second", "third"},
					[]any{"fourth"},
				},
				"baz": []any{"fifth", "sixth"},
			},
			output: map[string]any{
				"foo": []any{
					[]any{"first"},
					[]any{"second", "third"},
					[]any{"fourth"},
					"fifth", "sixth",
				},
			},
		},
		{
			name:    "non array arg",
			mapping: `root.foo = this.foo.concat(this.bar)`,
			input: map[string]any{
				"foo": []any{"first"},
				"bar": "second",
			},
			execErr: "expected array value, got string",
		},
		{
			name:    "non array target",
			mapping: `root.foo = this.foo.concat(this.bar)`,
			input: map[string]any{
				"bar": []any{"first"},
				"foo": "second",
			},
			execErr: "expected array value, got string",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			exec, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			res, err := exec.Query(test.input)

			if test.execErr == "" {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.execErr)
			}
		})
	}
}
