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

func TestZipMethod(t *testing.T) {
	testCases := []struct {
		name    string
		mapping string
		input   any
		output  any
		execErr string
	}{
		{
			name:    "three arrays",
			mapping: `root.foo = this.foo.zip(this.bar, this.baz)`,
			input: map[string]any{
				"foo": []any{"a", "b", "c"},
				"bar": []any{1, 2, 3},
				"baz": []any{
					[]any{"x"},
					[]any{"y"},
					[]any{"z"},
				},
			},
			output: map[string]any{
				"foo": [][]any{
					{"a", 1, []any{"x"}},
					{"b", 2, []any{"y"}},
					{"c", 3, []any{"z"}},
				},
			},
		},
		{
			name:    "non array arg",
			mapping: `root.foo = this.foo.zip(this.bar)`,
			input: map[string]any{
				"foo": []any{"first"},
				"bar": "second",
			},
			execErr: "expected array value, got string",
		},
		{
			name:    "non array target",
			mapping: `root.foo = this.foo.zip(this.bar)`,
			input: map[string]any{
				"bar": []any{"first"},
				"foo": "second",
			},
			execErr: "expected array value, got string",
		},
		{
			name:    "jagged array value parameters",
			mapping: `root.foo = this.foo.zip(this.bar, this.baz)`,
			input: map[string]any{
				"baz": []any{"first", "second"},
				"bar": []any{"third"},
				"foo": []any{"forth"},
			},
			execErr: "can't zip different length array values",
		},
		{
			name:    "jagged array value and parameters",
			mapping: `root.foo = this.foo.zip(this.bar, this.baz)`,
			input: map[string]any{
				"baz": []any{"first", "second"},
				"bar": []any{"third", "forth"},
				"foo": []any{"fifth"},
			},
			execErr: "can't zip different length array values",
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
