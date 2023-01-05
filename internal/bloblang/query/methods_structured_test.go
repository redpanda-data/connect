package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMethodImmutability(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target any
		args   []any
		exp    any
	}{
		{
			name:   "merge arrays",
			method: "merge",
			target: []any{"foo", "bar"},
			args: []any{
				[]any{"baz", "buz"},
			},
			exp: []any{"foo", "bar", "baz", "buz"},
		},
		{
			name:   "merge into an array",
			method: "merge",
			target: []any{"foo", "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: []any{"foo", "bar", map[string]any{"baz": "buz"}},
		},
		{
			name:   "merge objects",
			method: "merge",
			target: map[string]any{"foo": "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: map[string]any{
				"foo": "bar",
				"baz": "buz",
			},
		},
		{
			name:   "merge collision",
			method: "merge",
			target: map[string]any{"foo": "bar", "baz": "buz"},
			args: []any{
				map[string]any{"foo": "qux"},
			},
			exp: map[string]any{
				"foo": []any{"bar", "qux"},
				"baz": "buz",
			},
		},

		{
			name:   "assign arrays",
			method: "assign",
			target: []any{"foo", "bar"},
			args: []any{
				[]any{"baz", "buz"},
			},
			exp: []any{"foo", "bar", "baz", "buz"},
		},
		{
			name:   "assign into an array",
			method: "assign",
			target: []any{"foo", "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: []any{"foo", "bar", map[string]any{"baz": "buz"}},
		},
		{
			name:   "assign objects",
			method: "assign",
			target: map[string]any{"foo": "bar"},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: map[string]any{
				"foo": "bar",
				"baz": "buz",
			},
		},
		{
			name:   "assign collision",
			method: "assign",
			target: map[string]any{"foo": "bar", "baz": "buz"},
			args: []any{
				map[string]any{"foo": "qux"},
			},
			exp: map[string]any{
				"foo": "qux",
				"baz": "buz",
			},
		},

		{
			name:   "contains object positive",
			method: "contains",
			target: []any{
				map[string]any{"foo": "bar"},
			},
			args: []any{
				map[string]any{"foo": "bar"},
			},
			exp: true,
		},
		{
			name:   "contains object negative",
			method: "contains",
			target: []any{
				map[string]any{"foo": "bar"},
			},
			args: []any{
				map[string]any{"baz": "buz"},
			},
			exp: false,
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone := IClone(test.target)
			argsClone := IClone(test.args).([]any)

			fn, err := InitMethodHelper(test.method, NewLiteralFunction("", targetClone), argsClone...)
			require.NoError(t, err)

			res, err := fn.Exec(FunctionContext{
				Maps:     map[string]Function{},
				Index:    0,
				MsgBatch: nil,
			})
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone)
			assert.Equal(t, test.args, argsClone)
		})
	}
}
