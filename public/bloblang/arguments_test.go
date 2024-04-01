package bloblang

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArgumentsLength(t *testing.T) {
	var a, b, c int
	set := NewArgSpec().IntVar(&a).IntVar(&b).IntVar(&c)

	assert.EqualError(t, set.Extract([]any{0, 1}), "expected 3 arguments, received 2")
	assert.EqualError(t, set.Extract([]any{0, 1, 2, 3}), "expected 3 arguments, received 4")
	assert.NoError(t, set.Extract([]any{0, 1, 2}))
}

func TestArgumentTypes(t *testing.T) {
	var a int
	var b int64
	var c float64
	var d bool
	var e string
	var f any
	set := NewArgSpec().
		IntVar(&a).
		Int64Var(&b).
		Float64Var(&c).
		BoolVar(&d).
		StringVar(&e).
		AnyVar(&f)

	testCases := []struct {
		name string
		args []any
		exp  []any
		err  string
	}{
		{
			name: "bad int",
			args: []any{
				"nope", int64(2), 3.0, true, "hello world", "and this",
			},
			err: `bad argument 0: expected int value, got string (nope)`,
		},
		{
			name: "bad int64",
			args: []any{
				int64(1), "nope", 3.0, true, "hello world", "and this",
			},
			err: `bad argument 1: expected int64 value, got string (nope)`,
		},
		{
			name: "bad float64",
			args: []any{
				int64(1), int64(2), "nope", true, "hello world", "and this",
			},
			err: `bad argument 2: expected float64 value, got string (nope)`,
		},
		{
			name: "bad bool",
			args: []any{
				int64(1), int64(2), 3.0, "nope", "hello world", "and this",
			},
			err: `bad argument 3: expected bool value, got string (nope)`,
		},
		{
			name: "bad string",
			args: []any{
				int64(1), int64(2), 3.0, true, 30, "and this",
			},
			err: "bad argument 4: expected string value, got int (30)",
		},
		{
			name: "good values",
			args: []any{
				int64(1), int64(2), 3.0, true, "hello world", "and this",
			},
			exp: []any{
				1, int64(2), 3.0, true, "hello world", "and this",
			},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := set.Extract(test.args)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.exp, []any{
					a, b, c, d, e, f,
				})
			}
		})
	}
}
