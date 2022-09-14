package query

import (
	"fmt"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/require"
)

func TestIteratorMethods(t *testing.T) {
	literalFn := func(val any) Function {
		fn := NewLiteralFunction("", val)
		return fn
	}
	jsonFn := func(json string) Function {
		t.Helper()
		gObj, err := gabs.ParseJSON([]byte(json))
		require.NoError(t, err)
		fn := NewLiteralFunction("", gObj.Data())
		return fn
	}
	arithmetic := func(left, right Function, op ArithmeticOperator) Function {
		t.Helper()
		fn, err := NewArithmeticExpression(
			[]Function{left, right},
			[]ArithmeticOperator{op},
		)
		require.NoError(t, err)
		return fn
	}
	type easyMethod struct {
		name string
		args []any
	}
	methods := func(fn Function, methods ...easyMethod) Function {
		t.Helper()
		for _, m := range methods {
			var err error
			fn, err = InitMethodHelper(m.name, fn, m.args...)
			require.NoError(t, err)
		}
		return fn
	}
	method := func(name string, args ...any) easyMethod {
		return easyMethod{name: name, args: args}
	}

	tests := map[string]struct {
		input  Function
		value  *any
		output any
		err    string
	}{
		"check map each": {
			input: func() Function {
				fn, err := newMapEachMethod(jsonFn(`["foo","bar"]`), methods(
					NewFieldFunction(""),
					method("uppercase"),
				))
				require.NoError(t, err)
				return fn
			}(),
			output: []any{"FOO", "BAR"},
		},
		"check map each 2": {
			input: func() Function {
				fn, err := newMapEachMethod(
					jsonFn(`["foo","bar"]`),
					methods(
						literalFn("(%v)"),
						method("format", NewFieldFunction("")),
						method("uppercase"),
					))
				require.NoError(t, err)
				return fn
			}(),
			output: []any{"(FOO)", "(BAR)"},
		},
		"check map each object": {
			input: func() Function {
				fn, err := newMapEachMethod(
					jsonFn(`{"foo":"hello world","bar":"this is ash"}`),
					methods(
						NewFieldFunction("value"),
						method("uppercase"),
					))
				require.NoError(t, err)
				return fn
			}(),
			output: map[string]any{
				"foo": "HELLO WORLD",
				"bar": "THIS IS ASH",
			},
		},
		"check filter array": {
			input: func() Function {
				fn, err := newFilterMethod(
					jsonFn(`[2,14,4,11,7]`),
					arithmetic(
						NewFieldFunction(""),
						NewLiteralFunction("", 10.0),
						ArithmeticGt,
					))
				require.NoError(t, err)
				return fn
			}(),
			output: []any{14.0, 11.0},
		},
		"check filter object": {
			input: func() Function {
				fn, err := newFilterMethod(
					jsonFn(`{"foo":"hello ! world","bar":"this is ash","baz":"im cool!"}`),
					methods(
						NewFieldFunction("value"),
						method("contains", "!"),
					))
				require.NoError(t, err)
				return fn
			}(),
			output: map[string]any{
				"foo": "hello ! world",
				"baz": "im cool!",
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res, err := test.input.Exec(FunctionContext{
				Maps: map[string]Function{},
			}.WithValueFunc(func() *any { return test.value }))
			if len(test.err) > 0 {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.output, res)
		})
	}
}

func filterFunction() Function {
	i := 0
	return ClosureFunction("", func(ctx FunctionContext) (any, error) {
		i++
		return i%100 == 0, nil
	}, nil)
}

func benchFilterIter(b *testing.B, n, m int) {
	startingArray := make([]any, n)
	for i := range startingArray {
		startingArray[i] = fmt.Sprintf("foo%v", i)
	}

	var err error
	var fn Function = NewLiteralFunction("", startingArray)
	filter := filterFunction()
	for i := 0; i < m; i++ {
		fn, err = newFilterMethod(fn, filter)
		require.NoError(b, err)
	}

	startingContext := FunctionContext{}.WithValue(startingArray)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fn.Exec(startingContext)
		require.NoError(b, err)
	}
}

func benchFilterNoIter(b *testing.B, n, m int) {
	startingArray := make([]any, n)
	for i := range startingArray {
		startingArray[i] = fmt.Sprintf("foo%v", i)
	}

	var err error
	var fn Function = NewLiteralFunction("", startingArray)
	filter := filterFunction()
	for i := 0; i < m; i++ {
		fn, err = InitMethodHelper("filter", fn, filter)
		require.NoError(b, err)
	}

	startingContext := FunctionContext{}.WithValue(startingArray)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fn.Exec(startingContext)
		require.NoError(b, err)
	}
}

func BenchmarkFilterIter(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		for _, m := range []int{1, 10, 100} {
			n, m := n, m
			b.Run(fmt.Sprintf("OldN%vM%v", n, m), func(b *testing.B) {
				benchFilterNoIter(b, n, m)
			})
			b.Run(fmt.Sprintf("NewN%vM%v", n, m), func(b *testing.B) {
				benchFilterIter(b, n, m)
			})
		}
	}
}
