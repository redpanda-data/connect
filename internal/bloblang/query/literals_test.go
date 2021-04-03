package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiterals(t *testing.T) {
	mustVal := func(i interface{}, err error) interface{} {
		t.Helper()
		require.NoError(t, err)
		return i
	}

	tests := map[string]struct {
		input   interface{}
		value   interface{}
		output  interface{}
		err     error
		targets []TargetPath
	}{
		"dynamic object values": {
			input: mustVal(NewMapLiteral(
				[][2]interface{}{
					{"test1", NewFieldFunction("first")},
					{"test2", NewFieldFunction("second")},
					{"deleteme", Delete(nil)},
					{"donotmapme", Nothing(nil)},
					{"test3", "static"},
				},
			)),
			value: map[string]interface{}{
				"first":  "foo",
				"second": "bar",
			},
			output: map[string]interface{}{
				"test1": "foo",
				"test2": "bar",
				"test3": "static",
			},
			targets: []TargetPath{
				NewTargetPath(TargetValue, "first"),
				NewTargetPath(TargetValue, "second"),
			},
		},
		"dynamic object keys and values": {
			input: mustVal(NewMapLiteral(
				[][2]interface{}{
					{NewFieldFunction("first"), NewFieldFunction("second")},
					{"test2", "static"},
				},
			)),
			value: map[string]interface{}{
				"first":  "foo",
				"second": "bar",
			},
			output: map[string]interface{}{
				"foo":   "bar",
				"test2": "static",
			},
			targets: []TargetPath{
				NewTargetPath(TargetValue, "first"),
				NewTargetPath(TargetValue, "second"),
			},
		},
		"object literal function keys and values": {
			input: mustVal(NewMapLiteral(
				[][2]interface{}{
					{NewLiteralFunction("", "first"), NewLiteralFunction("", "second")},
					{NewLiteralFunction("", "third"), NewLiteralFunction("", "fourth")},
				},
			)),
			value: map[string]interface{}{},
			output: map[string]interface{}{
				"first": "second",
				"third": "fourth",
			},
		},
		"static object": {
			input: mustVal(NewMapLiteral(
				[][2]interface{}{
					{"test1", "static1"},
					{"test2", "static2"},
					{"deleteme", Delete(nil)},
					{"donotmapme", Nothing(nil)},
					{"test3", "static3"},
				},
			)),
			output: map[string]interface{}{
				"test1": "static1",
				"test2": "static2",
				"test3": "static3",
			},
		},
		"dynamic array values": {
			input: NewArrayLiteral(
				NewFieldFunction("first"),
				NewFieldFunction("second"),
				Delete(nil),
				"static",
				Nothing(nil),
				NewLiteralFunction("", "static literal"),
			),
			value: map[string]interface{}{
				"first":  "foo",
				"second": "bar",
			},
			output: []interface{}{
				"foo",
				"bar",
				"static",
				"static literal",
			},
			targets: []TargetPath{
				NewTargetPath(TargetValue, "first"),
				NewTargetPath(TargetValue, "second"),
			},
		},
		"static array values": {
			input: NewArrayLiteral(
				"static1",
				Delete(nil),
				NewLiteralFunction("", "static2"),
				Nothing(nil),
				"static3",
			),
			output: []interface{}{
				"static1",
				"static2",
				"static3",
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			var err error
			var targets []TargetPath

			res := test.input
			if fn, ok := test.input.(Function); ok {
				res, err = fn.Exec(FunctionContext{
					Maps: map[string]Function{},
				}.WithValue(test.value))
				_, targets = fn.QueryTargets(TargetsContext{
					Maps: map[string]Function{},
				})
			}

			if test.err != nil {
				require.EqualError(t, err, test.err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.output, res)
			require.Equal(t, test.targets, targets)
		})
	}
}
