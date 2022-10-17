package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	mustFunc := func(fn Function, err error) Function {
		t.Helper()
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		input    Function
		value    *any
		output   any
		err      error
		messages []easyMsg
		index    int
	}{
		"if false": {
			input: NewIfFunction(
				mustFunc(NewArithmeticExpression(
					[]Function{
						NewLiteralFunction("", int64(10)),
						NewLiteralFunction("", int64(20)),
					},
					[]ArithmeticOperator{
						ArithmeticGt,
					},
				)),
				NewLiteralFunction("", "foo"),
				nil,
				nil,
			),
			output: Nothing(nil),
		},
		"if false else": {
			input: NewIfFunction(
				mustFunc(NewArithmeticExpression(
					[]Function{
						NewLiteralFunction("", int64(10)),
						NewLiteralFunction("", int64(20)),
					},
					[]ArithmeticOperator{
						ArithmeticGt,
					},
				)),
				NewLiteralFunction("", "foo"),
				nil,
				NewLiteralFunction("", "bar"),
			),
			output: "bar",
		},
		"if else if": {
			input: NewIfFunction(
				NewLiteralFunction("", false),
				NewLiteralFunction("", "foo"),
				[]ElseIf{
					{
						QueryFn: NewLiteralFunction("", false),
						MapFn:   NewLiteralFunction("", "bar"),
					},
					{
						QueryFn: NewLiteralFunction("", true),
						MapFn:   NewLiteralFunction("", "baz"),
					},
				},
				NewLiteralFunction("", "buz"),
			),
			output: "baz",
		},
		"if true": {
			input: NewIfFunction(
				mustFunc(NewArithmeticExpression(
					[]Function{
						NewLiteralFunction("", int64(10)),
						NewLiteralFunction("", int64(20)),
					},
					[]ArithmeticOperator{
						ArithmeticLt,
					},
				)),
				NewLiteralFunction("", "foo"),
				nil,
				NewLiteralFunction("", Nothing(nil)),
			),
			output: "foo",
		},
		"if query fails": {
			input: NewIfFunction(
				NewVarFunction("doesnt exist"),
				NewLiteralFunction("", "foo"),
				nil,
				NewLiteralFunction("", "bar"),
			),
			err: errors.New("failed to check if condition: variables were undefined"),
		},
		"match context fails": {
			input: NewMatchFunction(
				NewVarFunction("doesnt exist"),
				NewMatchCase(NewLiteralFunction("", true), NewLiteralFunction("", "foo")),
			),
			err: errors.New("variables were undefined"),
		},
		"match first case fails": {
			input: NewMatchFunction(
				NewLiteralFunction("", "context"),
				NewMatchCase(NewVarFunction("doesnt exist"), NewLiteralFunction("", "foo")),
				NewMatchCase(NewLiteralFunction("", true), NewLiteralFunction("", "bar")),
			),
			err: errors.New("failed to check match case 0: variables were undefined"),
		},
		"match second case fails": {
			input: NewMatchFunction(
				NewLiteralFunction("", "context"),
				NewMatchCase(NewLiteralFunction("", true), NewLiteralFunction("", "bar")),
				NewMatchCase(NewVarFunction("doesnt exist"), NewLiteralFunction("", "foo")),
			),
			output: "bar",
		},
		"match context empty": {
			input: NewMatchFunction(
				nil,
				NewMatchCase(NewLiteralFunction("", true), NewFieldFunction("")),
			),
			value: func() *any {
				var v any = "context"
				return &v
			}(),
			output: "context",
		},
		"match context": {
			input: NewMatchFunction(
				NewLiteralFunction("", "context"),
				NewMatchCase(NewLiteralFunction("", true), NewFieldFunction("")),
			),
			output: "context",
		},
		"match context all fail": {
			input: NewMatchFunction(
				NewLiteralFunction("", "context"),
				NewMatchCase(NewLiteralFunction("", false), NewLiteralFunction("", "foo")),
				NewMatchCase(NewLiteralFunction("", false), NewLiteralFunction("", "bar")),
			),
			output: Nothing(nil),
		},
		"named context map arithmetic": {
			input: mustFunc(NewArithmeticExpression(
				[]Function{
					mustFunc(NewMapMethod(
						NewFieldFunction("foo"),
						NewNamedContextFunction("next", mustFunc(NewArithmeticExpression(
							[]Function{
								NewNamedContextFieldFunction("next", "bar"),
								NewFieldFunction("baz"),
							},
							[]ArithmeticOperator{
								ArithmeticAdd,
							},
						))),
					)),
					NewFieldFunction("foo.bar"),
				},
				[]ArithmeticOperator{
					ArithmeticAdd,
				},
			)),
			value: func() *any {
				var v any = map[string]any{
					"foo": map[string]any{
						"bar": 7,
					},
					"baz": 23,
				}
				return &v
			}(),
			output: int64(37),
		},
		"named context map to literal": {
			input: mustFunc(NewMapMethod(
				NewFieldFunction("foo"),
				NewNamedContextFunction("next", NewArrayLiteral(
					NewNamedContextFieldFunction("next", "bar"),
					NewFieldFunction("baz"),
				).(Function)),
			)),
			value: func() *any {
				var v any = map[string]any{
					"foo": map[string]any{
						"bar": 7,
					},
					"baz": 23,
				}
				return &v
			}(),
			output: []any{7, 23},
		},
		"dropped context map to literal": {
			input: mustFunc(NewMapMethod(
				NewFieldFunction("foo"),
				NewNamedContextFunction("_", NewArrayLiteral(
					NewFieldFunction("baz"),
				).(Function)),
			)),
			value: func() *any {
				var v any = map[string]any{
					"foo": map[string]any{
						"bar": 7,
					},
					"baz": 23,
				}
				return &v
			}(),
			output: []any{23},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			msg := message.QuickBatch(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSetMut(k, v)
					}
				}
				msg = append(msg, part)
			}

			for i := 0; i < 10; i++ {
				res, err := test.input.Exec(FunctionContext{
					Maps:     map[string]Function{},
					Index:    test.index,
					MsgBatch: msg,
				}.WithValueFunc(func() *any { return test.value }))
				if test.err != nil {
					require.EqualError(t, err, test.err.Error())
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, test.output, res)
			}

			// Ensure nothing changed
			for i, m := range test.messages {
				doc, err := msg.Get(i).AsStructuredMut()
				if err == nil {
					msg.Get(i).SetStructured(doc)
				}
				assert.Equal(t, m.content, string(msg.Get(i).AsBytes()))
			}
		})
	}
}

func TestExpressionTargets(t *testing.T) {
	mustFunc := func(fn Function, err error) Function {
		t.Helper()
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		input  Function
		output []TargetPath
	}{
		"named context map arithmetic": {
			input: mustFunc(NewMapMethod(
				NewFieldFunction("foo"),
				NewNamedContextFunction("next", mustFunc(NewArithmeticExpression(
					[]Function{
						NewNamedContextFieldFunction("next", "bar"),
						NewFieldFunction("baz"),
					},
					[]ArithmeticOperator{
						ArithmeticAdd,
					},
				))),
			)),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo"),
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "baz"),
			},
		},
		"named context map to literal": {
			input: mustFunc(NewMapMethod(
				NewFieldFunction("foo"),
				NewNamedContextFunction("next", NewArrayLiteral(
					NewNamedContextFieldFunction("next", "bar"),
					NewFieldFunction("baz"),
				).(Function)),
			)),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo"),
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "baz"),
			},
		},
		"dropped context map to literal": {
			input: mustFunc(NewMapMethod(
				NewFieldFunction("foo"),
				NewNamedContextFunction("_", NewArrayLiteral(
					NewFieldFunction("baz"),
				).(Function)),
			)),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo"),
				NewTargetPath(TargetValue, "baz"),
			},
		},
		"if query path": {
			input: NewIfFunction(
				mustFunc(InitFunctionHelper("json", "foo.bar")),
				NewLiteralFunction("", "foo"),
				nil,
				mustFunc(InitFunctionHelper("var", "baz")),
			),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetVariable, "baz"),
			},
		},
		"if else if query path": {
			input: NewIfFunction(
				mustFunc(InitFunctionHelper("json", "foo.bar")),
				NewLiteralFunction("", "foo"),
				[]ElseIf{
					{
						QueryFn: mustFunc(InitFunctionHelper("json", "foo.baz")),
						MapFn:   NewLiteralFunction("", "bar"),
					},
					{
						QueryFn: mustFunc(InitFunctionHelper("meta", "buz")),
						MapFn:   mustFunc(InitFunctionHelper("meta", "quz")),
					},
				},
				mustFunc(InitFunctionHelper("var", "baz")),
			),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetVariable, "baz"),
				NewTargetPath(TargetValue, "foo", "baz"),
				NewTargetPath(TargetMetadata, "buz"),
				NewTargetPath(TargetMetadata, "quz"),
			},
		},
		"match empty context": {
			input: NewMatchFunction(
				nil,
				NewMatchCase(
					NewFieldFunction("foo"),
					NewFieldFunction("bar"),
				),
				NewMatchCase(
					NewFieldFunction("baz"),
					NewFieldFunction("buz"),
				),
			),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo"),
				NewTargetPath(TargetValue, "bar"),
				NewTargetPath(TargetValue, "baz"),
				NewTargetPath(TargetValue, "buz"),
			},
		},
		"match meta context": {
			input: NewMatchFunction(
				mustFunc(InitFunctionHelper("meta", "foo")),
				NewMatchCase(
					mustFunc(InitFunctionHelper("meta", "bar")),
					NewFieldFunction("baz"),
				),
				NewMatchCase(
					NewFieldFunction("buz"),
					NewLiteralFunction("", "qux"),
				),
			),
			output: []TargetPath{
				NewTargetPath(TargetMetadata, "bar"),
				NewTargetPath(TargetMetadata, "foo", "baz"),
				NewTargetPath(TargetMetadata, "foo", "buz"),
				NewTargetPath(TargetMetadata, "foo"),
			},
		},
		"match value context": {
			input: NewMatchFunction(
				NewFieldFunction("foo.bar"),
				NewMatchCase(
					mustFunc(InitFunctionHelper("meta", "bar")),
					NewFieldFunction("baz.buz"),
				),
				NewMatchCase(
					NewFieldFunction("qux.quz"),
					NewLiteralFunction("", "quack"),
				),
			),
			output: []TargetPath{
				NewTargetPath(TargetMetadata, "bar"),
				NewTargetPath(TargetValue, "foo", "bar", "baz", "buz"),
				NewTargetPath(TargetValue, "foo", "bar", "qux", "quz"),
				NewTargetPath(TargetValue, "foo", "bar"),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, res := test.input.QueryTargets(TargetsContext{
				Maps: map[string]Function{},
			})
			assert.Equal(t, test.output, res)
		})
	}
}
