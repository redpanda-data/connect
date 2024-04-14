package field

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestStaticExpressionOptimization(t *testing.T) {
	tests := []struct {
		input  []Resolver
		output string
	}{
		{
			input: []Resolver{
				StaticResolver("a static string"),
			},
			output: "a static string",
		},
		{
			input: []Resolver{
				StaticResolver("multiple "),
				StaticResolver("static "),
				StaticResolver("strings"),
			},
			output: "multiple static strings",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.output, func(t *testing.T) {
			e := NewExpression(test.input...)
			assert.Equal(t, test.output, e.static)
			assert.Empty(t, e.resolvers)
		})
	}
}

func TestExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	tests := map[string]struct {
		expression  *Expression
		output      string
		numDyn      int
		messages    []easyMsg
		index       int
		errContains string
	}{
		"static string": {
			expression: NewExpression(
				StaticResolver("static string hello world"),
			),
			output: `static string hello world`,
		},
		"unsuspicious string": {
			expression: NewExpression(
				StaticResolver("${{! not a thing"),
			),
			output: `${{! not a thing`,
		},
		"unsuspicious string 2": {
			expression: NewExpression(
				StaticResolver("${! not a thing"),
			),
			output: `${! not a thing`,
		},
		"dollar on its own": {
			expression: NewExpression(
				StaticResolver("hello $ world"),
			),
			output: `hello $ world`,
		},
		"json function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 3": {
			expression: NewExpression(NewQueryResolver(func() query.Function {
				fn, err := query.InitFunctionHelper("json", "foo")
				require.NoError(t, err)
				return fn
			}())),
			numDyn: 1,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 4": {
			expression: NewExpression(NewQueryResolver(func() query.Function {
				fn, err := query.InitFunctionHelper("json", "foo")
				require.NoError(t, err)
				return fn
			}())),
			numDyn: 1,
			output: `{"bar":"baz"}`,
			index:  0,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"baz"}}`},
			},
		},
		"two json functions": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					return fn
				}()),
				StaticResolver(" and "),
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "bar")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 2,
			output: `foo value and bar value`,
			index:  0,
			messages: []easyMsg{
				{content: `{"foo":"foo value","bar":"bar value"}`},
			},
		},
		"json_from function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethodHelper("from", fn, int64(1))
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function gone wrong": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			messages: []easyMsg{
				{content: `not valid json`},
			},
			errContains: "invalid character 'o'",
		},
		"json_from function 2": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethodHelper("from", fn, int64(0))
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `null`,
			messages: []easyMsg{
				{content: `{}`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 3": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethodHelper("from", fn, int64(-1))
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"this expression": {
			expression: NewExpression(
				NewQueryResolver(query.NewFieldFunction("foo")),
			),
			numDyn: 1,
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"meta function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `from foo`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]any{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"metadata function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `from foo`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]any{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"metadata function not exist": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `null`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]any{
					"bar": "from bar",
				}},
			},
		},
		"all meta function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `{"bar":"from bar","foo":"from foo"}`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]any{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"all metadata function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta")
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `{"bar":"from bar","foo":"from foo"}`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]any{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"meta_from function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunctionHelper("meta", "bar")
					require.NoError(t, err)
					fn, err = query.InitMethodHelper("from", fn, int64(1))
					require.NoError(t, err)
					return fn
				}()),
			),
			numDyn: 1,
			output: `from bar from 1`,
			messages: []easyMsg{
				{content: `first`, meta: map[string]any{
					"foo": "from foo from 0",
					"bar": "from bar from 0",
				}},
				{content: `second`, meta: map[string]any{
					"foo": "from foo from 1",
					"bar": "from bar from 1",
				}},
			},
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

			assert.Equal(t, test.numDyn, test.expression.NumDynamicExpressions())
			res, err := test.expression.String(test.index, msg)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.output, res)
			}
		})
	}
}
