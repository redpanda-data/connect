package field

import (
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			ei := NewExpression(test.input...)

			e := ei.(*expression)
			assert.Equal(t, test.output, e.static)
			assert.Equal(t, 0, len(e.resolvers))
		})
	}
}

func TestExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		expression Expression
		output     string
		messages   []easyMsg
		index      int
		escaped    bool
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
		"echo function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, ok := query.DeprecatedFunction("echo", "this")
					require.True(t, ok)
					return fn
				}()),
			),
			output: `this`,
		},
		"echo function 2": {
			expression: NewExpression(
				StaticResolver("foo "),
				NewQueryResolver(func() query.Function {
					fn, ok := query.DeprecatedFunction("echo", "")
					require.True(t, ok)
					return fn
				}()),
				StaticResolver(" bar"),
			),
			output: `foo  bar`,
		},
		"json function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("json")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("json", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 3": {
			expression: NewExpression(NewQueryResolver(func() query.Function {
				fn, err := query.InitFunction("json", "foo")
				require.NoError(t, err)
				return fn
			}())),
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 4": {
			expression: NewExpression(NewQueryResolver(func() query.Function {
				fn, err := query.InitFunction("json", "foo")
				require.NoError(t, err)
				return fn
			}())),
			output:  `{\"bar\":\"baz\"}`,
			index:   0,
			escaped: true,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"baz"}}`},
			},
		},
		"json_from function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethod("from", fn, int64(1))
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 2": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethod("from", fn, int64(0))
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `null`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 3": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("json", "foo")
					require.NoError(t, err)
					fn, err = query.InitMethod("from", fn, int64(-1))
					require.NoError(t, err)
					return fn
				}()),
			),
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
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"meta function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `from foo`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]string{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"metadata function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `from foo`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]string{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"metadata function not exist": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta", "foo")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: ``,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]string{
					"bar": "from bar",
				}},
			},
		},
		"all meta function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `{"bar":"from bar","foo":"from foo"}`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]string{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"all metadata function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta")
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `{"bar":"from bar","foo":"from foo"}`,
			messages: []easyMsg{
				{content: `hello world`, meta: map[string]string{
					"foo": "from foo",
					"bar": "from bar",
				}},
			},
		},
		"meta_from function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, err := query.InitFunction("meta", "bar")
					require.NoError(t, err)
					fn, err = query.InitMethod("from", fn, int64(1))
					require.NoError(t, err)
					return fn
				}()),
			),
			output: `from bar from 1`,
			messages: []easyMsg{
				{content: `first`, meta: map[string]string{
					"foo": "from foo from 0",
					"bar": "from bar from 0",
				}},
				{content: `second`, meta: map[string]string{
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

			msg := message.New(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.Metadata().Set(k, v)
					}
				}
				msg.Append(part)
			}

			var res string
			if test.escaped {
				res = string(test.expression.BytesEscaped(test.index, msg))
			} else {
				res = test.expression.String(test.index, msg)
			}
			assert.Equal(t, test.output, res)
		})
	}
}

func TestLegacyExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		expression Expression
		output     string
		messages   []easyMsg
		index      int
		escaped    bool
	}{
		"json function": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, ok := query.DeprecatedFunction("json_field", "")
					require.True(t, ok)
					return fn
				}()),
			),
			index:  1,
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function escaped": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, ok := query.DeprecatedFunction("json_field", "")
					require.True(t, ok)
					return fn
				}()),
			),
			escaped: true,
			index:   1,
			output:  `{\"foo\":\"bar\"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			expression: NewExpression(
				NewQueryResolver(func() query.Function {
					fn, ok := query.DeprecatedFunction("json_field", "1")
					require.True(t, ok)
					return fn
				}()),
			),
			index:  0,
			output: `null`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			msg := message.New(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.Metadata().Set(k, v)
					}
				}
				msg.Append(part)
			}

			var res string
			if test.escaped {
				res = string(test.expression.BytesEscapedLegacy(test.index, msg))
			} else {
				res = test.expression.StringLegacy(test.index, msg)
			}
			assert.Equal(t, test.output, res)
		})
	}
}
