package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestFieldStaticExpressionOptimization(t *testing.T) {
	tests := map[string]string{
		"a static string":                          "a static string",
		"a string ${{!with escapes}} still static": "a string ${!with escapes} still static",
		"a string $ with dollars still static":     "a string $ with dollars still static",
		"  ":                                       "  ",
		"":                                         "",
	}

	for k, v := range tests {
		t.Run(k, func(t *testing.T) {
			rs, pErr := parseFieldResolvers(GlobalContext(), k)
			require.Nil(t, pErr)

			e := field.NewExpression(rs...)

			res, err := e.String(0, message.QuickBatch(nil))
			require.NoError(t, err)
			assert.Equal(t, v, res)

			bres, err := e.Bytes(0, message.QuickBatch(nil))
			require.NoError(t, err)
			assert.Equal(t, v, string(bres))
		})
	}
}

func TestFieldExpressionParserErrors(t *testing.T) {
	tests := map[string]struct {
		input string
		err   string
	}{
		"bad function": {
			input: `static string ${!not a function} hello world`,
			err:   `char 22: required: expected end of expression`,
		},
		"bad function 2": {
			input: `static string ${!not_a_function()} hello world`,
			err:   `char 18: unrecognised function 'not_a_function'`,
		},
		"bad args": {
			input: `foo ${!json("foo") whats this?} bar`,
			err:   `char 20: required: expected end of expression`,
		},
		"bad args 2": {
			input: `foo ${!json("foo} bar`,
			err:   `char 22: required: expected end quote`,
		},
		"bad args 3": {
			input: `foo ${!json(} bar`,
			err:   `char 13: required: expected function argument`,
		},
		"bad args 4": {
			input: `foo ${!json(0,} bar`,
			err:   `char 15: required: expected function argument`,
		},
		"unfinished escape": {
			input: `a string that ends ${{!with unfinished escapes`,
			err:   `char 24: required: expected end of escaped expression`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseField(GlobalContext(), test.input)
			require.NotNil(t, err)
			require.Equal(t, test.err, err.ErrorAtChar([]rune(test.input)))
		})
	}
}

func TestFieldExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		index    int
	}{
		"static string": {
			input:  `static string hello world`,
			output: `static string hello world`,
		},
		"dollar on its own": {
			input:  `hello $ world`,
			output: `hello $ world`,
		},
		"dollar on its own 2": {
			input:  `hello world $`,
			output: `hello world $`,
		},
		"dollar on its own 3": {
			input:  `$ hello world`,
			output: `$ hello world`,
		},
		"escaped string": {
			input:  `hello ${{!this is escaped}} world`,
			output: `hello ${!this is escaped} world`,
		},
		"escaped string 2": {
			input:  `hello world ${{!this is escaped}}`,
			output: `hello world ${!this is escaped}`,
		},
		"escaped string 3": {
			input:  `${{!this is escaped}} hello world`,
			output: `${!this is escaped} hello world`,
		},
		"escaped string 4": {
			input:  `${{!this is escaped}}`,
			output: `${!this is escaped}`,
		},
		"json function": {
			input:  `${!json()}`,
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			input:  `${!json("foo")}`,
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 3": {
			input:  `${!json("foo")}`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 4": {
			input:  `${!json("foo")}`,
			output: `{"bar":"baz"}`,
			index:  0,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"baz"}}`},
			},
		},
		"json function 5": {
			input:  `${!json("foo")   }`,
			output: `{"bar":"baz"}`,
			index:  0,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"baz"}}`},
			},
		},
		"json_from function": {
			input:  `${!json("foo").from(1)}`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 2": {
			input:  `${!json("foo").from(0)}`,
			output: `null`,
			messages: []easyMsg{
				{content: `{}`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 3": {
			input:  `${!json("foo").from(-1)}`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
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

			e, pErr := ParseField(GlobalContext(), test.input)
			require.Nil(t, pErr)

			res, err := e.String(test.index, msg)
			require.NoError(t, err)
			assert.Equal(t, test.output, res)
		})
	}
}
