package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestLiteralParserErrors(t *testing.T) {
	tests := map[string]struct {
		input string
		err   string
	}{
		"bad object key": {
			input: `{5:"foo"}`,
			err:   `line 1 char 1: object keys must be strings, received: int64`,
		},
		"bad array element": {
			input: `[5,null,"unterminated string]`,
			err:   `line 1 char 30: required: expected end quote`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := tryParseQuery(test.input)
			assert.Equal(t, test.err, err.ErrorAtPosition([]rune(test.input)))
		})
	}
}

func TestLiteralParser(t *testing.T) {
	tests := map[string]struct {
		mapping  string
		result   any
		parseErr string
		err      string
		value    *any
	}{
		"basic map": {
			mapping: `{"foo":"bar"}`,
			result: map[string]any{
				"foo": "bar",
			},
		},
		"basic map trailing comma": {
			mapping: `{"foo":"bar",}`,
			result: map[string]any{
				"foo": "bar",
			},
		},
		"dynamic map": {
			mapping: `{"foo":(5 + 5)}`,
			result: map[string]any{
				"foo": int64(10),
			},
		},
		"dynamic map trailing comma": {
			mapping: `{"foo":(5 + 5),}`,
			result: map[string]any{
				"foo": int64(10),
			},
		},
		"dynamic map dynamic key": {
			mapping: `{("foobar".uppercase()):5}`,
			result: map[string]any{
				"FOOBAR": int64(5),
			},
		},
		"dynamic map nested": {
			mapping: `{"foo":{"bar":(5 + 5)}}`,
			result: map[string]any{
				"foo": map[string]any{
					"bar": int64(10),
				},
			},
		},
		"dynamic array": {
			mapping: `["foo",(5 + 5),null]`,
			result: []any{
				"foo", int64(10), nil,
			},
		},
		"dynamic array trailing comma": {
			mapping: `["foo",(5 + 5),null,]`,
			result: []any{
				"foo", int64(10), nil,
			},
		},
		"dynamic array nested": {
			mapping: `["foo",[(5 + 5),"bar"],null]`,
			result: []any{
				"foo", []any{int64(10), "bar"}, nil,
			},
		},
		"bad array element": {
			mapping:  `["foo",(5 + "not a number"),"bar"]`,
			parseErr: "cannot add types number (from number literal) and string (from string literal): 5 + \"",
		},
		"bad object value": {
			mapping:  `{"foo":(5 + "not a number")}`,
			parseErr: "cannot add types number (from number literal) and string (from string literal): 5 + \"",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := queryParser(Context{
				Functions: query.AllFunctions,
				Methods:   query.AllMethods,
			})([]rune(test.mapping))
			if test.parseErr != "" {
				assert.Equal(t, test.parseErr, res.Err.Error())
				return
			}
			require.Nil(t, res.Err)
			require.Implements(t, (*query.Function)(nil), res.Payload)
			q := res.Payload

			result, err := q.Exec(query.FunctionContext{
				Index: 0, MsgBatch: message.QuickBatch(nil),
			}.WithValueFunc(func() *any { return test.value }))
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				assert.Equal(t, test.result, result)
			}
		})
	}
}
