package field

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
)

func TestStaticExpressionOptimization(t *testing.T) {
	tests := map[string]string{
		"a static string":                          "a static string",
		"a string ${{!with escapes}} still static": "a string ${!with escapes} still static",
		"a string $ with dollars still static":     "a string $ with dollars still static",
		"  ":                                       "  ",
		"":                                         "",
	}

	for k, v := range tests {
		t.Run(k, func(t *testing.T) {
			e, err := parse(k)
			_ = assert.NoError(t, err) &&
				assert.Equal(t, v, e.static) &&
				assert.Equal(t, 0, len(e.resolvers)) &&
				assert.Equal(t, v, e.String(0, message.New(nil))) &&
				assert.Equal(t, v, e.StringLegacy(0, message.New(nil))) &&
				assert.Equal(t, v, string(e.Bytes(0, message.New(nil)))) &&
				assert.Equal(t, v, string(e.BytesEscaped(0, message.New(nil)))) &&
				assert.Equal(t, v, string(e.BytesLegacy(0, message.New(nil)))) &&
				assert.Equal(t, v, string(e.BytesEscapedLegacy(0, message.New(nil))))
		})
	}
}

func TestExpressionParserErrors(t *testing.T) {
	tests := map[string]struct {
		input string
		err   string
	}{
		"bad function": {
			input: `static string ${!not a function} hello world`,
			err:   `failed to parse expression: char 20: failed to parse function arguments: expected: (`,
		},
		"bad function 2": {
			input: `static string ${!not_a_function()} hello world`,
			err:   `failed to parse expression: char 17: unrecognised function 'not_a_function', expected one of: [batch_size content count error field hostname json meta timestamp timestamp_unix timestamp_unix_nano timestamp_utc uuid_v4]`,
		},
		"bad args": {
			input: `foo ${!json("foo") whats this?} bar`,
			err:   `failed to parse expression: char 19: unexpected contents at end of expression: whats this?`,
		},
		"bad args 2": {
			input: `foo ${!json("foo} bar`,
			err:   `failed to parse expression: char 12: failed to parse function arguments: expected one of: [boolean number quoted-string]`,
		},
		"bad args 3": {
			input: `foo ${!json(} bar`,
			err:   `failed to parse expression: char 12: failed to parse function arguments: unexpected end of input`,
		},
		"bad args 4": {
			input: `foo ${!json(0,} bar`,
			err:   `failed to parse expression: char 14: failed to parse function arguments: unexpected end of input`,
		},
		"bad args 5": {
			input: `foo ${!json} bar`,
			err:   `failed to parse expression: char 11: expected: function-parameters`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parse(test.input)
			_ = assert.Error(t, err) &&
				assert.Equal(t, test.err, err.Error())
		})
	}
}

func TestExpressions(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		index    int
		escaped  bool
	}{
		"static string": {
			input:  `static string hello world`,
			output: `static string hello world`,
		},
		"unsuspicious string": {
			input:  `${{! not a thing`,
			output: `${{! not a thing`,
		},
		"unsuspicious string 2": {
			input:  `${! not a thing`,
			output: `${! not a thing`,
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
		"echo function": {
			input:  `${!echo:this}`,
			output: `this`,
		},
		"echo function 2": {
			input:  `foo ${!echo:} bar`,
			output: `foo  bar`,
		},
		"echo function 3": {
			input:  `${!echo} bar`,
			output: ` bar`,
		},
		"echo function 4": {
			input:   `${!echo:"this"}`,
			output:  `\"this\"`,
			escaped: true,
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
			input:   `${!json("foo")}`,
			output:  `{\"bar\":\"baz\"}`,
			index:   0,
			escaped: true,
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
				{content: `not json`},
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

			e, err := parse(test.input)
			if !assert.NoError(t, err) {
				return
			}
			var res string
			if test.escaped {
				res = string(e.BytesEscaped(test.index, msg))
			} else {
				res = e.String(test.index, msg)
			}
			assert.Equal(t, test.output, res)
		})
	}
}
