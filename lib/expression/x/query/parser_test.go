package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionParserErrors(t *testing.T) {
	tests := map[string]struct {
		input string
		err   string
	}{
		"bad function": {
			input: `not a function`,
			err:   `char 0: unrecognised function 'not', expected one of: [batch_size content count error field hostname json meta timestamp timestamp_unix timestamp_unix_nano timestamp_utc uuid_v4]`,
		},
		"bad function 2": {
			input: `not_a_function()`,
			err:   `char 0: unrecognised function 'not_a_function', expected one of: [batch_size content count error field hostname json meta timestamp timestamp_unix timestamp_unix_nano timestamp_utc uuid_v4]`,
		},
		"bad args 2": {
			input: `json("foo`,
			err:   `char 5: failed to parse function arguments: expected one of: [boolean number quoted-string]`,
		},
		"bad args 3": {
			input: `json(`,
			err:   `char 5: failed to parse function arguments: unexpected end of input`,
		},
		"bad args 4": {
			input: `json(0,`,
			err:   `char 7: failed to parse function arguments: unexpected end of input`,
		},
		"bad args 5": {
			input: `json`,
			err:   `char 4: expected params '()' after function: 'json'`,
		},
		"bad args 6": {
			input: `json(foo)`,
			err:   `char 5: failed to parse function arguments: expected one of: [boolean number quoted-string]`,
		},
		"bad args 7": {
			input: `json(5)`,
			err:   `char 4: expected string param, received int64`,
		},
		"bad args 8": {
			input: `json(false)`,
			err:   `char 4: expected string param, received bool`,
		},
		"bad args 9": {
			input: `json(json("foo"))`,
			err:   `char 5: failed to parse function arguments: expected one of: [boolean number quoted-string]`,
		},
		"bad operators": {
			input: `json("foo") + `,
			err:   `char 14: unexpected end of input`,
		},
		"bad expression": {
			input: `(json("foo") `,
			err:   `char 13: unexpected end of input`,
		},
		"bad expression 2": {
			input: `(json("foo") + `,
			err:   `char 15: unexpected end of input`,
		},
		"bad expression 3": {
			input: `(json("foo") + meta("bar") `,
			err:   `char 27: unexpected end of input`,
		},
		"bad method": {
			input: `json("foo").nope()`,
			err:   `char 12: unrecognised method 'nope', expected one of: [from from_all map or sum]`,
		},
		"bad method args": {
			input: `json("foo").from`,
			err:   `char 12: expected params '()' after method: 'from'`,
		},
		"bad method args 2": {
			input: `json("foo").from(`,
			err:   `char 17: failed to parse method arguments: unexpected end of input`,
		},
		"bad method args 3": {
			input: `json("foo").from()`,
			err:   `char 16: expected one argument, received: 0`,
		},
		"bad method args 4": {
			input: `json("foo").from("nah")`,
			err:   `char 16: expected int param, received string`,
		},
		"bad map args": {
			input: `json("foo").map()`,
			err:   `char 15: expected one argument, received: 0`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := tryParse(test.input)
			_ = assert.Error(t, err) &&
				assert.Equal(t, test.err, err.Error())
		})
	}
}

func TestFunctionParserLimits(t *testing.T) {
	tests := map[string]struct {
		input     string
		remaining string
	}{
		"nothing": {
			input:     `json("foo") + meta("bar")`,
			remaining: ``,
		},
		"space before": {
			input:     `   json("foo") + meta("bar")`,
			remaining: ``,
		},
		"space before 2": {
			input:     `   json("foo")   +    meta("bar")`,
			remaining: ``,
		},
		"extra text": {
			input:     `json("foo") and this`,
			remaining: `and this`,
		},
		"extra text 2": {
			input:     `json("foo") + meta("bar") and this`,
			remaining: `and this`,
		},
		"extra text 3": {
			input:     `json("foo")+meta("bar")and this`,
			remaining: `and this`,
		},
		"extra text 4": {
			input:     `json("foo")+meta("bar")         and this`,
			remaining: `and this`,
		},
		"squiggly bracket": {
			input:     `json("foo")}`,
			remaining: `}`,
		},
		"normal bracket": {
			input:     `json("foo"))`,
			remaining: `)`,
		},
		"normal bracket 2": {
			input:     `json("foo"))))`,
			remaining: `)))`,
		},
		"normal bracket 3": {
			input:     `json("foo")) + json("bar")`,
			remaining: `) + json("bar")`,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := Parse([]rune(test.input))
			_ = assert.NoError(t, res.Err) &&
				assert.Equal(t, test.remaining, string(res.Remaining))
		})
	}
}
