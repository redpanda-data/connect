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
			err:   `char 0: unrecognised function 'not a function', expected one of: [batch_size content content_from count error error_from hostname json json_from meta meta_from timestamp timestamp_unix timestamp_unix_nano timestamp_utc uuid_v4]`,
		},
		"bad function 2": {
			input: `not_a_function()`,
			err:   `char 0: unrecognised function 'not_a_function', expected one of: [batch_size content content_from count error error_from hostname json json_from meta meta_from timestamp timestamp_unix timestamp_unix_nano timestamp_utc uuid_v4]`,
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
			input: `json_from(0,`,
			err:   `char 12: failed to parse function arguments: unexpected end of input`,
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
			err:   `char 0: expected string param, received int64`,
		},
		"bad args 8": {
			input: `json(false)`,
			err:   `char 0: expected string param, received bool`,
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
