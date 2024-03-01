package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDotEnv(t *testing.T) {
	parser := dotEnvParser

	tests := map[string]struct {
		input     string
		result    map[string]string
		remaining string
		err       *Error
	}{
		"empty input": {
			result: map[string]string{},
		},
		"empty lines": {
			input:  "\n  \n  \t\n\n #foo bar   \n\n",
			result: map[string]string{},
		},
		"bad assignment": {
			input:     "not a thing",
			err:       NewError([]rune("a thing"), "Environment variable assignment"),
			remaining: "not a thing",
		},
		"single line": {
			input:  `FOO=bar`,
			result: map[string]string{"FOO": "bar"},
		},
		"single assignment with empty lines": {
			input:  "\n \nFOO=bar\n  \n ",
			result: map[string]string{"FOO": "bar"},
		},
		"multiple assignments": {
			input: "FOO=bar\nBAZ=buz # Cool\n\nBEV=qux",
			result: map[string]string{
				"FOO": "bar",
				"BAZ": "buz",
				"BEV": "qux",
			},
		},
		"multiple assignments with quotes": {
			input: "FOO=bar\n# Meow meow\nBAZ=\"buz\" # Cool\n\nBEV=qux",
			result: map[string]string{
				"FOO": "bar",
				"BAZ": "buz",
				"BEV": "qux",
			},
		},
		"multiple assignments gibberish at the end": {
			input:     "FOO=bar\nBAZ=buz\n\nBEV=qux\nand some stuff here",
			err:       NewError([]rune("some stuff here"), "Environment variable assignment"),
			remaining: "FOO=bar\nBAZ=buz\n\nBEV=qux\nand some stuff here",
		},
		"multiple assignments with spaces": {
			input: `
FIRST = foo

SECOND= "bar"

THIRD =b"az

FOURTH   =    buz

FIFTH =  

`,
			result: map[string]string{
				"FIRST":  "foo",
				"SECOND": "bar",
				"THIRD":  "b\"az",
				"FOURTH": "buz",
				"FIFTH":  "",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}
