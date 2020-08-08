package parser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrorStrings(t *testing.T) {
	tests := []struct {
		err *Error
		exp string
	}{
		{
			err: NewError([]rune("input data"), "foo", "bar", "baz"),
			exp: `expected foo, bar, or baz, got: input`,
		},
		{
			err: NewError([]rune("input data"), "foo", "bar", "foo", "baz"),
			exp: `expected foo, bar, or baz, got: input`,
		},
		{
			err: NewError(nil, "foo", "bar", "baz"),
			exp: `expected foo, bar, or baz, but reached end of input`,
		},
		{
			err: NewError(nil),
			exp: `encountered unexpected end of input`,
		},
		{
			err: NewError([]rune("?!"), "foo", "bar", "baz"),
			exp: `expected foo, bar, or baz, got: ?!`,
		},
		{
			err: NewError([]rune("nope")),
			exp: `encountered unexpected input: nope`,
		},
		{
			err: NewError([]rune("input data"), "foo", "bar"),
			exp: `expected foo or bar, got: input`,
		},
		{
			err: NewError([]rune("input data"), "foo"),
			exp: `expected foo, got: input`,
		},
		{
			err: NewFatalError([]rune("input data"), errors.New("nope"), "foo", "bar", "baz"),
			exp: `nope: expected foo, bar, or baz, got: input`,
		},
		{
			err: NewFatalError([]rune("nope"), errors.New("oh no")),
			exp: `oh no: nope`,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.exp, test.err.Error())
	}
}

func TestErrorPositionalStrings(t *testing.T) {
	tests := []struct {
		input string
		err   *Error
		exp   string
	}{
		{
			input: `hello world input data`,
			err:   NewError([]rune("input data"), "foo", "bar", "baz"),
			exp:   `line 1 char 13: expected foo, bar, or baz`,
		},
		{
			input: `hello world input data`,
			err:   NewError(nil, "foo", "bar", "baz"),
			exp:   `line 1 char 23: expected foo, bar, or baz`,
		},
		{
			input: `hello world input data`,
			err:   NewError(nil),
			exp:   `line 1 char 23: encountered unexpected end of input`,
		},
		{
			input: "hello \nworld \ninput data",
			err:   NewError([]rune("input data"), "foo", "bar", "baz"),
			exp:   `line 3 char 1: expected foo, bar, or baz`,
		},
		{
			input: "hello \nworld \ni",
			err:   NewError([]rune("i"), "foo", "bar", "baz"),
			exp:   `line 3 char 1: expected foo, bar, or baz`,
		},
		{
			input: "hello \nworld \n input data\n foo",
			err:   NewError([]rune("input data\n foo"), "foo", "bar", "baz"),
			exp:   `line 3 char 2: expected foo, bar, or baz`,
		},
		{
			input: "hello \nworld \n i",
			err:   NewError([]rune("i"), "foo", "bar", "baz"),
			exp:   `line 3 char 2: expected foo, bar, or baz`,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.exp, test.err.ErrorAtPosition([]rune(test.input)))
	}
}

func TestErrorPositionalStringsStructured(t *testing.T) {
	tests := []struct {
		input string
		err   *Error
		exp   string
	}{
		{
			input: `hello world input data`,
			err:   NewError([]rune("input data"), "foo", "bar", "baz"),
			exp: `line 1 char 13: expected foo, bar, or baz
  |
1 | hello world input data
  |             ^---`,
		},
		{
			input: `hello world input data`,
			err:   NewError(nil, "foo", "bar", "baz"),
			exp: `line 1 char 23: expected foo, bar, or baz
  |
1 | hello world input data
  |                       ^---`,
		},
		{
			input: `hello world input data`,
			err:   NewError(nil),
			exp: `line 1 char 23: encountered unexpected end of input
  |
1 | hello world input data
  |                       ^---`,
		},
		{
			input: `hello world this is a really long input string input data`,
			err:   NewError([]rune("input data"), "foo", "bar", "baz"),
			exp: `line 1 char 48: expected foo, bar, or baz
  |
1 | hello world this is a really long input string input data
  |                                                ^---`,
		},
		{
			input: `hello world this is a really long input string input data with too much information that we dont want to entirely print`,
			err:   NewError([]rune("this is a really long input string input data with too much information that we dont want to entirely print"), "foo", "bar", "baz"),
			exp: `line 1 char 13: expected foo, bar, or baz
  |
1 | hello world this is a really long input string input data with too much 
  |             ^---`,
		},
		{
			input: "hello world\nthis is a really\nlong input string input data",
			err:   NewError([]rune("input data"), "foo", "bar", "baz"),
			exp: `line 3 char 19: expected foo, bar, or baz
  |
3 | long input string input data
  |                   ^---`,
		},
		{
			input: "hello world\n\nthis is a really\nlong input string input\n\ndata",
			err:   NewError([]rune("input\n\ndata"), "foo", "bar", "baz"),
			exp: `line 4 char 19: expected foo, bar, or baz
  |
4 | long input string input
  |                   ^---`,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.exp, test.err.ErrorAtPositionStructured("", []rune(test.input)))
	}
}
