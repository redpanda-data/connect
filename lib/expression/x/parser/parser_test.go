package parser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNotEnd(t *testing.T) {
	errInner := errors.New("test err")
	notEnd := NotEnd(func([]rune) Result {
		return Result{Err: errInner}
	})
	assert.Equal(t, errInner, notEnd([]rune("foo")).Err)
	assert.Equal(t, "unexpected end of input", notEnd([]rune("")).Err.Error())
}

func TestChar(t *testing.T) {
	char := Char('x')

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only the char": {
			input:  "x",
			result: "x",
		},
		"lots of the char": {
			input:     "xxxx",
			result:    "x",
			remaining: "xxx",
		},
		"wrong input": {
			input:     "not x",
			remaining: "not x",
			err:       ExpectedError{"x"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := char([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNotChar(t *testing.T) {
	char := NotChar('x')

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only the char": {
			input:     "x",
			remaining: "x",
			err:       ExpectedError{"not x"},
		},
		"lots of the char": {
			input:     "xxxx",
			remaining: "xxxx",
			err:       ExpectedError{"not x"},
		},
		"only not the char": {
			input:     "n",
			result:    "n",
			remaining: "",
		},
		"not the char": {
			input:     "not x",
			result:    "not ",
			remaining: "x",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := char([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestInSet(t *testing.T) {
	inSet := InSet('a', 'b', 'c')

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only a char": {
			input:     "a",
			result:    "a",
			remaining: "",
		},
		"lots of the set": {
			input:     "abcabc",
			remaining: "",
			result:    "abcabc",
		},
		"lots of the set and some": {
			input:     "abcabc and this",
			remaining: " and this",
			result:    "abcabc",
		},
		"not in the set": {
			input:     "n",
			remaining: "n",
			err:       ExpectedError{"chars(abc)"},
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"chars(abc)"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestMatch(t *testing.T) {
	str := Match("abc")

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
			err:       ExpectedError{"abc"},
		},
		"only string": {
			input:     "abc",
			result:    "abc",
			remaining: "",
		},
		"lots of the string": {
			input:     "abcabc",
			remaining: "abc",
			result:    "abc",
		},
		"lots of the string and some": {
			input:     "abcabc and this",
			remaining: "abc and this",
			result:    "abc",
		},
		"not in the string": {
			input:     "n",
			remaining: "n",
			err:       ExpectedError{"abc"},
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"abc"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := str([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNumber(t *testing.T) {
	p := Number()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"just digits": {
			input:     "123",
			result:    int64(123),
			remaining: "",
		},
		"digits plus": {
			input:     "123 foo",
			result:    int64(123),
			remaining: " foo",
		},
		"float number": {
			input:     "0.123",
			result:    float64(0.123),
			remaining: "",
		},
		"float number 2": {
			input:     "0.123 foo",
			result:    float64(0.123),
			remaining: " foo",
		},
		"not a number": {
			input:     "hello",
			remaining: "hello",
			err:       ExpectedError{"number"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := p([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestBoolean(t *testing.T) {
	p := Boolean()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"just bool": {
			input:     "true",
			result:    true,
			remaining: "",
		},
		"just bool 2": {
			input:     "false",
			result:    false,
			remaining: "",
		},
		"not a bool": {
			input:     "hello",
			remaining: "hello",
			err:       ExpectedError{"boolean"},
		},
		"bool and stuff": {
			input:     "false foo",
			result:    false,
			remaining: " foo",
		},
		"bool and stuff 2": {
			input:     "true foo",
			result:    true,
			remaining: " foo",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := p([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestQuotedString(t *testing.T) {
	str := QuotedString()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only quote": {
			input:     `"foo"`,
			result:    "foo",
			remaining: "",
		},
		"quote plus extra": {
			input:     `"foo" and this`,
			result:    "foo",
			remaining: " and this",
		},
		"quote with escapes": {
			input:     `"foo\u263abar" and this`,
			result:    "foo☺bar",
			remaining: " and this",
		},
		"quote with escaped quotes": {
			input:     `"foo\"bar\"baz" and this`,
			result:    `foo"bar"baz`,
			remaining: " and this",
		},
		"quote with escaped quotes 2": {
			input:     `"foo\\\"bar\\\"baz" and this`,
			result:    `foo\"bar\"baz`,
			remaining: " and this",
		},
		"not quoted": {
			input:     `foo`,
			remaining: "foo",
			err:       ExpectedError{"quoted-string"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := str([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestSpacesAndTabs(t *testing.T) {
	inSet := SpacesAndTabs()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only a char": {
			input:     " ",
			result:    " ",
			remaining: "",
		},
		"lots of the set": {
			input:     " \t ",
			remaining: "",
			result:    " \t ",
		},
		"lots of the set and some": {
			input:     "  \t\t and this",
			remaining: "and this",
			result:    "  \t\t ",
		},
		"not in the set": {
			input:     "n",
			remaining: "n",
			err:       ExpectedError{"whitespace"},
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"whitespace"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNewline(t *testing.T) {
	inSet := Newline()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only a line feed": {
			input:     "\n",
			result:    "\n",
			remaining: "",
		},
		"only a carriage return": {
			input:     "\r",
			remaining: "\r",
			err:       ExpectedError{"line break"},
		},
		"a carriage return line feed": {
			input:     "\r\n",
			result:    "\r\n",
			remaining: "",
		},
		"a carriage return line feed plus": {
			input:     "\r\n foo",
			result:    "\r\n",
			remaining: " foo",
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"line break"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestAnyOf(t *testing.T) {
	anyOf := AnyOf(
		Char('a'),
		Char('b'),
		Char('c'),
	)

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"only a char": {
			input:     "a",
			result:    "a",
			remaining: "",
		},
		"lots of the set": {
			input:     "abcabc",
			remaining: "bcabc",
			result:    "a",
		},
		"lots of the set 2": {
			input:     "bcabc",
			remaining: "cabc",
			result:    "b",
		},
		"lots of the set 3": {
			input:     "cabc",
			remaining: "abc",
			result:    "c",
		},
		"lots of the set and some": {
			input:     "a and this",
			remaining: " and this",
			result:    "a",
		},
		"lots of the set and some 2": {
			input:     "b and this",
			remaining: " and this",
			result:    "b",
		},
		"lots of the set and some 3": {
			input:     "c and this",
			remaining: " and this",
			result:    "c",
		},
		"not in the set": {
			input:     "n",
			remaining: "n",
			err:       ExpectedError{"a", "b", "c"},
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"a", "b", "c"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := anyOf([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}
