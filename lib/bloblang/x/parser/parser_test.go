package parser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestInRange(t *testing.T) {
	parser := InRange('a', 'c')

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
			err:       ExpectedError{"range(a - c)"},
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       ExpectedError{"range(a - c)"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestAnyOfErrors(t *testing.T) {
	tests := map[string]struct {
		resultErrs []error
		err        string
	}{
		"One parser fails": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
			},
			err: `char 5: expected: foo`,
		},
		"Two parsers fail": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"bar"},
				},
			},
			err: `char 5: expected one of: [foo bar]`,
		},
		"Two parsers fail 2": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"bar"},
				},
				ExpectedError{"foo"},
			},
			err: `char 5: expected: bar`,
		},
		"Two parsers fail 3": {
			resultErrs: []error{
				ExpectedError{"foo"},
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"bar"},
				},
			},
			err: `char 5: expected: bar`,
		},
		"Two parsers fail 4": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				PositionalError{
					Position: 8,
					Err:      ExpectedError{"bar"},
				},
			},
			err: `char 8: expected: bar`,
		},
		"Two parsers fail 5": {
			resultErrs: []error{
				PositionalError{
					Position: 8,
					Err:      ExpectedError{"bar"},
				},
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
			},
			err: `char 8: expected: bar`,
		},
		"Fatal parsers fail": {
			resultErrs: []error{
				errors.New("this is a real error"),
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
			},
			err: `this is a real error`,
		},
		"Fatal parsers fail 2": {
			resultErrs: []error{
				PositionalError{
					Position: 0,
					Err:      errors.New("this is a real error"),
				},
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
			},
			err: `char 0: this is a real error`,
		},
		"Fatal parsers fail 3": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				errors.New("this is a real error"),
			},
			err: `this is a real error`,
		},
		"Fatal parsers fail 4": {
			resultErrs: []error{
				PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				PositionalError{
					Position: 0,
					Err:      errors.New("this is a real error"),
				},
			},
			err: `char 0: this is a real error`,
		},
	}

	for name, test := range tests {
		childParsers := []Type{}
		for _, err := range test.resultErrs {
			err := err
			childParsers = append(childParsers, func([]rune) Result {
				return Result{
					Err: err,
				}
			})
		}
		t.Run(name, func(t *testing.T) {
			res := AnyOf(childParsers...)([]rune("foobar"))
			assert.Equal(t, test.err, res.Err.Error(), "Error")
		})
	}
}

func TestBestMatch(t *testing.T) {
	tests := map[string]struct {
		inputResults []Result
		result       Result
	}{
		"Three parsers fail": {
			inputResults: []Result{
				{
					Err: PositionalError{
						Position: 5,
						Err:      ExpectedError{"foo"},
					},
					Remaining: []rune("foobar"),
				},
				{
					Err: PositionalError{
						Position: 3,
						Err:      ExpectedError{"bar"},
					},
					Remaining: []rune("foobar"),
				},
				{
					Err: PositionalError{
						Position: 4,
						Err:      ExpectedError{"bar"},
					},
					Remaining: []rune("foobar"),
				},
			},
			result: Result{
				Err: PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				Remaining: []rune("foobar"),
			},
		},
		"One parser succeeds": {
			inputResults: []Result{
				{
					Err: PositionalError{
						Position: 5,
						Err:      ExpectedError{"foo"},
					},
					Remaining: []rune("foobar"),
				},
				{
					Result:    "test",
					Remaining: []rune("bar"),
				},
			},
			result: Result{
				Err: PositionalError{
					Position: 5,
					Err:      ExpectedError{"foo"},
				},
				Remaining: []rune("foobar"),
			},
		},
		"One parser succeeds 2": {
			inputResults: []Result{
				{
					Err: PositionalError{
						Position: 4,
						Err:      ExpectedError{"foo"},
					},
					Remaining: []rune("foobar"),
				},
				{
					Result:    "test",
					Remaining: []rune("r"),
				},
			},
			result: Result{
				Result:    "test",
				Remaining: []rune("r"),
			},
		},
		"Three parsers fail one severe": {
			inputResults: []Result{
				{
					Err: PositionalError{
						Position: 5,
						Err:      ExpectedError{"foo"},
					},
					Remaining: []rune("foobar"),
				},
				{
					Err:       errors.New("this is a real error"),
					Remaining: []rune("foobar"),
				},
				{
					Err: PositionalError{
						Position: 4,
						Err:      ExpectedError{"bar"},
					},
					Remaining: []rune("foobar"),
				},
			},
			result: Result{
				Err:       errors.New("this is a real error"),
				Remaining: []rune("foobar"),
			},
		},
	}

	for name, test := range tests {
		childParsers := []Type{}
		for _, res := range test.inputResults {
			res := res
			childParsers = append(childParsers, func([]rune) Result {
				return res
			})
		}
		t.Run(name, func(t *testing.T) {
			res := BestMatch(childParsers...)([]rune("foobar"))
			assert.Equal(t, test.result, res)
		})
	}
}

func TestSnakeCase(t *testing.T) {
	parser := SnakeCase()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       string
	}{
		"empty input": {
			err: `char 0: unexpected end of input`,
		},
		"only a char": {
			input:     "a",
			result:    "a",
			remaining: "",
		},
		"lots of the set": {
			input:     "a1c_a2c",
			remaining: "",
			result:    "a1c_a2c",
		},
		"lots of the set and some": {
			input:     "abc_abc and this",
			remaining: " and this",
			result:    "abc_abc",
		},
		"not in the set": {
			input:     "N",
			remaining: "N",
			err:       `char 0: expected one of: [range(a - z) range(0 - 9) _]`,
		},
		"lots not in the set": {
			input:     "NONONONO",
			remaining: "NONONONO",
			err:       `char 0: expected one of: [range(a - z) range(0 - 9) _]`,
		},
		"prefix underscore": {
			input:     "_foobar baz",
			remaining: "_foobar baz",
			err:       `char 0: unexpected prefixed underscore`,
		},
		"suffix underscore": {
			input:     "foo_bar_ baz",
			remaining: "foo_bar_ baz",
			err:       `char 8: unexpected suffixed underscore`,
		},
		"double underscore": {
			input:     "foo_bar__baz",
			remaining: "foo_bar__baz",
			err:       `char 8: unexpected double underscore`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			if res.Err != nil || len(test.err) > 0 {
				assert.Equal(t, test.err, res.Err.Error(), "Error")
			}
			assert.Equal(t, test.result, res.Result, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestSequence(t *testing.T) {
	parser := Sequence(Match("abc"), Match("def"))

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: ErrAtPosition(0, errors.New("unexpected end of input")),
		},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
			err:       ErrAtPosition(0, ExpectedError{"abc"}),
		},
		"matches first": {
			input:     "abcNo",
			remaining: "abcNo",
			err:       ErrAtPosition(3, ExpectedError{"def"}),
		},
		"matches some of second": {
			input:     "abcdeNo",
			remaining: "abcdeNo",
			err:       ErrAtPosition(3, ExpectedError{"def"}),
		},
		"matches all": {
			input:     "abcdef",
			remaining: "",
			result:    []interface{}{"abc", "def"},
		},
		"matches some": {
			input:     "abcdef and this",
			remaining: " and this",
			result:    []interface{}{"abc", "def"},
		},
		"matches only one": {
			input:     "abcdefabcdef",
			remaining: "abcdef",
			result:    []interface{}{"abc", "def"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			_ = assert.Equal(t, test.err, res.Err, "Error") &&
				assert.Equal(t, test.result, res.Result, "Result") &&
				assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestAllOf(t *testing.T) {
	parser := AllOf(Match("abc"))

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
		"matches first": {
			input:     "abcNo",
			remaining: "No",
			result:    []interface{}{"abc"},
		},
		"matches some of second": {
			input:     "abcabNo",
			remaining: "abNo",
			result:    []interface{}{"abc"},
		},
		"matches all": {
			input:     "abcabc",
			remaining: "",
			result:    []interface{}{"abc", "abc"},
		},
		"matches some": {
			input:     "abcabc and this",
			remaining: " and this",
			result:    []interface{}{"abc", "abc"},
		},
		"matches all of these": {
			input:     "abcabcabcabcdef and this",
			remaining: "def and this",
			result:    []interface{}{"abc", "abc", "abc", "abc"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
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

func TestDiscard(t *testing.T) {
	parser := Discard(Match("abc"))

	tests := map[string]struct {
		input     string
		remaining string
	}{
		"empty input": {},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
		},
		"only string": {
			input:     "abc",
			remaining: "",
		},
		"lots of the string": {
			input:     "abcabc",
			remaining: "abc",
		},
		"lots of the string and some": {
			input:     "abcabc and this",
			remaining: "abc and this",
		},
		"not in the string": {
			input:     "n",
			remaining: "n",
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			require.NoError(t, res.Err)
			assert.Nil(t, res.Result)
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestDiscardAll(t *testing.T) {
	parser := DiscardAll(Match("abc"))

	tests := map[string]struct {
		input     string
		remaining string
	}{
		"empty input": {},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
		},
		"only string": {
			input:     "abc",
			remaining: "",
		},
		"lots of the string": {
			input:     "abcabc",
			remaining: "",
		},
		"lots of the string broken": {
			input:     "abcabc abc and this",
			remaining: " abc and this",
		},
		"lots of the string and some": {
			input:     "abcabc and this",
			remaining: " and this",
		},
		"not in the string": {
			input:     "n",
			remaining: "n",
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			require.NoError(t, res.Err)
			assert.Nil(t, res.Result)
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
		"float number 3": {
			input:     "1.23.0 notthis",
			result:    float64(1.23),
			remaining: ".0 notthis",
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

func TestArray(t *testing.T) {
	p := LiteralValue()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"empty array": {
			input:     "[] and this",
			result:    []interface{}(nil),
			remaining: " and this",
		},
		"empty array with whitespace": {
			input:     "[ \t] and this",
			result:    []interface{}(nil),
			remaining: " and this",
		},
		"single element array": {
			input:     `[ "foo" ] and this`,
			result:    []interface{}{"foo"},
			remaining: " and this",
		},
		"tailing comma array": {
			input:     `[ "foo", ] and this`,
			err:       ErrAtPosition(9, ExpectedError{"boolean", "number", "quoted-string", "null", "array", "object"}),
			remaining: `[ "foo", ] and this`,
		},
		"random stuff array": {
			input:     `[ "foo", whats this ] and this`,
			err:       ErrAtPosition(9, ExpectedError{"boolean", "number", "quoted-string", "null", "array", "object"}),
			remaining: `[ "foo", whats this ] and this`,
		},
		"random stuff array 2": {
			input:     `[ "foo" whats this ] and this`,
			err:       ErrAtPosition(8, ExpectedError{","}),
			remaining: `[ "foo" whats this ] and this`,
		},
		"multiple elements array": {
			input:     `[ "foo", false,5.2] and this`,
			result:    []interface{}{"foo", false, float64(5.2)},
			remaining: " and this",
		},
		"multiple elements array line broken": {
			input: `[
	"foo",
	null,
	"bar",
	[true,false]
] and this`,
			result:    []interface{}{"foo", nil, "bar", []interface{}{true, false}},
			remaining: " and this",
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

func TestObject(t *testing.T) {
	p := LiteralValue()

	tests := map[string]struct {
		input     string
		result    interface{}
		remaining string
		err       error
	}{
		"empty input": {
			err: errors.New("unexpected end of input"),
		},
		"empty object": {
			input:     "{} and this",
			result:    map[string]interface{}{},
			remaining: " and this",
		},
		"empty object with whitespace": {
			input:     "{ \t} and this",
			result:    map[string]interface{}{},
			remaining: " and this",
		},
		"single value object": {
			input:     `{"foo":"bar"} and this`,
			result:    map[string]interface{}{"foo": "bar"},
			remaining: " and this",
		},
		"tailing comma object": {
			input:     `{ "foo": } and this`,
			err:       ErrAtPosition(9, ExpectedError{"boolean", "number", "quoted-string", "null", "array", "object"}),
			remaining: `{ "foo": } and this`,
		},
		"random stuff object": {
			input:     `{ "foo": whats this } and this`,
			err:       ErrAtPosition(9, ExpectedError{"boolean", "number", "quoted-string", "null", "array", "object"}),
			remaining: `{ "foo": whats this } and this`,
		},
		"multiple values random stuff object": {
			input:     `{ "foo":true "bar":5.2 } and this`,
			err:       ErrAtPosition(13, ExpectedError{","}),
			remaining: `{ "foo":true "bar":5.2 } and this`,
		},
		"multiple values object": {
			input:     `{ "foo":true, "bar":5.2 } and this`,
			result:    map[string]interface{}{"foo": true, "bar": 5.2},
			remaining: " and this",
		},
		"multiple values object line broken": {
			input: `{
	"foo":2  ,
	"bar":     null,
	"baz":
		"three",
	"quz":   [true,false]
} and this`,
			result: map[string]interface{}{
				"foo": int64(2),
				"bar": nil,
				"baz": "three",
				"quz": []interface{}{true, false},
			},
			remaining: " and this",
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
			err:       ExpectedError{"line-break"},
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
			err:       ExpectedError{"line-break"},
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
