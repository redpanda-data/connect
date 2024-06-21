package parser

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChar(t *testing.T) {
	char := Char('x')

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "x"),
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
			err:       NewError([]rune("not x"), "x"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := char([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNotChar(t *testing.T) {
	char := NotChar('x')

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "not x"),
		},
		"only the char": {
			input:     "x",
			remaining: "x",
			err:       NewError([]rune("x"), "not x"),
		},
		"lots of the char": {
			input:     "xxxx",
			remaining: "xxxx",
			err:       NewError([]rune("xxxx"), "not x"),
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
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestInSet(t *testing.T) {
	inSet := InSet('a', 'b', 'c')

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "chars(abc)"),
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
			err:       NewError([]rune("n"), "chars(abc)"),
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "chars(abc)"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNotInSet(t *testing.T) {
	inSet := NotInSet('#', '\n', ' ')

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "not chars(#\n )"),
		},
		"only a char": {
			input:     "#",
			remaining: "#",
			err:       NewError([]rune("#"), "not chars(#\n )"),
		},
		"some text then set": {
			input:     "abcabc#foo",
			remaining: "#foo",
			result:    "abcabc",
		},
		"some text then two from set": {
			input:     "abcabc#\nfoo",
			remaining: "#\nfoo",
			result:    "abcabc",
		},
		"only text not in set": {
			input:     "abcabc",
			remaining: "",
			result:    "abcabc",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestEmptyLine(t *testing.T) {
	parser := EmptyLine

	tests := map[string]struct {
		input     string
		result    any
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "Empty line"),
		},
		"empty line": {
			input:     "\n",
			result:    nil,
			remaining: "\n",
		},
		"empty line with extra": {
			input:     "\n foo",
			result:    nil,
			remaining: "\n foo",
		},
		"non-empty line": {
			input:     "foo\n",
			err:       NewError([]rune("foo\n"), "Empty line"),
			remaining: "foo\n",
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

func TestInRange(t *testing.T) {
	parser := InRange('a', 'c')

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "range(a - c)"),
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
			err:       NewError([]rune("n"), "range(a - c)"),
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "range(a - c)"),
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

func TestOneOfErrors(t *testing.T) {
	input := "foo bar baz"
	tests := map[string]struct {
		resultErrs []*Error
		err        string
	}{
		"One parser fails": {
			resultErrs: []*Error{
				NewError([]rune("bar baz"), "foo"),
			},
			err: `line 1 char 5: expected foo`,
		},
		"Two parsers fail": {
			resultErrs: []*Error{
				NewError([]rune("bar baz"), "foo"),
				NewError([]rune("bar baz"), "bar"),
			},
			err: `line 1 char 5: expected foo or bar`,
		},
		"Two parsers fail 2": {
			resultErrs: []*Error{
				NewError([]rune("bar baz"), "bar"),
				NewError([]rune("o bar baz"), "foo"),
			},
			err: `line 1 char 5: expected bar`,
		},
		"Two parsers fail 3": {
			resultErrs: []*Error{
				NewError([]rune("o bar baz"), "foo"),
				NewError([]rune("bar baz"), "bar"),
			},
			err: `line 1 char 5: expected bar`,
		},
		"Fatal parsers fail": {
			resultErrs: []*Error{
				NewFatalError([]rune("bar baz"), errors.New("this is a real error")),
				NewError([]rune("bar baz"), "bar"),
			},
			err: `line 1 char 5: this is a real error`,
		},
		"Fatal parsers fail 2": {
			resultErrs: []*Error{
				NewFatalError([]rune("bar baz"), errors.New("this is a real error")),
				NewError([]rune("baz"), "bar"),
			},
			err: `line 1 char 5: this is a real error`,
		},
		"Fatal parsers fail 3": {
			resultErrs: []*Error{
				NewError([]rune("bar baz"), "bar"),
				NewFatalError([]rune("bar baz"), errors.New("this is a real error")),
			},
			err: `line 1 char 5: this is a real error`,
		},
	}

	for name, test := range tests {
		childParsers := []Func[any]{}
		for _, err := range test.resultErrs {
			err := err
			childParsers = append(childParsers, func([]rune) Result[any] {
				return Result[any]{
					Err: err,
				}
			})
		}
		t.Run(name, func(t *testing.T) {
			res := OneOf(childParsers...)([]rune(input))
			assert.Equal(t, test.err, res.Err.ErrorAtPosition([]rune(input)), "Error")
		})
	}
}

func TestBestMatch(t *testing.T) {
	tests := map[string]struct {
		inputResults []Result[any]
		result       Result[any]
	}{
		"Three parsers fail": {
			inputResults: []Result[any]{
				{
					Err:       NewError([]rune("ar"), "foo"),
					Remaining: []rune("foobar"),
				},
				{
					Err:       NewError([]rune("obar"), "bar"),
					Remaining: []rune("foobar"),
				},
				{
					Err:       NewError([]rune("bar"), "bar"),
					Remaining: []rune("foobar"),
				},
			},
			result: Result[any]{
				Err:       NewError([]rune("ar"), "foo"),
				Remaining: []rune("foobar"),
			},
		},
		"One parser succeeds": {
			inputResults: []Result[any]{
				{
					Err:       NewError([]rune("ar"), "foo"),
					Remaining: []rune("foobar"),
				},
				{
					Payload:   "test",
					Remaining: []rune("bar"),
				},
			},
			result: Result[any]{
				Err:       NewError([]rune("ar"), "foo"),
				Remaining: []rune("foobar"),
			},
		},
		"One parser succeeds 2": {
			inputResults: []Result[any]{
				{
					Err:       NewError([]rune("ar"), "foo"),
					Remaining: []rune("foobar"),
				},
				{
					Payload:   "test",
					Remaining: []rune("r"),
				},
			},
			result: Result[any]{
				Payload:   "test",
				Remaining: []rune("r"),
			},
		},
		"Three parsers fail one severe": {
			inputResults: []Result[any]{
				{
					Err:       NewError([]rune("ar"), "foo"),
					Remaining: []rune("foobar"),
				},
				{
					Err:       NewFatalError([]rune("r"), errors.New("this is a real error")),
					Remaining: []rune("foobar"),
				},
				{
					Err:       NewError([]rune("bar"), "bar"),
					Remaining: []rune("foobar"),
				},
			},
			result: Result[any]{
				Err:       NewFatalError([]rune("r"), errors.New("this is a real error")),
				Remaining: []rune("foobar"),
			},
		},
	}

	for name, test := range tests {
		childParsers := []Func[any]{}
		for _, res := range test.inputResults {
			res := res
			childParsers = append(childParsers, func([]rune) Result[any] {
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
	parser := SnakeCase

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       string
	}{
		"empty input": {
			err: `line 1 char 1: expected snake-case`,
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
			err:       `line 1 char 1: expected snake-case`,
		},
		"lots not in the set": {
			input:     "NONONONO",
			remaining: "NONONONO",
			err:       `line 1 char 1: expected snake-case`,
		},
		"prefix underscore": {
			input:     "_foobar baz",
			remaining: " baz",
			result:    "_foobar",
		},
		"suffix underscore": {
			input:     "foo_bar_ baz",
			remaining: " baz",
			result:    "foo_bar_",
		},
		"double underscore": {
			input:     "foo_bar__baz",
			remaining: "",
			result:    "foo_bar__baz",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			if res.Err != nil || test.err != "" {
				assert.Equal(t, test.err, res.Err.ErrorAtPosition([]rune(test.input)), "Error")
			}
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestTerm(t *testing.T) {
	parser := Term("a🥰c")

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "a🥰c"),
		},
		"smaller than string": {
			input:     "a🥰",
			remaining: "a🥰",
			err:       NewError([]rune("a🥰"), "a🥰c"),
		},
		"matches first": {
			input:     "a🥰cNo",
			result:    "a🥰c",
			remaining: "No",
		},
		"matches all": {
			input:     "a🥰c",
			remaining: "",
			result:    "a🥰c",
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

func TestUntilTerm(t *testing.T) {
	tests := map[string]struct {
		parser    Func[string]
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			parser: UntilTerm("abc"),
			err:    NewError([]rune(""), "abc"),
		},
		"smaller than string": {
			parser:    UntilTerm("abc"),
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches first": {
			parser:    UntilTerm("abc"),
			input:     "abcNo",
			result:    "",
			remaining: "abcNo",
		},
		"matches all": {
			parser:    UntilTerm("abc"),
			input:     "abc",
			remaining: "abc",
			result:    "",
		},
		"matches end": {
			parser:    UntilTerm("abc"),
			input:     "hello world abc",
			remaining: "abc",
			result:    "hello world ",
		},
		"matches before end": {
			parser:    UntilTerm("abc"),
			input:     "hello world abc this is ash",
			remaining: "abc this is ash",
			result:    "hello world ",
		},
		"single char term matches all": {
			parser:    UntilTerm("a"),
			input:     "a",
			remaining: "a",
			result:    "",
		},
		"single char term matches end": {
			parser:    UntilTerm("a"),
			input:     "helloa",
			remaining: "a",
			result:    "hello",
		},
		"single char term matches before end": {
			parser:    UntilTerm("a"),
			input:     "helloabar",
			remaining: "abar",
			result:    "hello",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := test.parser([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestSequence(t *testing.T) {
	parser := Sequence(Term("abc"), Term("def"))

	tests := map[string]struct {
		input     string
		result    []string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "abc"),
		},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches first": {
			input:     "abcNo",
			remaining: "abcNo",
			err:       NewError([]rune("No"), "def"),
		},
		"matches some of second": {
			input:     "abcdeNo",
			remaining: "abcdeNo",
			err:       NewError([]rune("deNo"), "def"),
		},
		"matches all": {
			input:     "abcdef",
			remaining: "",
			result:    []string{"abc", "def"},
		},
		"matches some": {
			input:     "abcdef and this",
			remaining: " and this",
			result:    []string{"abc", "def"},
		},
		"matches only one": {
			input:     "abcdefabcdef",
			remaining: "abcdef",
			result:    []string{"abc", "def"},
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

func TestAllOf(t *testing.T) {
	parser := UntilFail(Term("abc"))

	tests := map[string]struct {
		input     string
		result    []string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "abc"),
		},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches first": {
			input:     "abcNo",
			remaining: "No",
			result:    []string{"abc"},
		},
		"matches some of second": {
			input:     "abcabNo",
			remaining: "abNo",
			result:    []string{"abc"},
		},
		"matches all": {
			input:     "abcabc",
			remaining: "",
			result:    []string{"abc", "abc"},
		},
		"matches some": {
			input:     "abcabc and this",
			remaining: " and this",
			result:    []string{"abc", "abc"},
		},
		"matches all of these": {
			input:     "abcabcabcabcdef and this",
			remaining: "def and this",
			result:    []string{"abc", "abc", "abc", "abc"},
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

func TestDelimited(t *testing.T) {
	parser := Delimited(Term("abc"), charHash)

	tests := map[string]struct {
		input     string
		result    DelimitedResult[string, string]
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "abc"),
		},
		"no first primary": {
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches first": {
			input:     "abc#",
			remaining: "abc#",
			err:       NewError([]rune(""), "abc"),
		},
		"matches some of second": {
			input:     "abc#ab",
			remaining: "abc#ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches all": {
			input:     "abc#abc",
			remaining: "",
			result: DelimitedResult[string, string]{
				Primary:   []string{"abc", "abc"},
				Delimiter: []string{"#"},
			},
		},
		"matches some": {
			input:     "abc#abc and this",
			remaining: " and this",
			result: DelimitedResult[string, string]{
				Primary:   []string{"abc", "abc"},
				Delimiter: []string{"#"},
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

func TestDelimitedPatternAllowTrailing(t *testing.T) {
	parser := DelimitedPattern(charHash, Term("abc"), charComma, Char('!'))

	tests := map[string]struct {
		input     string
		result    []string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "#"),
		},
		"no start": {
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "#"),
		},
		"smaller than string": {
			input:     "#ab",
			remaining: "#ab",
			err:       NewError([]rune("ab"), "abc"),
		},
		"matches first": {
			input:     "#abc!No",
			remaining: "No",
			result:    []string{"abc"},
		},
		"matches first trailing": {
			input:     "#abc,!No",
			remaining: "No",
			result:    []string{"abc"},
		},
		"matches some of second": {
			input:     "#abc,abNo",
			remaining: "#abc,abNo",
			err:       NewError([]rune("abNo"), "abc"),
		},
		"matches not stopped": {
			input:     "#abc,abcNo",
			remaining: "#abc,abcNo",
			err:       NewError([]rune("No"), ",", "!"),
		},
		"matches all": {
			input:     "#abc,abc!",
			remaining: "",
			result:    []string{"abc", "abc"},
		},
		"matches all trailing": {
			input:     "#abc,abc,!",
			remaining: "",
			result:    []string{"abc", "abc"},
		},
		"matches some": {
			input:     "#abc,abc! and this",
			remaining: " and this",
			result:    []string{"abc", "abc"},
		},
		"matches all of these": {
			input:     "#abc,abc,abc,abc!def and this",
			remaining: "def and this",
			result:    []string{"abc", "abc", "abc", "abc"},
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

func TestMustBe(t *testing.T) {
	tests := map[string]struct {
		inputRes  Result[any]
		outputRes Result[any]
	}{
		"No error": {
			inputRes: Result[any]{
				Payload:   "foo",
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Payload:   "foo",
				Remaining: []rune("foobar"),
			},
		},
		"Real error": {
			inputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
		},
		"Expected error": {
			inputRes: Result[any]{
				Err:       NewError(nil, "testerr"),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("required"), "testerr"),
				Remaining: []rune("foobar"),
			},
		},
		"Expected already fatal error": {
			inputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr"), "testerr"),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr"), "testerr"),
				Remaining: []rune("foobar"),
			},
		},
		"Expected and positioned error": {
			inputRes: Result[any]{
				Err:       NewError([]rune("foo"), "testerr"),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError([]rune("foo"), errors.New("required"), "testerr"),
				Remaining: []rune("foobar"),
			},
		},
		"Fatal and positioned error": {
			inputRes: Result[any]{
				Err:       NewFatalError([]rune("foo"), errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError([]rune("foo"), errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
		},
	}

	for name, test := range tests {
		test := test
		childParser := func([]rune) Result[any] {
			return test.inputRes
		}
		t.Run(name, func(t *testing.T) {
			res := MustBe(childParser)([]rune("foobar"))
			assert.Equal(t, test.outputRes, res)
		})
	}
}

func TestInterceptExpectedError(t *testing.T) {
	tests := map[string]struct {
		inputRes  Result[any]
		outputRes Result[any]
	}{
		"No error": {
			inputRes: Result[any]{
				Payload:   "foo",
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Payload:   "foo",
				Remaining: []rune("foobar"),
			},
		},
		"Real error": {
			inputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewFatalError(nil, errors.New("testerr")),
				Remaining: []rune("foobar"),
			},
		},
		"Expected error": {
			inputRes: Result[any]{
				Err:       NewError(nil, "testerr"),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewError(nil, "foobar"),
				Remaining: []rune("foobar"),
			},
		},
		"Expected and positioned error": {
			inputRes: Result[any]{
				Err:       NewError([]rune("foo"), "testerr"),
				Remaining: []rune("foobar"),
			},
			outputRes: Result[any]{
				Err:       NewError([]rune("foo"), "foobar"),
				Remaining: []rune("foobar"),
			},
		},
	}

	for name, test := range tests {
		test := test
		childParser := func([]rune) Result[any] {
			return test.inputRes
		}
		t.Run(name, func(t *testing.T) {
			res := Expect(childParser, "foobar")([]rune("foobar"))
			assert.Equal(t, test.outputRes, res)
		})
	}
}

func TestMatch(t *testing.T) {
	str := Term("abc")

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "abc"),
		},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
			err:       NewError([]rune("ab"), "abc"),
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
			err:       NewError([]rune("n"), "abc"),
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "abc"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := str([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestDiscard(t *testing.T) {
	parser := Discard(Term("abc"))

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
			require.Nil(t, res.Err)
			assert.Empty(t, res.Payload)
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestDiscardAll(t *testing.T) {
	parser := DiscardAll(Term("abc"))

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
			require.Nil(t, res.Err)
			assert.Empty(t, res.Payload)
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestOptional(t *testing.T) {
	parser := Optional(
		Sequence(
			Term("abc"),
			Term("def"),
		),
	)

	tests := map[string]struct {
		input     string
		result    []string
		remaining string
		err       *Error
	}{
		"empty input": {},
		"smaller than string": {
			input:     "ab",
			remaining: "ab",
		},
		"only first string": {
			input:     "abc",
			remaining: "abc",
		},
		"bit of second string": {
			input:     "abcde",
			remaining: "abcde",
		},
		"full string": {
			input:     "abcdef",
			remaining: "",
			result:    []string{"abc", "def"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := parser([]rune(test.input))
			assert.Equal(t, test.err, res.Err)
			assert.Equal(t, test.result, res.Payload)
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNumber(t *testing.T) {
	p := Number

	tests := map[string]struct {
		input     string
		result    any
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "number"),
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
			err:       NewError([]rune("hello"), "number"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := p([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestBoolean(t *testing.T) {
	p := Boolean

	tests := map[string]struct {
		input     string
		result    bool
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "boolean"),
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
			err:       NewError([]rune("hello"), "boolean"),
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
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestQuotedString(t *testing.T) {
	str := QuotedString

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "quoted string"),
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
			err:       NewError([]rune("foo"), "quoted string"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := str([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestQuotedMultilineString(t *testing.T) {
	str := TripleQuoteString

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "quoted string"),
		},
		"only quote": {
			input:     `"""foo"""`,
			result:    "foo",
			remaining: "",
		},
		"quote plus extra": {
			input:     `"""foo""" and this`,
			result:    "foo",
			remaining: " and this",
		},
		"quote with escapes": {
			input:     `"""foo\u263abar""" and this`,
			result:    `foo\u263abar`,
			remaining: " and this",
		},
		"quote with escaped quotes": {
			input:     `"""foo"bar"baz""" and this`,
			result:    `foo"bar"baz`,
			remaining: " and this",
		},
		"quote with escaped quotes 2": {
			input: `"""foo\\\"
bar\\\"

baz""" and this`,
			result: `foo\\\"
bar\\\"

baz`,
			remaining: " and this",
		},
		"not quoted": {
			input:     `foo`,
			remaining: "foo",
			err:       NewError([]rune("foo"), "quoted string"),
		},
		"unfinished quotes": {
			input:     `"""foo\"bar\"baz and this`,
			remaining: `"""foo\"bar\"baz and this`,
			err:       NewFatalError([]rune(``), errors.New("required"), "end triple-quote"),
		},
		"unfinished end quotes": {
			input:     `"""""0`,
			remaining: `"""""0`,
			err:       NewFatalError([]rune(``), errors.New("required"), "end triple-quote"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := str([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestArray(t *testing.T) {
	p := LiteralValue()

	tests := map[string]struct {
		input     string
		result    any
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "boolean", "number", "quoted string", "quoted string", "null", "array", "object"),
		},
		"empty array": {
			input:     "[] and this",
			result:    []any{},
			remaining: " and this",
		},
		"empty array with whitespace": {
			input:     "[ \t] and this",
			result:    []any{},
			remaining: " and this",
		},
		"single element array": {
			input:     `[ "foo" ] and this`,
			result:    []any{"foo"},
			remaining: " and this",
		},
		"tailing comma array": {
			input:     `[ "foo", ] and this`,
			result:    []any{"foo"},
			remaining: ` and this`,
		},
		"random stuff array": {
			input:     `[ "foo", whats this ] and this`,
			err:       NewError([]rune(`whats this ] and this`), "boolean", "number", "quoted string", "quoted string", "null", "array", "object"),
			remaining: `[ "foo", whats this ] and this`,
		},
		"random stuff array 2": {
			input:     `[ "foo" whats this ] and this`,
			err:       NewError([]rune(`whats this ] and this`), ",", "]"),
			remaining: `[ "foo" whats this ] and this`,
		},
		"multiple elements array": {
			input:     `[ "foo", false,5.2] and this`,
			result:    []any{"foo", false, float64(5.2)},
			remaining: " and this",
		},
		"multiple elements array line broken": {
			input: `[
	"foo",
	null,
	"bar",
	[true,false]
] and this`,
			result:    []any{"foo", nil, "bar", []any{true, false}},
			remaining: " and this",
		},
		"multiple elements array line broken windows style": {
			input:     "[\r\n  \"foo\",\r\n  null,\r\n  \"bar\",\r\n  [true,false]\r\n] and this",
			result:    []any{"foo", nil, "bar", []any{true, false}},
			remaining: " and this",
		},
		"multiple elements array comments": {
			input: `[
	"foo", # this is a thing
	null,
# the following lines are things
	"bar",
	[true,false] # and this
] and this`,
			result:    []any{"foo", nil, "bar", []any{true, false}},
			remaining: " and this",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := p([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestObject(t *testing.T) {
	p := LiteralValue()

	tests := map[string]struct {
		input     string
		result    any
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "boolean", "number", "quoted string", "quoted string", "null", "array", "object"),
		},
		"empty object": {
			input:     "{} and this",
			result:    map[string]any{},
			remaining: " and this",
		},
		"empty object with whitespace": {
			input:     "{ \t} and this",
			result:    map[string]any{},
			remaining: " and this",
		},
		"single value object": {
			input:     `{"foo":"bar"} and this`,
			result:    map[string]any{"foo": "bar"},
			remaining: " and this",
		},
		"unfinished item object": {
			input:     `{ "foo": } and this`,
			err:       NewError([]rune("} and this"), "boolean", "number", "quoted string", "quoted string", "null", "array", "object"),
			remaining: `{ "foo": } and this`,
		},
		"random stuff object": {
			input:     `{ "foo": whats this } and this`,
			err:       NewError([]rune("whats this } and this"), "boolean", "number", "quoted string", "quoted string", "null", "array", "object"),
			remaining: `{ "foo": whats this } and this`,
		},
		"multiple values random stuff object": {
			input:     `{ "foo":true "bar":5.2 } and this`,
			err:       NewError([]rune(`"bar":5.2 } and this`), ",", "}"),
			remaining: `{ "foo":true "bar":5.2 } and this`,
		},
		"multiple values object": {
			input:     `{ "foo":true, "bar":5.2 } and this`,
			result:    map[string]any{"foo": true, "bar": 5.2},
			remaining: " and this",
		},
		"multiple values trailing comma object": {
			input:     `{ "foo":true, "bar":5.2, } and this`,
			result:    map[string]any{"foo": true, "bar": 5.2},
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
			result: map[string]any{
				"foo": int64(2),
				"bar": nil,
				"baz": "three",
				"quz": []any{true, false},
			},
			remaining: " and this",
		},
		"multiple values object with comments": {
			input: `{ # start of object
	"foo":2  , # heres a thing
	# A comment followed by an empty comment
	#
	# Followed by another comment
	"bar":     null,
# now these things are crazy
	"baz":
		"three",
	"quz":   [true,false] # woah!
} and this`,
			result: map[string]any{
				"foo": int64(2),
				"bar": nil,
				"baz": "three",
				"quz": []any{true, false},
			},
			remaining: " and this",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := p([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestSpacesAndTabs(t *testing.T) {
	inSet := SpacesAndTabs

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "whitespace"),
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
			err:       NewError([]rune("n"), "whitespace"),
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "whitespace"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestNewline(t *testing.T) {
	inSet := Newline

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "line break"),
		},
		"only a line feed": {
			input:     "\n",
			result:    "\n",
			remaining: "",
		},
		"only a carriage return": {
			input:     "\r",
			remaining: "\r",
			err:       NewError([]rune(""), "line break"),
		},
		"carriage return line feed": {
			input:     "\r\n",
			result:    "\r\n",
			remaining: "",
		},
		"a line feed plus": {
			input:     "\n foo",
			result:    "\n",
			remaining: " foo",
		},
		"crlf plus": {
			input:     "\r\n foo",
			result:    "\r\n",
			remaining: " foo",
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "line break"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := inSet([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestAnyOf(t *testing.T) {
	anyOf := OneOf(
		Char('a'),
		Char('b'),
		Char('c'),
	)

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "a", "b", "c"),
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
			err:       NewError([]rune("n"), "a", "b", "c"),
		},
		"lots not in the set": {
			input:     "nononono",
			remaining: "nononono",
			err:       NewError([]rune("nononono"), "a", "b", "c"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := anyOf([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}

func TestTakeOnly(t *testing.T) {
	pattern := TakeOnly(1, UntilFail(OneOf(Char('1'), Char('2'), Char('3'))))

	tests := map[string]struct {
		input     string
		result    string
		remaining string
		err       *Error
	}{
		"empty input": {
			err: NewError([]rune(""), "1", "2", "3"),
		},
		"fewer than needed": {
			input:     "2",
			result:    "",
			remaining: "",
		},
		"exactly needed": {
			input:     "12",
			result:    "2",
			remaining: "",
		},
		"more than needed": {
			input:     "123",
			result:    "2",
			remaining: "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			res := pattern([]rune(test.input))
			require.Equal(t, test.err, res.Err, "Error")
			assert.Equal(t, test.result, res.Payload, "Result")
			assert.Equal(t, test.remaining, string(res.Remaining), "Remaining")
		})
	}
}
