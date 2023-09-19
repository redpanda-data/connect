package parser

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

//------------------------------------------------------------------------------

// Result represents the result of a parser given an input.
type Result struct {
	Payload   any
	Err       *Error
	Remaining []rune
}

// Func is the common signature of a parser function.
type Func func([]rune) Result

//------------------------------------------------------------------------------

// Success creates a result with a payload from successful parsing.
func Success(payload any, remaining []rune) Result {
	return Result{
		Payload:   payload,
		Remaining: remaining,
	}
}

// Fail creates a result with an error from failed parsing.
func Fail(err *Error, input []rune) Result {
	return Result{
		Err:       err,
		Remaining: input,
	}
}

//------------------------------------------------------------------------------

// Char parses a single character and expects it to match one candidate.
func Char(c rune) Func {
	return func(input []rune) Result {
		if len(input) == 0 || input[0] != c {
			return Fail(NewError(input, string(c)), input)
		}
		return Success(string(c), input[1:])
	}
}

// NotChar parses any number of characters until they match a single candidate.
func NotChar(c rune) Func {
	exp := "not " + string(c)
	return func(input []rune) Result {
		if len(input) == 0 || input[0] == c {
			return Fail(NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] == c {
				return Success(string(input[:i]), input[i:])
			}
		}
		return Success(string(input), nil)
	}
}

// InSet parses any number of characters within a set of runes.
func InSet(set ...rune) Func {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	exp := fmt.Sprintf("chars(%v)", string(set))
	return func(input []rune) Result {
		if len(input) == 0 {
			return Fail(NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; !exists {
				if i == 0 {
					return Fail(NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// NotInSet parses any number of characters until a rune within a given set is
// encountered.
func NotInSet(set ...rune) Func {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	exp := fmt.Sprintf("not chars(%v)", string(set))
	return func(input []rune) Result {
		if len(input) == 0 {
			return Fail(NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; exists {
				if i == 0 {
					return Fail(NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// InRange parses any number of characters between two runes inclusive.
func InRange(lower, upper rune) Func {
	exp := fmt.Sprintf("range(%c - %c)", lower, upper)
	return func(input []rune) Result {
		if len(input) == 0 {
			return Fail(NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] < lower || input[i] > upper {
				if i == 0 {
					return Fail(NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// SpacesAndTabs parses any number of space or tab characters.
func SpacesAndTabs() Func {
	return Expect(InSet(' ', '\t'), "whitespace")
}

// Term parses a single instance of a string.
func Term(term string) Func {
	termRunes := []rune(term)
	return func(input []rune) Result {
		if len(input) < len(termRunes) {
			return Fail(NewError(input, term), input)
		}
		for i, c := range termRunes {
			if input[i] != c {
				return Fail(NewError(input, term), input)
			}
		}
		return Success(term, input[len(termRunes):])
	}
}

// UntilTerm parses any number of characters until an instance of a string is
// met. The provided term is not included in the result.
func UntilTerm(term string) Func {
	termRunes := []rune(term)
	return func(input []rune) Result {
		if len(input) < len(termRunes) {
			return Fail(NewError(input, term), input)
		}
		i := 0
		for ; i <= (len(input) - len(termRunes)); i++ {
			matched := true
			for j := 0; j < len(termRunes); j++ {
				if input[i+j] != termRunes[j] {
					matched = false
					break
				}
			}
			if matched {
				return Success(string(input[:i]), input[i:])
			}
		}
		return Fail(NewError(input, term), input)
	}
}

// Number parses any number of numerical characters into either an int64 or, if
// the number contains float characters, a float64.
func Number() Func {
	digitSet := InSet([]rune("0123456789")...)
	dot := Char('.')
	minus := Char('-')
	return func(input []rune) Result {
		var negative bool
		res := minus(input)
		if res.Err == nil {
			negative = true
		}
		res = Expect(digitSet, "number")(res.Remaining)
		if res.Err != nil {
			return res
		}
		resStr := res.Payload.(string)
		if resTest := dot(res.Remaining); resTest.Err == nil {
			if resTest = digitSet(resTest.Remaining); resTest.Err == nil {
				resStr = resStr + "." + resTest.Payload.(string)
				res = resTest
			}
		}
		if strings.Contains(resStr, ".") {
			f, err := strconv.ParseFloat(resStr, 64)
			if err != nil {
				err = fmt.Errorf("failed to parse '%v' as float: %v", resStr, err)
				return Fail(NewFatalError(input, err), input)
			}
			if negative {
				f = -f
			}
			res.Payload = f
		} else {
			i, err := strconv.ParseInt(resStr, 10, 64)
			if err != nil {
				err = fmt.Errorf("failed to parse '%v' as integer: %v", resStr, err)
				return Fail(NewFatalError(input, err), input)
			}
			if negative {
				i = -i
			}
			res.Payload = i
		}
		return res
	}
}

// Boolean parses either 'true' or 'false' into a boolean value.
func Boolean() Func {
	parser := Expect(OneOf(Term("true"), Term("false")), "boolean")
	return func(input []rune) Result {
		res := parser(input)
		if res.Err == nil {
			res.Payload = res.Payload.(string) == "true"
		}
		return res
	}
}

// Null parses a null literal value.
func Null() Func {
	nullMatch := Term("null")
	return func(input []rune) Result {
		res := nullMatch(input)
		if res.Err == nil {
			res.Payload = nil
		}
		return res
	}
}

// Array parses an array literal.
func Array() Func {
	begin, comma, end := Char('['), Char(','), Char(']')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)
	return func(input []rune) Result {
		return DelimitedPattern(
			Expect(Sequence(
				begin,
				whitespace,
			), "array"),
			LiteralValue(),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				end,
			),
			true,
		)(input)
	}
}

// Object parses an object literal.
func Object() Func {
	begin, comma, end := Char('{'), Char(','), Char('}')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)

	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				begin,
				whitespace,
			), "object"),
			Sequence(
				QuotedString(),
				Discard(SpacesAndTabs()),
				Char(':'),
				Discard(whitespace),
				LiteralValue(),
			),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				end,
			),
			true,
		)(input)
		if res.Err != nil {
			return res
		}

		values := map[string]any{}
		for _, sequenceValue := range res.Payload.([]any) {
			slice := sequenceValue.([]any)
			values[slice[0].(string)] = slice[4]
		}

		res.Payload = values
		return res
	}
}

// LiteralValue parses a literal bool, number, quoted string, null value, array
// of literal values, or object.
func LiteralValue() Func {
	return OneOf(
		Boolean(),
		Number(),
		TripleQuoteString(),
		QuotedString(),
		Null(),
		Array(),
		Object(),
	)
}

// JoinStringPayloads wraps a parser that returns a []interface{} of exclusively
// string values and returns a result of a joined string of all the elements.
//
// Warning! If the result is not a []interface{}, or if an element is not a
// string, then this parser returns a zero value instead.
func JoinStringPayloads(p Func) Func {
	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		var buf bytes.Buffer
		slice, _ := res.Payload.([]any)

		for _, v := range slice {
			str, _ := v.(string)
			buf.WriteString(str)
		}
		res.Payload = buf.String()
		return res
	}
}

// Comment parses a # comment (always followed by a line break).
func Comment() Func {
	p := JoinStringPayloads(
		Sequence(
			Char('#'),
			JoinStringPayloads(
				Optional(UntilFail(NotChar('\n'))),
			),
			Newline(),
		),
	)
	return func(input []rune) Result {
		return p(input)
	}
}

// SnakeCase parses any number of characters of a camel case string. This parser
// is very strict and does not support double underscores, prefix or suffix
// underscores.
func SnakeCase() Func {
	return Expect(JoinStringPayloads(UntilFail(OneOf(
		InRange('a', 'z'),
		InRange('0', '9'),
		Char('_'),
	))), "snake-case")
}

// TripleQuoteString parses a single instance of a triple-quoted multiple line
// string. The result is the inner contents.
func TripleQuoteString() Func {
	return func(input []rune) Result {
		if len(input) < 6 ||
			input[0] != '"' ||
			input[1] != '"' ||
			input[2] != '"' {
			return Fail(NewError(input, "quoted string"), input)
		}
		for i := 3; i < len(input)-2; i++ {
			if input[i] == '"' &&
				input[i+1] == '"' &&
				input[i+2] == '"' {
				return Success(string(input[3:i]), input[i+3:])
			}
		}
		return Fail(NewFatalError(input[len(input):], errors.New("required"), "end triple-quote"), input)
	}
}

// QuotedString parses a single instance of a quoted string. The result is the
// inner contents unescaped.
func QuotedString() Func {
	return func(input []rune) Result {
		if len(input) == 0 || input[0] != '"' {
			return Fail(NewError(input, "quoted string"), input)
		}
		escaped := false
		for i := 1; i < len(input); i++ {
			if input[i] == '"' && !escaped {
				unquoted, err := strconv.Unquote(string(input[:i+1]))
				if err != nil {
					err = fmt.Errorf("failed to unescape quoted string contents: %v", err)
					return Fail(NewFatalError(input, err), input)
				}
				return Success(unquoted, input[i+1:])
			}
			if input[i] == '\n' {
				Fail(NewFatalError(input[i:], errors.New("required"), "end quote"), input)
			}
			if input[i] == '\\' {
				escaped = !escaped
			} else if escaped {
				escaped = false
			}
		}
		return Fail(NewFatalError(input[len(input):], errors.New("required"), "end quote"), input)
	}
}

// EmptyLine ensures that a line is empty, but doesn't advance the parser beyond
// the newline char.
func EmptyLine() Func {
	return func(r []rune) Result {
		if len(r) > 0 && r[0] == '\n' {
			return Success(nil, r)
		}
		return Fail(NewError(r, "Empty line"), r)
	}
}

// EndOfInput ensures that the input is now empty.
func EndOfInput() Func {
	return func(r []rune) Result {
		if len(r) == 0 {
			return Success(nil, r)
		}
		return Fail(NewError(r, "End of input"), r)
	}
}

// Newline parses a line break.
func Newline() Func {
	return Expect(
		JoinStringPayloads(
			Sequence(
				Optional(Char('\r')),
				Char('\n'),
			),
		),
		"line break")
}

// NewlineAllowComment parses an optional comment followed by a mandatory line
// break.
func NewlineAllowComment() Func {
	return Expect(OneOf(Comment(), Newline()), "line break")
}

// UntilFail applies a parser until it fails, and returns a slice containing all
// results. If the parser does not succeed at least once an error is returned.
func UntilFail(parser Func) Func {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil {
			return res
		}
		results := []any{res.Payload}
		for {
			if res = parser(res.Remaining); res.Err != nil {
				return Success(results, res.Remaining)
			}
			results = append(results, res.Payload)
		}
	}
}

// DelimitedPattern attempts to parse zero or more primary parsers in between a
// start and stop parser, where after the first parse a delimiter is expected.
// Parsing is stopped only once an explicit stop parser is successful.
//
// If allowTrailing is set to false and a delimiter is parsed but a subsequent
// primary parse fails then an error is returned.
//
// Only the results of the primary parser are returned, the results of the
// start, delimiter and stop parsers are discarded.
func DelimitedPattern(
	start, primary, delimiter, stop Func,
	allowTrailing bool,
) Func {
	return func(input []rune) Result {
		res := start(input)
		if res.Err != nil {
			return res
		}

		results := []any{}

		if res = primary(res.Remaining); res.Err != nil {
			if resStop := stop(res.Remaining); resStop.Err == nil {
				resStop.Payload = results
				return resStop
			}
			return Fail(res.Err, input)
		}
		results = append(results, res.Payload)

		for {
			if res = delimiter(res.Remaining); res.Err != nil {
				resStop := stop(res.Remaining)
				if resStop.Err == nil {
					resStop.Payload = results
					return resStop
				}
				res.Err.Add(resStop.Err)
				return Fail(res.Err, input)
			}
			if res = primary(res.Remaining); res.Err != nil {
				if allowTrailing {
					if resStop := stop(res.Remaining); resStop.Err == nil {
						resStop.Payload = results
						return resStop
					}
				}
				return Fail(res.Err, input)
			}
			results = append(results, res.Payload)
		}
	}
}

// DelimitedResult is an explicit result struct returned by the Delimited
// parser, containing a slice of primary parser payloads and a slice of
// delimited parser payloads.
type DelimitedResult struct {
	Primary   []any
	Delimiter []any
}

// Delimited attempts to parse one or more primary parsers, where after the
// first parse a delimiter is expected. Parsing is stopped only once a delimiter
// parse is not successful.
//
// Two slices are returned, the first element being a slice of primary results
// and the second element being the delimiter results.
func Delimited(primary, delimiter Func) Func {
	return func(input []rune) Result {
		delimRes := DelimitedResult{}

		res := primary(input)
		if res.Err != nil {
			return res
		}
		delimRes.Primary = append(delimRes.Primary, res.Payload)

		for {
			if res = delimiter(res.Remaining); res.Err != nil {
				return Success(delimRes, res.Remaining)
			}
			delimRes.Delimiter = append(delimRes.Delimiter, res.Payload)
			if res = primary(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}
			delimRes.Primary = append(delimRes.Primary, res.Payload)
		}
	}
}

// Sequence applies a sequence of parsers and returns either a slice of the
// results or an error if any parser fails.
func Sequence(parsers ...Func) Func {
	return func(input []rune) Result {
		results := make([]any, 0, len(parsers))
		res := Result{
			Remaining: input,
		}
		for _, p := range parsers {
			if res = p(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}
			results = append(results, res.Payload)
		}
		return Success(results, res.Remaining)
	}
}

// Optional applies a child parser and if it returns an ExpectedError then it is
// cleared and a nil result is returned instead. Any other form of error will be
// returned unchanged.
func Optional(parser Func) Func {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err = nil
		}
		return res
	}
}

// Discard the result of a child parser, regardless of the result. This has the
// effect of running the parser and returning only Remaining.
func Discard(parser Func) Func {
	return func(input []rune) Result {
		res := parser(input)
		res.Payload = nil
		res.Err = nil
		return res
	}
}

// DiscardAll the results of a child parser, applied until it fails. This has
// the effect of running the parser and returning only Remaining.
func DiscardAll(parser Func) Func {
	return func(input []rune) Result {
		res := parser(input)
		for res.Err == nil {
			res = parser(res.Remaining)
		}
		res.Payload = nil
		res.Err = nil
		return res
	}
}

// MustBe applies a parser and if the result is a non-fatal error then it is
// upgraded to a fatal one.
func MustBe(parser Func) Func {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err.Err = errors.New("required")
		}
		return res
	}
}

// Expect applies a parser and if an error is returned the list of expected candidates is replaced with the given
// strings. This is useful for providing better context to users.
func Expect(parser Func, expected ...string) Func {
	return func(input []rune) Result {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err.Expected = expected
		}
		return res
	}
}

// OneOf accepts one or more parsers and tries them in order against an input.
// If a parser returns an ExpectedError then the next parser is tried and so
// on. Otherwise, the result is returned.
func OneOf(parsers ...Func) Func {
	return func(input []rune) Result {
		var err *Error
		for _, p := range parsers {
			res := p(input)
			if res.Err == nil || res.Err.IsFatal() {
				return res
			}
			if err == nil || len(err.Input) > len(res.Err.Input) {
				err = res.Err
			} else if len(err.Input) == len(res.Err.Input) {
				err.Add(res.Err)
			}
		}
		return Fail(err, input)
	}
}

func bestMatch(left, right Result) Result {
	remainingLeft := len(left.Remaining)
	remainingRight := len(right.Remaining)
	if left.Err != nil {
		remainingLeft = len(left.Err.Input)
	}
	if right.Err != nil {
		remainingRight = len(right.Err.Input)
	}
	if remainingRight == remainingLeft {
		if left.Err == nil {
			return left
		}
		if right.Err == nil {
			return right
		}
	}
	if remainingRight < remainingLeft {
		return right
	}
	return left
}

// BestMatch accepts one or more parsers and tries them all against an input.
// If all parsers return either a result or an error then the parser that got
// further through the input will have its result returned. This means that an
// error may be returned even if a parser was successful.
//
// For example, given two parsers, A searching for 'aa', and B searching for
// 'aaaa', if the input 'aaab' were provided then an error from parser B would
// be returned, as although the input didn't match, it matched more of parser B
// than parser A.
func BestMatch(parsers ...Func) Func {
	if len(parsers) == 1 {
		return parsers[0]
	}
	return func(input []rune) Result {
		res := parsers[0](input)
		for _, p := range parsers[1:] {
			resTmp := p(input)
			res = bestMatch(res, resTmp)
		}
		return res
	}
}

//------------------------------------------------------------------------------
