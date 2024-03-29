package parser

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Result represents the result of a parser given an input.
type Result[T any] struct {
	Payload   T
	Err       *Error
	Remaining []rune
}

// Func is the common signature of a parser function.
type Func[T any] func([]rune) Result[T]

// ZeroedFuncAs converts a Func of type Tin into a Func of type Tout.
//
// WARNING: No conversion is made between payloads of Tin to Tout, instead a
// zero value of Tout will be emitted.
func ZeroedFuncAs[Tin, Tout any](f Func[Tin]) Func[Tout] {
	return func(r []rune) Result[Tout] {
		return ResultInto[Tout](f(r))
	}
}

// FuncAsAny converts a Func of type Tin into a Func of type any. The payload is
// passed unchanged but cast into an any.
func FuncAsAny[T any](f Func[T]) Func[any] {
	return func(r []rune) Result[any] {
		tmpRes := f(r)

		outRes := ResultInto[any](tmpRes)
		outRes.Payload = tmpRes.Payload
		return outRes
	}
}

//------------------------------------------------------------------------------

// Success creates a result with a payload from successful parsing.
func Success[T any](payload T, remaining []rune) Result[T] {
	return Result[T]{
		Payload:   payload,
		Remaining: remaining,
	}
}

// Fail creates a result with an error from failed parsing.
func Fail[T any](err *Error, input []rune) Result[T] {
	return Result[T]{
		Err:       err,
		Remaining: input,
	}
}

func ResultInto[T, L any](from Result[L]) Result[T] {
	return Result[T]{
		Err:       from.Err,
		Remaining: from.Remaining,
	}
}

//------------------------------------------------------------------------------

// Char parses a single character and expects it to match one candidate.
func Char(c rune) Func[string] {
	return func(input []rune) Result[string] {
		if len(input) == 0 || input[0] != c {
			return Fail[string](NewError(input, string(c)), input)
		}
		return Success(string(c), input[1:])
	}
}

var (
	charBracketOpen  = Char('(')
	charBracketClose = Char(')')
	charSquigOpen    = Char('{')
	charSquigClose   = Char('}')
	charSquareOpen   = Char('[')
	charSquareClose  = Char(']')
	charDot          = Char('.')
	charUnderscore   = Char('_')
	charMinus        = Char('-')
	charEquals       = Char('=')
	charComma        = Char(',')
	charColon        = Char(':')
	charDollar       = Char('$')
	charHash         = Char('#')
)

// NotChar parses any number of characters until they match a single candidate.
func NotChar(c rune) Func[string] {
	exp := "not " + string(c)
	return func(input []rune) Result[string] {
		if len(input) == 0 || input[0] == c {
			return Fail[string](NewError(input, exp), input)
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
func InSet(set ...rune) Func[string] {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	exp := fmt.Sprintf("chars(%v)", string(set))
	return func(input []rune) Result[string] {
		if len(input) == 0 {
			return Fail[string](NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; !exists {
				if i == 0 {
					return Fail[string](NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// NotInSet parses any number of characters until a rune within a given set is
// encountered.
func NotInSet(set ...rune) Func[string] {
	setMap := make(map[rune]struct{}, len(set))
	for _, r := range set {
		setMap[r] = struct{}{}
	}
	exp := fmt.Sprintf("not chars(%v)", string(set))
	return func(input []rune) Result[string] {
		if len(input) == 0 {
			return Fail[string](NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if _, exists := setMap[input[i]]; exists {
				if i == 0 {
					return Fail[string](NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// InRange parses any number of characters between two runes inclusive.
func InRange(lower, upper rune) Func[string] {
	exp := fmt.Sprintf("range(%c - %c)", lower, upper)
	return func(input []rune) Result[string] {
		if len(input) == 0 {
			return Fail[string](NewError(input, exp), input)
		}
		i := 0
		for ; i < len(input); i++ {
			if input[i] < lower || input[i] > upper {
				if i == 0 {
					return Fail[string](NewError(input, exp), input)
				}
				break
			}
		}
		return Success(string(input[:i]), input[i:])
	}
}

// SpacesAndTabs parses any number of space or tab characters.
var SpacesAndTabs = Expect(InSet(' ', '\t'), "whitespace")

// Term parses a single instance of a string.
func Term(term string) Func[string] {
	termRunes := []rune(term)
	return func(input []rune) Result[string] {
		if len(input) < len(termRunes) {
			return Fail[string](NewError(input, term), input)
		}
		for i, c := range termRunes {
			if input[i] != c {
				return Fail[string](NewError(input, term), input)
			}
		}
		return Success(term, input[len(termRunes):])
	}
}

// UntilTerm parses any number of characters until an instance of a string is
// met. The provided term is not included in the result.
func UntilTerm(term string) Func[string] {
	termRunes := []rune(term)
	return func(input []rune) Result[string] {
		if len(input) < len(termRunes) {
			return Fail[string](NewError(input, term), input)
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
		return Fail[string](NewError(input, term), input)
	}
}

// Number parses any number of numerical characters into either an int64 or, if
// the number contains float characters, a float64.
var Number = func() Func[any] {
	digitSet := InSet([]rune("0123456789")...)
	dot := charDot
	expectNumber := Expect(digitSet, "number")

	return func(input []rune) Result[any] {
		var negative bool
		res := charMinus(input)
		if res.Err == nil {
			negative = true
		}
		res = expectNumber(res.Remaining)
		if res.Err != nil {
			return ResultInto[any](res)
		}
		resStr := res.Payload
		if resTest := dot(res.Remaining); resTest.Err == nil {
			if resTest = digitSet(resTest.Remaining); resTest.Err == nil {
				resStr = resStr + "." + resTest.Payload
				res = resTest
			}
		}

		outRes := ResultInto[any](res)
		if strings.Contains(resStr, ".") {
			f, err := strconv.ParseFloat(resStr, 64)
			if err != nil {
				err = fmt.Errorf("failed to parse '%v' as float: %v", resStr, err)
				return Fail[any](NewFatalError(input, err), input)
			}
			if negative {
				f = -f
			}
			outRes.Payload = f
		} else {
			i, err := strconv.ParseInt(resStr, 10, 64)
			if err != nil {
				err = fmt.Errorf("failed to parse '%v' as integer: %v", resStr, err)
				return Fail[any](NewFatalError(input, err), input)
			}
			if negative {
				i = -i
			}
			outRes.Payload = i
		}
		return outRes
	}
}()

// Boolean parses either 'true' or 'false' into a boolean value.
var Boolean = func() Func[bool] {
	parser := Expect(OneOf(Term("true"), Term("false")), "boolean")
	return func(input []rune) Result[bool] {
		res := parser(input)
		if res.Err == nil {
			return Success(res.Payload == "true", res.Remaining)
		}
		return ResultInto[bool](res)
	}
}()

// Null parses a null literal value.
var Null = func() Func[any] {
	nullMatch := Term("null")
	return func(input []rune) Result[any] {
		res := ResultInto[any](nullMatch(input))
		if res.Err == nil {
			res.Payload = nil
		}
		return res
	}
}()

var DiscardedWhitespaceNewlineComments = DiscardAll(OneOf(SpacesAndTabs, NewlineAllowComment))

// Array parses an array literal.
func Array() Func[[]any] {
	pattern := DelimitedPattern(
		Expect(Sequence(charSquareOpen, DiscardedWhitespaceNewlineComments), "array"),
		LiteralValue(),
		Sequence(
			Discard(SpacesAndTabs),
			charComma,
			DiscardedWhitespaceNewlineComments,
		),
		Sequence(DiscardedWhitespaceNewlineComments, charSquareClose),
	)

	return func(r []rune) Result[[]any] {
		return pattern(r)
	}
}

// Object parses an object literal.
func Object() Func[map[string]any] {
	pattern := DelimitedPattern(
		Expect(Sequence(
			charSquigOpen, DiscardedWhitespaceNewlineComments,
		), "object"),
		Sequence(
			FuncAsAny(QuotedString),
			FuncAsAny(Discard(SpacesAndTabs)),
			FuncAsAny(charColon),
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			LiteralValue(),
		),
		Sequence(
			Discard(SpacesAndTabs),
			charComma,
			DiscardedWhitespaceNewlineComments,
		),
		Sequence(DiscardedWhitespaceNewlineComments, charSquigClose),
	)

	return func(input []rune) Result[map[string]any] {
		res := pattern(input)
		if res.Err != nil {
			return Fail[map[string]any](res.Err, input)
		}

		values := map[string]any{}
		for _, kv := range res.Payload {
			values[kv[0].(string)] = kv[4]
		}

		return Success(values, res.Remaining)
	}
}

// LiteralValue parses a literal bool, number, quoted string, null value, array
// of literal values, or object.
func LiteralValue() Func[any] {
	return func(r []rune) Result[any] {
		return OneOf(
			FuncAsAny(Boolean),
			FuncAsAny(Number),
			FuncAsAny(TripleQuoteString),
			FuncAsAny(QuotedString),
			FuncAsAny(Null),
			FuncAsAny(Array()),
			FuncAsAny(Object()),
		)(r)
	}
}

// JoinStringPayloads wraps a parser that returns a []interface{} of exclusively
// string values and returns a result of a joined string of all the elements.
//
// Warning! If the result is not a []interface{}, or if an element is not a
// string, then this parser returns a zero value instead.
func JoinStringPayloads(p Func[[]string]) Func[string] {
	return func(input []rune) Result[string] {
		res := p(input)
		if res.Err != nil {
			return ResultInto[string](res)
		}

		var buf bytes.Buffer
		for _, v := range res.Payload {
			buf.WriteString(v)
		}

		outRes := ResultInto[string](res)
		outRes.Payload = buf.String()
		return outRes
	}
}

// Comment parses a # comment (always followed by a line break).
var Comment = JoinStringPayloads(
	Sequence(
		charHash,
		JoinStringPayloads(
			Optional(UntilFail(NotChar('\n'))),
		),
		Newline,
	),
)

// SnakeCase parses any number of characters of a camel case string. This parser
// is very strict and does not support double underscores, prefix or suffix
// underscores.
var SnakeCase = Expect(JoinStringPayloads(UntilFail(OneOf(
	InRange('a', 'z'),
	InRange('0', '9'),
	charUnderscore,
))), "snake-case")

// TripleQuoteString parses a single instance of a triple-quoted multiple line
// string. The result is the inner contents.
func TripleQuoteString(input []rune) Result[string] {
	if len(input) < 6 ||
		input[0] != '"' ||
		input[1] != '"' ||
		input[2] != '"' {
		return Fail[string](NewError(input, "quoted string"), input)
	}
	for i := 3; i < len(input)-2; i++ {
		if input[i] == '"' &&
			input[i+1] == '"' &&
			input[i+2] == '"' {
			return Success(string(input[3:i]), input[i+3:])
		}
	}
	return Fail[string](NewFatalError(input[len(input):], errors.New("required"), "end triple-quote"), input)
}

// QuotedString parses a single instance of a quoted string. The result is the
// inner contents unescaped.
func QuotedString(input []rune) Result[string] {
	if len(input) == 0 || input[0] != '"' {
		return Fail[string](NewError(input, "quoted string"), input)
	}
	escaped := false
	for i := 1; i < len(input); i++ {
		if input[i] == '"' && !escaped {
			unquoted, err := strconv.Unquote(string(input[:i+1]))
			if err != nil {
				err = fmt.Errorf("failed to unescape quoted string contents: %v", err)
				return Fail[string](NewFatalError(input, err), input)
			}
			return Success(unquoted, input[i+1:])
		}
		if input[i] == '\n' {
			Fail[string](NewFatalError(input[i:], errors.New("required"), "end quote"), input)
		}
		if input[i] == '\\' {
			escaped = !escaped
		} else if escaped {
			escaped = false
		}
	}
	return Fail[string](NewFatalError(input[len(input):], errors.New("required"), "end quote"), input)
}

// EmptyLine ensures that a line is empty, but doesn't advance the parser beyond
// the newline char.
func EmptyLine(r []rune) Result[any] {
	if len(r) > 0 && r[0] == '\n' {
		return Success[any](nil, r)
	}
	return Fail[any](NewError(r, "Empty line"), r)
}

// EndOfInput ensures that the input is now empty.
func EndOfInput(r []rune) Result[any] {
	if len(r) == 0 {
		return Success[any](nil, r)
	}
	return Fail[any](NewError(r, "End of input"), r)
}

// Newline parses a line break.
var Newline = Expect(
	JoinStringPayloads(
		Sequence(
			Optional(Char('\r')),
			Char('\n'),
		),
	),
	"line break",
)

// NewlineAllowComment parses an optional comment followed by a mandatory line
// break.
var NewlineAllowComment = Expect(OneOf(Comment, Newline), "line break")

// UntilFail applies a parser until it fails, and returns a slice containing all
// results. If the parser does not succeed at least once an error is returned.
func UntilFail[T any](parser Func[T]) Func[[]T] {
	return func(input []rune) Result[[]T] {
		res := parser(input)
		if res.Err != nil {
			return ResultInto[[]T](res)
		}
		results := []T{res.Payload}
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
func DelimitedPattern[S, P, D, E any](
	start Func[S], primary Func[P], delimiter Func[D], stop Func[E],
) Func[[]P] {
	return func(input []rune) Result[[]P] {
		var remaining []rune
		if res := start(input); res.Err != nil {
			return ResultInto[[]P](res)
		} else {
			remaining = res.Remaining
		}

		results := []P{}

		if res := primary(remaining); res.Err != nil {
			if resStop := stop(res.Remaining); resStop.Err == nil {
				return Success(results, resStop.Remaining)
			}
			return Fail[[]P](res.Err, input)
		} else {
			results = append(results, res.Payload)
			remaining = res.Remaining
		}

		for {
			if res := delimiter(remaining); res.Err != nil {
				resStop := stop(res.Remaining)
				if resStop.Err == nil {
					return Success(results, resStop.Remaining)
				}
				res.Err.Add(resStop.Err)
				return Fail[[]P](res.Err, input)
			} else {
				remaining = res.Remaining
			}

			if res := primary(remaining); res.Err != nil {
				if resStop := stop(res.Remaining); resStop.Err == nil {
					return Success(results, resStop.Remaining)
				}
				return Fail[[]P](res.Err, input)
			} else {
				results = append(results, res.Payload)
				remaining = res.Remaining
			}
		}
	}
}

// DelimitedResult is an explicit result struct returned by the Delimited
// parser, containing a slice of primary parser payloads and a slice of
// delimited parser payloads.
type DelimitedResult[P, D any] struct {
	Primary   []P
	Delimiter []D
}

// Delimited attempts to parse one or more primary parsers, where after the
// first parse a delimiter is expected. Parsing is stopped only once a delimiter
// parse is not successful.
//
// Two slices are returned, the first element being a slice of primary results
// and the second element being the delimiter results.
func Delimited[P, D any](primary Func[P], delimiter Func[D]) Func[DelimitedResult[P, D]] {
	return func(input []rune) Result[DelimitedResult[P, D]] {
		delimRes := DelimitedResult[P, D]{}

		res := primary(input)
		if res.Err != nil {
			return ResultInto[DelimitedResult[P, D]](res)
		}
		delimRes.Primary = append(delimRes.Primary, res.Payload)

		for {
			dRes := delimiter(res.Remaining)
			if dRes.Err != nil {
				return Success(delimRes, dRes.Remaining)
			}
			delimRes.Delimiter = append(delimRes.Delimiter, dRes.Payload)

			if res = primary(dRes.Remaining); res.Err != nil {
				return Fail[DelimitedResult[P, D]](res.Err, input)
			}
			delimRes.Primary = append(delimRes.Primary, res.Payload)
		}
	}
}

// TakeOnly wraps an array based combinator with one that only extracts a single
// element of the resulting values. NOTE: If the index is
func TakeOnly[T any](index int, p Func[[]T]) Func[T] {
	return func(input []rune) Result[T] {
		res := p(input)
		if res.Err != nil {
			return Fail[T](res.Err, input)
		}
		if len(res.Payload) <= index {
			return ResultInto[T](res)
		}
		return Success(res.Payload[index], res.Remaining)
	}
}

// Sequence applies a sequence of parsers and returns either a slice of the
// results or an error if any parser fails.
func Sequence[T any](parsers ...Func[T]) Func[[]T] {
	return func(input []rune) Result[[]T] {
		results := make([]T, 0, len(parsers))

		res := Result[T]{
			Remaining: input,
		}
		for _, p := range parsers {
			if res = p(res.Remaining); res.Err != nil {
				return Fail[[]T](res.Err, input)
			}
			results = append(results, res.Payload)
		}
		return Success(results, res.Remaining)
	}
}

// Optional applies a child parser and if it returns an ExpectedError then it is
// cleared and a zero result is returned instead. Any other form of error will
// be returned unchanged.
func Optional[T any](parser Func[T]) Func[T] {
	return func(input []rune) Result[T] {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err = nil
		}
		return res
	}
}

// OptionalPtr applies a child parser and if it returns an ExpectedError then it
// is cleared and a nil result is returned instead. Any other form of error will
// be returned unchanged (but converted to a pointer type).
func OptionalPtr[T any](parser Func[T]) Func[*T] {
	return func(input []rune) Result[*T] {
		res := parser(input)
		if res.Err == nil {
			return Success(&res.Payload, res.Remaining)
		}
		if !res.Err.IsFatal() {
			res.Err = nil
			return Success[*T](nil, res.Remaining)
		}
		return Fail[*T](res.Err, input)
	}
}

// Discard the result of a child parser, regardless of the result. This has the
// effect of running the parser and returning only Remaining.
func Discard[T any](parser Func[T]) Func[T] {
	return func(input []rune) Result[T] {
		res := parser(input)
		var tmp T
		res.Payload = tmp
		res.Err = nil
		return res
	}
}

// DiscardAll the results of a child parser, applied until it fails. This has
// the effect of running the parser and returning only Remaining.
func DiscardAll[T any](parser Func[T]) Func[T] {
	return func(input []rune) Result[T] {
		res := parser(input)
		for res.Err == nil {
			res = parser(res.Remaining)
		}
		var tmp T
		res.Payload = tmp
		res.Err = nil
		return res
	}
}

// MustBe applies a parser and if the result is a non-fatal error then it is
// upgraded to a fatal one.
func MustBe[T any](parser Func[T]) Func[T] {
	return func(input []rune) Result[T] {
		res := parser(input)
		if res.Err != nil && !res.Err.IsFatal() {
			res.Err.Err = errors.New("required")
		}
		return res
	}
}

// Expect applies a parser and if an error is returned the list of expected candidates is replaced with the given
// strings. This is useful for providing better context to users.
func Expect[T any](parser Func[T], expected ...string) Func[T] {
	return func(input []rune) Result[T] {
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
func OneOf[T any](parsers ...Func[T]) Func[T] {
	return func(input []rune) Result[T] {
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
		return Fail[T](err, input)
	}
}

func bestMatch[T any](left, right Result[T]) Result[T] {
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
func BestMatch[T any](parsers ...Func[T]) Func[T] {
	if len(parsers) == 1 {
		return parsers[0]
	}
	return func(input []rune) Result[T] {
		res := parsers[0](input)
		for _, p := range parsers[1:] {
			resTmp := p(input)
			res = bestMatch(res, resTmp)
		}
		return res
	}
}
