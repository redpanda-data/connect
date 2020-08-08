package query

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
)

//------------------------------------------------------------------------------

type badFunctionErr string

func (e badFunctionErr) Error() string {
	return fmt.Sprintf("unrecognised function '%v'", string(e))
}

type badMethodErr string

func (e badMethodErr) Error() string {
	return fmt.Sprintf("unrecognised method '%v'", string(e))
}

//------------------------------------------------------------------------------

func functionArgsParser(allowFunctions bool) parser.Type {
	open, comma, close := parser.Char('('), parser.Char(','), parser.Char(')')
	whitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	paramTypes := []parser.Type{
		parseLiteralWithTails(parser.Boolean()),
		parseLiteralWithTails(parser.Number()),
		parseLiteralWithTails(parser.QuotedString()),
	}

	return func(input []rune) parser.Result {
		tmpParamTypes := paramTypes
		if allowFunctions {
			tmpParamTypes = append([]parser.Type{}, paramTypes...)
			tmpParamTypes = append(tmpParamTypes, Parse)
		}
		return parser.DelimitedPattern(
			parser.Expect(
				parser.Sequence(
					open,
					whitespace,
				),
				"function arguments",
			),
			parser.Expect(
				parser.MustBe(parser.OneOf(tmpParamTypes...)),
				"function argument",
			),
			parser.Sequence(
				parser.Discard(parser.SpacesAndTabs()),
				comma,
				whitespace,
			),
			parser.Sequence(
				whitespace,
				close,
			),
			false, false,
		)(input)
	}
}

func parseFunctionTail(fn Function) parser.Type {
	openBracket := parser.Char('(')
	closeBracket := parser.Char(')')

	whitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		res := parser.OneOf(
			parser.Sequence(
				parser.Expect(openBracket, "method"),
				whitespace,
				Parse,
				whitespace,
				closeBracket,
			),
			methodParser(fn),
			fieldLiteralMapParser(fn),
		)(input)
		if seqSlice, isSlice := res.Payload.([]interface{}); isSlice {
			method, err := mapMethod(fn, seqSlice[2].(Function))
			if err != nil {
				res.Err = parser.NewFatalError(input, err)
				res.Remaining = input
			} else {
				res.Payload = method
			}
		}
		return res
	}
}

func parseLiteralWithTails(litParser parser.Type) parser.Type {
	delim := parser.Sequence(
		parser.Char('.'),
		parser.Discard(
			parser.Sequence(
				parser.NewlineAllowComment(),
				parser.SpacesAndTabs(),
			),
		),
	)

	return func(input []rune) parser.Result {
		res := litParser(input)
		if res.Err != nil {
			return res
		}

		lit := res.Payload
		var fn Function
		for {
			if res = delim(res.Remaining); res.Err != nil {
				var payload interface{} = lit
				if fn != nil {
					payload = fn
				}
				return parser.Result{
					Payload:   payload,
					Remaining: res.Remaining,
				}
			}
			if fn == nil {
				fn = literalFunction(lit)
			}
			if res = parser.MustBe(parseFunctionTail(fn))(res.Remaining); res.Err != nil {
				return parser.Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			fn = res.Payload.(Function)
		}
	}
}

func parseWithTails(fnParser parser.Type) parser.Type {
	delim := parser.Sequence(
		parser.Char('.'),
		parser.Discard(
			parser.Sequence(
				parser.NewlineAllowComment(),
				parser.SpacesAndTabs(),
			),
		),
	)

	mightNot := parser.Sequence(
		parser.Optional(parser.Sequence(
			parser.Char('!'),
			parser.Discard(parser.SpacesAndTabs()),
		)),
		fnParser,
	)

	return func(input []rune) parser.Result {
		res := mightNot(input)
		if res.Err != nil {
			return res
		}

		seq := res.Payload.([]interface{})
		isNot := seq[0] != nil
		fn := seq[1].(Function)
		for {
			if res = delim(res.Remaining); res.Err != nil {
				if isNot {
					fn = &notMethod{fn}
				}
				return parser.Result{
					Payload:   fn,
					Remaining: res.Remaining,
				}
			}
			if res = parser.MustBe(parseFunctionTail(fn))(res.Remaining); res.Err != nil {
				return parser.Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			fn = res.Payload.(Function)
		}
	}
}

func quotedPathSegmentParser() parser.Type {
	pattern := parser.QuotedString()

	return func(input []rune) parser.Result {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		rawSegment, _ := res.Payload.(string)

		// Convert into a JSON pointer style path string.
		rawSegment = strings.Replace(rawSegment, "~", "~0", -1)
		rawSegment = strings.Replace(rawSegment, ".", "~1", -1)

		return parser.Result{
			Payload:   rawSegment,
			Remaining: res.Remaining,
		}
	}
}

func fieldLiteralMapParser(ctxFn Function) parser.Type {
	fieldPathParser := parser.Expect(
		parser.OneOf(
			parser.JoinStringPayloads(
				parser.UntilFail(
					parser.OneOf(
						parser.InRange('a', 'z'),
						parser.InRange('A', 'Z'),
						parser.InRange('0', '9'),
						parser.InRange('*', '+'),
						parser.Char('_'),
						parser.Char('-'),
						parser.Char('~'),
					),
				),
			),
			quotedPathSegmentParser(),
		),
		"field path",
	)

	return func(input []rune) parser.Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		fn, err := getMethodCtor(ctxFn, res.Payload.(string))
		if err != nil {
			return parser.Result{
				Remaining: input,
				Err:       parser.NewFatalError(input, err),
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func variableLiteralParser() parser.Type {
	varPathParser := parser.Expect(
		parser.Sequence(
			parser.Char('$'),
			parser.JoinStringPayloads(
				parser.UntilFail(
					parser.OneOf(
						parser.InRange('a', 'z'),
						parser.InRange('A', 'Z'),
						parser.InRange('0', '9'),
						parser.Char('_'),
						parser.Char('-'),
					),
				),
			),
		),
		"variable path",
	)

	return func(input []rune) parser.Result {
		res := varPathParser(input)
		if res.Err != nil {
			return res
		}

		var fn Function
		var err error

		path := res.Payload.([]interface{})[1].(string)
		fn, err = varFunction(path)
		if err != nil {
			return parser.Result{
				Remaining: input,
				Err:       parser.NewFatalError(input, err),
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func fieldLiteralRootParser() parser.Type {
	fieldPathParser := parser.Expect(
		parser.JoinStringPayloads(
			parser.UntilFail(
				parser.OneOf(
					parser.InRange('a', 'z'),
					parser.InRange('A', 'Z'),
					parser.InRange('0', '9'),
					parser.InRange('*', '+'),
					parser.Char('_'),
					parser.Char('~'),
				),
			),
		),
		"field path",
	)

	return func(input []rune) parser.Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		var fn Function
		var err error

		path := res.Payload.(string)
		if path == "this" {
			fn, err = fieldFunctionCtor()
		} else {
			fn, err = fieldFunctionCtor(path)
		}
		if err != nil {
			return parser.Result{
				Remaining: input,
				Err:       parser.NewFatalError(input, err),
			}
		}

		return parser.Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func methodParser(fn Function) parser.Type {
	p := parser.Sequence(
		parser.Expect(
			parser.SnakeCase(),
			"method",
		),
		functionArgsParser(true),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetMethod := seqSlice[0].(string)
		mtor, exists := methods[targetMethod]
		if !exists {
			return parser.Result{
				Err:       parser.NewFatalError(input, badMethodErr(targetMethod)),
				Remaining: input,
			}
		}

		args := seqSlice[1].([]interface{})
		method, err := mtor(fn, args...)
		if err != nil {
			return parser.Result{
				Err:       parser.NewFatalError(input, err),
				Remaining: input,
			}
		}
		return parser.Result{
			Payload:   method,
			Remaining: res.Remaining,
		}
	}
}

func functionParser() parser.Type {
	p := parser.Sequence(
		parser.Expect(
			parser.SnakeCase(),
			"function",
		),
		functionArgsParser(true),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetFunc := seqSlice[0].(string)
		ctor, exists := functions[targetFunc]
		if !exists {
			return parser.Result{
				Err:       parser.NewFatalError(input, badFunctionErr(targetFunc)),
				Remaining: input,
			}
		}

		args := seqSlice[1].([]interface{})
		fn, err := ctor(args...)
		if err != nil {
			return parser.Result{
				Err:       parser.NewFatalError(input, err),
				Remaining: input,
			}
		}
		return parser.Result{
			Payload:   fn,
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------

func parseDeprecatedFunction(input []rune) parser.Result {
	var targetFunc, arg string

	for i := 0; i < len(input); i++ {
		if input[i] == ':' {
			targetFunc = string(input[:i])
			arg = string(input[i+1:])
			break
		}
	}
	if len(targetFunc) == 0 {
		targetFunc = string(input)
	}

	ftor, exists := deprecatedFunctions[targetFunc]
	if !exists {
		return parser.Result{
			Err:       parser.NewError(input),
			Remaining: input,
		}
	}
	return parser.Result{
		Payload:   wrapDeprecatedFunction(ftor(arg)),
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------
