package parser

import (
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func functionArgsParser(pCtx Context) Func {
	open, comma, close := Char('('), Char(','), Char(')')
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	return func(input []rune) Result {
		return DelimitedPattern(
			Expect(Sequence(open, whitespace), "function arguments"),
			MustBe(Expect(queryParser(pCtx), "function argument")),
			MustBe(Expect(Sequence(Discard(SpacesAndTabs()), comma, whitespace), "comma")),
			MustBe(Expect(Sequence(whitespace, close), "closing bracket")),
			true,
		)(input)
	}
}

func parseFunctionTail(fn query.Function, pCtx Context) Func {
	openBracket := Char('(')
	closeBracket := Char(')')

	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	return func(input []rune) Result {
		res := OneOf(
			Sequence(
				Expect(openBracket, "method"),
				whitespace,
				queryParser(pCtx),
				whitespace,
				closeBracket,
			),
			methodParser(fn, pCtx),
			fieldLiteralMapParser(fn),
		)(input)
		if seqSlice, isSlice := res.Payload.([]interface{}); isSlice {
			method, err := query.NewMapMethod(fn, seqSlice[2].(query.Function))
			if err != nil {
				res.Err = NewFatalError(input, err)
				res.Remaining = input
			} else {
				res.Payload = method
			}
		}
		return res
	}
}

func parseLiteralWithTails(litParser Func, pCtx Context) Func {
	delim := Sequence(
		Char('.'),
		Discard(
			Sequence(
				NewlineAllowComment(),
				SpacesAndTabs(),
			),
		),
	)

	return func(input []rune) Result {
		res := litParser(input)
		if res.Err != nil {
			return res
		}

		lit := res.Payload
		var fn query.Function
		for {
			if res = delim(res.Remaining); res.Err != nil {
				var payload interface{} = lit
				if fn != nil {
					payload = fn
				}
				return Success(payload, res.Remaining)
			}
			if fn == nil {
				fn = query.NewLiteralFunction(lit)
			}
			if res = MustBe(parseFunctionTail(fn, pCtx))(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}
			fn = res.Payload.(query.Function)
		}
	}
}

func parseWithTails(fnParser Func, pCtx Context) Func {
	delim := Sequence(
		Char('.'),
		Discard(
			Sequence(
				NewlineAllowComment(),
				SpacesAndTabs(),
			),
		),
	)

	mightNot := Sequence(
		Optional(Sequence(
			Char('!'),
			Discard(SpacesAndTabs()),
		)),
		fnParser,
	)

	return func(input []rune) Result {
		res := mightNot(input)
		if res.Err != nil {
			return res
		}

		seq := res.Payload.([]interface{})
		isNot := seq[0] != nil
		fn := seq[1].(query.Function)
		for {
			if res = delim(res.Remaining); res.Err != nil {
				if isNot {
					fn = query.Not(fn)
				}
				return Success(fn, res.Remaining)
			}
			if res = MustBe(parseFunctionTail(fn, pCtx))(res.Remaining); res.Err != nil {
				return Fail(res.Err, input)
			}
			fn = res.Payload.(query.Function)
		}
	}
}

func quotedPathSegmentParser() Func {
	pattern := QuotedString()

	return func(input []rune) Result {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		rawSegment, _ := res.Payload.(string)

		// Convert into a JSON pointer style path string.
		rawSegment = strings.Replace(rawSegment, "~", "~0", -1)
		rawSegment = strings.Replace(rawSegment, ".", "~1", -1)

		return Success(rawSegment, res.Remaining)
	}
}

func fieldLiteralMapParser(ctxFn query.Function) Func {
	fieldPathParser := Expect(
		OneOf(
			JoinStringPayloads(
				UntilFail(
					OneOf(
						InRange('a', 'z'),
						InRange('A', 'Z'),
						InRange('0', '9'),
						InRange('*', '+'),
						Char('_'),
						Char('-'),
						Char('~'),
					),
				),
			),
			quotedPathSegmentParser(),
		),
		"field path",
	)

	return func(input []rune) Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		fn, err := query.NewGetMethod(ctxFn, res.Payload.(string))
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		return Success(fn, res.Remaining)
	}
}

func variableLiteralParser() Func {
	varPathParser := Expect(
		Sequence(
			Char('$'),
			JoinStringPayloads(
				UntilFail(
					OneOf(
						InRange('a', 'z'),
						InRange('A', 'Z'),
						InRange('0', '9'),
						Char('_'),
						Char('-'),
					),
				),
			),
		),
		"variable path",
	)

	return func(input []rune) Result {
		res := varPathParser(input)
		if res.Err != nil {
			return res
		}

		path := res.Payload.([]interface{})[1].(string)
		fn := query.NewVarFunction(path)

		return Success(fn, res.Remaining)
	}
}

func fieldLiteralRootParser() Func {
	fieldPathParser := Expect(
		JoinStringPayloads(
			UntilFail(
				OneOf(
					InRange('a', 'z'),
					InRange('A', 'Z'),
					InRange('0', '9'),
					InRange('*', '+'),
					Char('_'),
					Char('~'),
				),
			),
		),
		"field path",
	)

	return func(input []rune) Result {
		res := fieldPathParser(input)
		if res.Err != nil {
			return res
		}

		var fn query.Function
		var err error

		path := res.Payload.(string)
		if path == "this" {
			fn = query.NewFieldFunction("")
		} else {
			fn = query.NewFieldFunction(path)
		}
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		return Success(fn, res.Remaining)
	}
}

func methodParser(fn query.Function, pCtx Context) Func {
	p := Sequence(
		Expect(
			SnakeCase(),
			"method",
		),
		functionArgsParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetMethod := seqSlice[0].(string)
		args := seqSlice[1].([]interface{})

		method, err := pCtx.InitMethod(targetMethod, fn, args...)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}
		return Success(method, res.Remaining)
	}
}

func functionParser(pCtx Context) Func {
	p := Sequence(
		Expect(
			SnakeCase(),
			"function",
		),
		functionArgsParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetFunc := seqSlice[0].(string)
		args := seqSlice[1].([]interface{})

		fn, err := pCtx.InitFunction(targetFunc, args...)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}
		return Success(fn, res.Remaining)
	}
}

//------------------------------------------------------------------------------

func parseDeprecatedFunction(input []rune) Result {
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

	fn, exists := query.DeprecatedFunction(targetFunc, arg)
	if !exists {
		return Fail(NewError(input), input)
	}
	return Success(fn, nil)
}

//------------------------------------------------------------------------------
