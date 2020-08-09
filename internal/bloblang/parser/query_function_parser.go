package parser

import (
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

func functionArgsParser(allowFunctions bool) Type {
	open, comma, close := Char('('), Char(','), Char(')')
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	paramTypes := []Type{
		parseLiteralWithTails(Boolean()),
		parseLiteralWithTails(Number()),
		parseLiteralWithTails(QuotedString()),
	}

	return func(input []rune) Result {
		tmpParamTypes := paramTypes
		if allowFunctions {
			tmpParamTypes = append([]Type{}, paramTypes...)
			tmpParamTypes = append(tmpParamTypes, ParseQuery)
		}
		return DelimitedPattern(
			Expect(
				Sequence(
					open,
					whitespace,
				),
				"function arguments",
			),
			Expect(
				MustBe(OneOf(tmpParamTypes...)),
				"function argument",
			),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				close,
			),
			false, false,
		)(input)
	}
}

func parseFunctionTail(fn query.Function) Type {
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
				ParseQuery,
				whitespace,
				closeBracket,
			),
			methodParser(fn),
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

func parseLiteralWithTails(litParser Type) Type {
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
				return Result{
					Payload:   payload,
					Remaining: res.Remaining,
				}
			}
			if fn == nil {
				fn = query.NewLiteralFunction(lit)
			}
			if res = MustBe(parseFunctionTail(fn))(res.Remaining); res.Err != nil {
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			fn = res.Payload.(query.Function)
		}
	}
}

func parseWithTails(fnParser Type) Type {
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
				return Result{
					Payload:   fn,
					Remaining: res.Remaining,
				}
			}
			if res = MustBe(parseFunctionTail(fn))(res.Remaining); res.Err != nil {
				return Result{
					Err:       res.Err,
					Remaining: input,
				}
			}
			fn = res.Payload.(query.Function)
		}
	}
}

func quotedPathSegmentParser() Type {
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

		return Result{
			Payload:   rawSegment,
			Remaining: res.Remaining,
		}
	}
}

func fieldLiteralMapParser(ctxFn query.Function) Type {
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
			return Result{
				Remaining: input,
				Err:       NewFatalError(input, err),
			}
		}

		return Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func variableLiteralParser() Type {
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

		return Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func fieldLiteralRootParser() Type {
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
			return Result{
				Remaining: input,
				Err:       NewFatalError(input, err),
			}
		}

		return Result{
			Remaining: res.Remaining,
			Payload:   fn,
		}
	}
}

func methodParser(fn query.Function) Type {
	p := Sequence(
		Expect(
			SnakeCase(),
			"method",
		),
		functionArgsParser(true),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetMethod := seqSlice[0].(string)
		args := seqSlice[1].([]interface{})

		method, err := query.InitMethod(targetMethod, fn, args...)
		if err != nil {
			return Result{
				Err:       NewFatalError(input, err),
				Remaining: input,
			}
		}
		return Result{
			Payload:   method,
			Remaining: res.Remaining,
		}
	}
}

func functionParser() Type {
	p := Sequence(
		Expect(
			SnakeCase(),
			"function",
		),
		functionArgsParser(true),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		targetFunc := seqSlice[0].(string)
		args := seqSlice[1].([]interface{})

		fn, err := query.InitFunction(targetFunc, args...)
		if err != nil {
			return Result{
				Err:       NewFatalError(input, err),
				Remaining: input,
			}
		}
		return Result{
			Payload:   fn,
			Remaining: res.Remaining,
		}
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
		return Result{
			Err:       NewError(input),
			Remaining: input,
		}
	}
	return Result{
		Payload:   fn,
		Err:       nil,
		Remaining: nil,
	}
}

//------------------------------------------------------------------------------
