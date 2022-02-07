package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func functionArgsParser(pCtx Context) Func {
	begin, comma, end := Char('('), Char(','), Char(')')
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	return func(input []rune) Result {
		return DelimitedPattern(
			Expect(Sequence(begin, whitespace), "function arguments"),
			MustBe(Expect(
				OneOf(
					namedArgParser(pCtx),
					queryParser(pCtx),
				), "function argument"),
			),
			MustBe(Expect(Sequence(Discard(SpacesAndTabs()), comma, whitespace), "comma")),
			MustBe(Expect(Sequence(whitespace, end), "closing bracket")),
			true,
		)(input)
	}
}

type namedArg struct {
	name  string
	value interface{}
}

func namedArgParser(pCtx Context) Func {
	colon := Char(':')
	whitespace := DiscardAll(SpacesAndTabs())

	pattern := Sequence(
		SnakeCase(),
		colon,
		whitespace,
		MustBe(Expect(queryParser(pCtx), "argument value")),
	)

	return func(input []rune) Result {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		resSlice := res.Payload.([]interface{})
		res.Payload = namedArg{name: resSlice[0].(string), value: resSlice[3]}
		return res
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
		rawSegment = strings.ReplaceAll(rawSegment, "~", "~0")
		rawSegment = strings.ReplaceAll(rawSegment, ".", "~1")

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
						Char('_'),
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

var errNoRoot = errors.New("unable to reference the `root` of your mapped document within a query. This feature will be introduced soon, but in the meantime in order to use a mapped value multiple times use variables (https://www.benthos.dev/docs/guides/bloblang/about#variables). If instead you wish to refer to a field `root` from your input document use `this.root`")

func fieldLiteralRootParser(pCtx Context) Func {
	fieldPathParser := Expect(
		JoinStringPayloads(
			UntilFail(
				OneOf(
					InRange('a', 'z'),
					InRange('A', 'Z'),
					InRange('0', '9'),
					Char('_'),
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

		path := res.Payload.(string)
		if path == "this" {
			fn = query.NewFieldFunction("")
		} else if path == "root" {
			return Fail(NewFatalError(input, errNoRoot), input)
		} else {
			if pCtx.HasNamedContext(path) {
				fn = query.NewNamedContextFieldFunction(path, "")
			} else {
				fn = query.NewFieldFunction(path)
			}
		}

		return Success(fn, res.Remaining)
	}
}

func extractArgsParserResult(paramsDef query.Params, args []interface{}) (*query.ParsedParams, error) {
	var namelessArgs []interface{}
	var namedArgs map[string]interface{}

	for _, arg := range args {
		namedArg, isNamed := arg.(namedArg)
		if isNamed {
			if namedArgs == nil {
				namedArgs = map[string]interface{}{}
			}
			if _, exists := namedArgs[namedArg.name]; exists {
				return nil, fmt.Errorf("duplicate named arg: %v", namedArg.name)
			}
			namedArgs[namedArg.name] = namedArg.value
		} else {
			namelessArgs = append(namelessArgs, arg)
		}
	}

	if len(namelessArgs) > 0 && len(namedArgs) > 0 {
		return nil, errors.New("cannot mix named and nameless arguments")
	}

	var parsedParams *query.ParsedParams
	var err error
	if len(namedArgs) > 0 {
		parsedParams, err = paramsDef.PopulateNamed(namedArgs)
	} else {
		parsedParams, err = paramsDef.PopulateNameless(namelessArgs...)
	}
	if err != nil {
		return nil, err
	}

	return parsedParams, nil
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
		params, err := pCtx.Methods.Params(targetMethod)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		parsedParams, err := extractArgsParserResult(params, seqSlice[1].([]interface{}))
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		method, err := pCtx.InitMethod(targetMethod, fn, parsedParams)
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
		params, err := pCtx.Functions.Params(targetFunc)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		parsedParams, err := extractArgsParserResult(params, seqSlice[1].([]interface{}))
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}

		fn, err := pCtx.InitFunction(targetFunc, parsedParams)
		if err != nil {
			return Fail(NewFatalError(input, err), input)
		}
		return Success(fn, res.Remaining)
	}
}
