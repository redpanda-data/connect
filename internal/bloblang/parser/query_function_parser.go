package parser

import (
	"errors"
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func functionArgsParser(pCtx Context) Func[[]any] {
	return func(input []rune) Result[[]any] {
		return DelimitedPattern(
			Expect(Sequence(charBracketOpen, DiscardedWhitespaceNewlineComments), "function arguments"),
			MustBe(Expect(
				OneOf(
					FuncAsAny(namedArgParser(pCtx)),
					FuncAsAny(queryParser(pCtx)),
				), "function argument"),
			),
			MustBe(Expect(Sequence(Discard(SpacesAndTabs), charComma, DiscardedWhitespaceNewlineComments), "comma")),
			MustBe(Expect(Sequence(DiscardedWhitespaceNewlineComments, charBracketClose), "closing bracket")),
		)(input)
	}
}

type namedArg struct {
	name  string
	value any
}

func namedArgParser(pCtx Context) Func[namedArg] {
	pattern := Sequence(
		FuncAsAny(SnakeCase),
		FuncAsAny(charColon),
		FuncAsAny(Discard(SpacesAndTabs)),
		FuncAsAny(MustBe(Expect(queryParser(pCtx), "argument value"))),
	)

	return func(input []rune) Result[namedArg] {
		res := pattern(input)
		if res.Err != nil {
			return Fail[namedArg](res.Err, input)
		}

		resSlice := res.Payload
		return Success(namedArg{name: resSlice[0].(string), value: resSlice[3]}, res.Remaining)
	}
}

func parseMapExpression(fn query.Function, pCtx Context) Func[query.Function] {
	// foo.(bar | baz)
	//     ^---------^
	pattern := TakeOnly(2, Sequence(
		strToQuery(Expect(charBracketOpen, "method")),
		strToQuery(DiscardedWhitespaceNewlineComments),
		queryParser(pCtx),
		strToQuery(DiscardedWhitespaceNewlineComments),
		strToQuery(charBracketClose),
	))

	return func(input []rune) Result[query.Function] {
		res := pattern(input)
		if res.Err != nil {
			return res
		}

		method, err := query.NewMapMethod(fn, res.Payload)
		if err != nil {
			res.Err = NewFatalError(input, err)
			res.Remaining = input
		} else {
			res.Payload = method
		}
		return res
	}
}

func parseFunctionTail(fn query.Function, pCtx Context) Func[query.Function] {
	return OneOf(
		// foo.(bar | baz)
		//     ^---------^
		parseMapExpression(fn, pCtx),
		// foo.bar()
		//     ^---^
		methodParser(fn, pCtx),
		// foo.bar
		//     ^-^
		fieldReferenceParser(fn),
	)
}

func parseWithTails(fnParser Func[query.Function], pCtx Context) Func[query.Function] {
	delimPattern := Sequence(charDot, DiscardedWhitespaceNewlineComments)

	mightNot := Sequence(
		FuncAsAny(Optional(TakeOnly(0, Sequence(
			Char('!'),
			Discard(SpacesAndTabs),
		)))),
		FuncAsAny(fnParser),
	)

	return func(input []rune) Result[query.Function] {
		var seq []any
		var remaining []rune

		if res := mightNot(input); res.Err != nil {
			return Fail[query.Function](res.Err, input)
		} else {
			seq = res.Payload
			remaining = res.Remaining
		}

		isNot := seq[0].(string) == "!"
		fn := seq[1].(query.Function)
		for {
			if res := delimPattern(remaining); res.Err != nil {
				if isNot {
					fn = query.Not(fn)
				}
				return Success(fn, res.Remaining)
			} else {
				remaining = res.Remaining
			}

			if res := MustBe(parseFunctionTail(fn, pCtx))(remaining); res.Err != nil {
				return Fail[query.Function](res.Err, input)
			} else {
				fn = res.Payload
				remaining = res.Remaining
			}
		}
	}
}

func quotedPathSegmentParser(input []rune) Result[string] {
	res := QuotedString(input)
	if res.Err != nil {
		return res
	}

	// Convert into a JSON pointer style path string.
	rawSegment := strings.ReplaceAll(res.Payload, "~", "~0")
	rawSegment = strings.ReplaceAll(rawSegment, ".", "~1")

	return Success(rawSegment, res.Remaining)
}

var fieldReferencePattern = Expect(
	OneOf(
		JoinStringPayloads(
			UntilFail(
				OneOf(
					InRange('a', 'z'),
					InRange('A', 'Z'),
					InRange('0', '9'),
					charUnderscore,
				),
			),
		),
		quotedPathSegmentParser,
	),
	"field path",
)

func fieldReferenceParser(ctxFn query.Function) Func[query.Function] {
	return func(input []rune) Result[query.Function] {
		res := fieldReferencePattern(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		fn, err := query.NewGetMethod(ctxFn, res.Payload)
		if err != nil {
			return Fail[query.Function](NewFatalError(input, err), input)
		}

		return Success(fn, res.Remaining)
	}
}

var variableReferencePattern = Expect(
	Sequence(
		charDollar,
		JoinStringPayloads(
			UntilFail(
				OneOf(
					InRange('a', 'z'),
					InRange('A', 'Z'),
					InRange('0', '9'),
					charUnderscore,
				),
			),
		),
	),
	"variable path",
)

func variableReferenceParser(input []rune) Result[query.Function] {
	res := variableReferencePattern(input)
	if res.Err != nil {
		return Fail[query.Function](res.Err, input)
	}
	return Success(query.NewVarFunction(res.Payload[1]), res.Remaining)
}

var metadataReferencePattern = Expect(
	Sequence(
		Char('@'),
		Optional(OneOf(
			JoinStringPayloads(
				UntilFail(
					OneOf(
						InRange('a', 'z'),
						InRange('A', 'Z'),
						InRange('0', '9'),
						charUnderscore,
					),
				),
			),
			QuotedString,
		)),
	),
	"metadata path",
)

func metadataReferenceParser(input []rune) Result[query.Function] {
	res := metadataReferencePattern(input)
	if res.Err != nil {
		return Fail[query.Function](res.Err, input)
	}
	return Success(query.NewMetaFunction(res.Payload[1]), res.Remaining)
}

var fieldReferenceRootPattern = Expect(
	JoinStringPayloads(
		UntilFail(
			OneOf(
				InRange('a', 'z'),
				InRange('A', 'Z'),
				InRange('0', '9'),
				charUnderscore,
			),
		),
	),
	"field path",
)

func fieldReferenceRootParser(pCtx Context) Func[query.Function] {
	return func(input []rune) Result[query.Function] {
		res := fieldReferenceRootPattern(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		var fn query.Function

		path := res.Payload
		if path == "this" {
			fn = query.NewFieldFunction("")
		} else if path == "root" {
			fn = query.NewRootFieldFunction("")
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

func extractArgsParserResult(paramsDef query.Params, args []any) (*query.ParsedParams, error) {
	var namelessArgs []any
	var namedArgs map[string]any

	for _, arg := range args {
		namedArg, isNamed := arg.(namedArg)
		if isNamed {
			if namedArgs == nil {
				namedArgs = map[string]any{}
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

func methodParser(fn query.Function, pCtx Context) Func[query.Function] {
	p := Sequence(FuncAsAny(Expect(SnakeCase, "method")), FuncAsAny(functionArgsParser(pCtx)))

	return func(input []rune) Result[query.Function] {
		res := p(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		seqSlice := res.Payload

		targetMethod := seqSlice[0].(string)
		params, err := pCtx.Methods.Params(targetMethod)
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}

		parsedParams, err := extractArgsParserResult(params, seqSlice[1].([]any))
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}

		method, err := pCtx.InitMethod(targetMethod, fn, parsedParams)
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}
		return Success(method, res.Remaining)
	}
}

func functionParser(pCtx Context) Func[query.Function] {
	p := Sequence(FuncAsAny(Expect(SnakeCase, "function")), FuncAsAny(functionArgsParser(pCtx)))

	return func(input []rune) Result[query.Function] {
		res := p(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		seqSlice := res.Payload

		targetFunc := seqSlice[0].(string)
		params, err := pCtx.Functions.Params(targetFunc)
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}

		parsedParams, err := extractArgsParserResult(params, seqSlice[1].([]any))
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}

		fn, err := pCtx.InitFunction(targetFunc, parsedParams)
		if err != nil {
			return Fail[query.Function](NewFatalError(res.Remaining, err), input)
		}
		return Success(fn, res.Remaining)
	}
}
