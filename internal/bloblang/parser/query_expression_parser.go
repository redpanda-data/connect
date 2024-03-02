package parser

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

func matchCaseParser(pCtx Context) Func[query.MatchCase] {
	ignoreToFn := ZeroedFuncAs[string, query.Function]

	p := Sequence(
		OneOf(
			ZeroedFuncAs[[]string, query.Function](Sequence(
				Expect(
					charUnderscore,
					"match case",
				),
				Optional(SpacesAndTabs),
				Term("=>"),
			)),
			TakeOnly(0, Sequence(
				Expect(
					queryParser(pCtx),
					"match case",
				),
				ignoreToFn(Optional(SpacesAndTabs)),
				ignoreToFn(Term("=>")),
			)),
		),
		ignoreToFn(Optional(SpacesAndTabs)),
		queryParser(pCtx),
	)

	return func(input []rune) Result[query.MatchCase] {
		res := p(input)
		if res.Err != nil {
			return Fail[query.MatchCase](res.Err, input)
		}

		var caseFn query.Function

		if p := res.Payload[0]; p == nil {
			caseFn = query.NewLiteralFunction("", true)
		} else if lit, isLiteral := p.(*query.Literal); isLiteral {
			caseFn = query.ClosureFunction("case statement", func(ctx query.FunctionContext) (any, error) {
				v := ctx.Value()
				if v == nil {
					return false, nil
				}
				return value.ICompare(*v, lit.Value), nil
			}, nil)
		} else {
			caseFn = p
		}

		return Success(
			query.NewMatchCase(caseFn, res.Payload[2]),
			res.Remaining,
		)
	}
}

func matchExpressionParser(pCtx Context) Func[query.Function] {
	return func(input []rune) Result[query.Function] {
		res := Sequence(
			FuncAsAny(Term("match")),
			FuncAsAny(SpacesAndTabs),
			FuncAsAny(Optional(queryParser(pCtx))),
			FuncAsAny(DiscardedWhitespaceNewlineComments),
			FuncAsAny(MustBe(
				DelimitedPattern(
					Sequence(
						charSquigOpen,
						DiscardedWhitespaceNewlineComments,
					),
					matchCaseParser(pCtx),
					Sequence(
						Discard(SpacesAndTabs),
						OneOf(
							charComma,
							NewlineAllowComment,
						),
						DiscardedWhitespaceNewlineComments,
					),
					Sequence(
						DiscardedWhitespaceNewlineComments,
						charSquigClose,
					),
				),
			)),
		)(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		seqSlice := res.Payload
		contextFn, _ := seqSlice[2].(query.Function)

		cases := seqSlice[4].([]query.MatchCase)

		return Success(query.NewMatchFunction(contextFn, cases...), res.Remaining)
	}
}

var strToQuery = ZeroedFuncAs[string, query.Function]

func ifExpressionParser(pCtx Context) Func[query.Function] {
	return func(input []rune) Result[query.Function] {
		ifParser := Sequence(
			strToQuery(Term("if")),
			strToQuery(SpacesAndTabs),
			MustBe(queryParser(pCtx)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigOpen)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			MustBe(queryParser(pCtx)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigClose)),
		)

		elseIfParser := Optional(Sequence(
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(Term("else if")),
			strToQuery(SpacesAndTabs),
			MustBe(queryParser(pCtx)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigOpen)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			MustBe(queryParser(pCtx)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigClose)),
		))

		elseParser := Optional(Sequence(
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(Term("else")),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigOpen)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			MustBe(queryParser(pCtx)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(charSquigClose)),
		))

		res := ifParser(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		seqSlice := res.Payload
		queryFn := seqSlice[2]
		ifFn := seqSlice[6]

		var elseIfs []query.ElseIf
		for {
			res = elseIfParser(res.Remaining)
			if res.Err != nil {
				return Fail[query.Function](res.Err, input)
			}
			if res.Payload == nil {
				break
			}
			seqSlice = res.Payload
			elseIfs = append(elseIfs, query.ElseIf{
				QueryFn: seqSlice[3],
				MapFn:   seqSlice[7],
			})
		}

		var elseFn query.Function

		res = elseParser(res.Remaining)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}
		if res.Payload != nil {
			elseFn = res.Payload[5]
		}

		return Success(query.NewIfFunction(queryFn, ifFn, elseIfs, elseFn), res.Remaining)
	}
}

func bracketsExpressionParser(pCtx Context) Func[query.Function] {
	return func(input []rune) Result[query.Function] {
		res := Sequence(
			strToQuery(Expect(
				charBracketOpen,
				"function",
			)),
			strToQuery(DiscardedWhitespaceNewlineComments),
			queryParser(pCtx),
			strToQuery(DiscardedWhitespaceNewlineComments),
			strToQuery(MustBe(Expect(charBracketClose, "closing bracket"))),
		)(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}
		return Success(res.Payload[2], res.Remaining)
	}
}

var contextNameParser = Expect(
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
	"context name",
)

func lambdaExpressionParser(pCtx Context) Func[query.Function] {
	nameParser := Expect(
		TakeOnly(0, Sequence(
			contextNameParser,
			SpacesAndTabs,
			Term("->"),
			SpacesAndTabs,
		)),
		"function",
	)

	return func(input []rune) Result[query.Function] {
		res := nameParser(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, input)
		}

		name := res.Payload
		if name != "_" {
			if pCtx.HasNamedContext(name) {
				return Fail[query.Function](
					NewFatalError(input, fmt.Errorf("context label `%v` would shadow a parent context", name)),
					input,
				)
			}
			if _, exists := map[string]struct{}{
				"root": {},
				"this": {},
			}[name]; exists {
				return Fail[query.Function](NewFatalError(input, fmt.Errorf("context label `%v` is not allowed", name)), input)
			}
			pCtx = pCtx.WithNamedContext(name)
		}

		queryRes := MustBe(queryParser(pCtx))(res.Remaining)
		if queryRes.Err != nil {
			return queryRes
		}

		queryFn := queryRes.Payload
		if chained, isChained := queryFn.(*query.NamedContextFunction); isChained {
			err := fmt.Errorf("it would be in poor taste to capture the same context under both '%v' and '%v'", name, chained.Name())
			return Fail[query.Function](NewFatalError(input, err), input)
		}
		return Success(query.NewNamedContextFunction(name, queryFn), queryRes.Remaining)
	}
}
