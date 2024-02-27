package parser

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
)

func matchCaseParser(pCtx Context) Func {
	p := Sequence(
		OneOf(
			Sequence(
				Expect(
					charUnderscore,
					"match case",
				),
				Optional(SpacesAndTabs),
				Term("=>"),
			),
			Sequence(
				Expect(
					queryParser(pCtx),
					"match case",
				),
				Optional(SpacesAndTabs),
				Term("=>"),
			),
		),
		Optional(SpacesAndTabs),
		queryParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]any)

		var caseFn query.Function
		switch t := seqSlice[0].([]any)[0].(type) {
		case query.Function:
			if lit, isLiteral := t.(*query.Literal); isLiteral {
				caseFn = query.ClosureFunction("case statement", func(ctx query.FunctionContext) (any, error) {
					v := ctx.Value()
					if v == nil {
						return false, nil
					}
					return value.ICompare(*v, lit.Value), nil
				}, nil)
			} else {
				caseFn = t
			}
		case string:
			caseFn = query.NewLiteralFunction("", true)
		}

		return Success(
			query.NewMatchCase(caseFn, seqSlice[2].(query.Function)),
			res.Remaining,
		)
	}
}

func matchExpressionParser(pCtx Context) Func {
	return func(input []rune) Result {
		res := Sequence(
			Term("match"),
			SpacesAndTabs,
			Optional(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(
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
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]any)
		contextFn, _ := seqSlice[2].(query.Function)

		cases := []query.MatchCase{}
		for _, caseVal := range seqSlice[4].([]any) {
			cases = append(cases, caseVal.(query.MatchCase))
		}

		res.Payload = query.NewMatchFunction(contextFn, cases...)
		return res
	}
}

func ifExpressionParser(pCtx Context) Func {
	return func(input []rune) Result {
		ifParser := Sequence(
			Term("if"),
			SpacesAndTabs,
			MustBe(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigOpen),
			DiscardedWhitespaceNewlineComments,
			MustBe(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigClose),
		)

		elseIfParser := Optional(Sequence(
			DiscardedWhitespaceNewlineComments,
			Term("else if"),
			SpacesAndTabs,
			MustBe(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigOpen),
			DiscardedWhitespaceNewlineComments,
			MustBe(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigClose),
		))

		elseParser := Optional(Sequence(
			DiscardedWhitespaceNewlineComments,
			Term("else"),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigOpen),
			DiscardedWhitespaceNewlineComments,
			MustBe(queryParser(pCtx)),
			DiscardedWhitespaceNewlineComments,
			MustBe(charSquigClose),
		))

		res := ifParser(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]any)
		queryFn := seqSlice[2].(query.Function)
		ifFn := seqSlice[6].(query.Function)

		var elseIfs []query.ElseIf
		for {
			res = elseIfParser(res.Remaining)
			if res.Err != nil {
				return res
			}
			if res.Payload == nil {
				break
			}
			seqSlice = res.Payload.([]any)
			elseIfs = append(elseIfs, query.ElseIf{
				QueryFn: seqSlice[3].(query.Function),
				MapFn:   seqSlice[7].(query.Function),
			})
		}

		var elseFn query.Function

		res = elseParser(res.Remaining)
		if res.Err != nil {
			return res
		}
		if res.Payload != nil {
			elseFn, _ = res.Payload.([]any)[5].(query.Function)
		}

		res.Payload = query.NewIfFunction(queryFn, ifFn, elseIfs, elseFn)
		return res
	}
}

func bracketsExpressionParser(pCtx Context) Func {
	return func(input []rune) Result {
		res := Sequence(
			Expect(
				charBracketOpen,
				"function",
			),
			DiscardedWhitespaceNewlineComments,
			queryParser(pCtx),
			DiscardedWhitespaceNewlineComments,
			MustBe(Expect(charBracketClose, "closing bracket")),
		)(input)
		if res.Err != nil {
			return res
		}
		res.Payload = res.Payload.([]any)[2]
		return res
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

func lambdaExpressionParser(pCtx Context) Func {
	return func(input []rune) Result {
		res := Expect(
			Sequence(
				contextNameParser,
				SpacesAndTabs,
				Term("->"),
				SpacesAndTabs,
			),
			"function",
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]any)
		name := seqSlice[0].(string)

		if name != "_" {
			if pCtx.HasNamedContext(name) {
				return Fail(NewFatalError(input, fmt.Errorf("context label `%v` would shadow a parent context", name)), input)
			}
			if _, exists := map[string]struct{}{
				"root": {},
				"this": {},
			}[name]; exists {
				return Fail(NewFatalError(input, fmt.Errorf("context label `%v` is not allowed", name)), input)
			}
			pCtx = pCtx.WithNamedContext(name)
		}

		res = MustBe(queryParser(pCtx))(res.Remaining)
		if res.Err != nil {
			return res
		}

		queryFn := res.Payload.(query.Function)
		if chained, isChained := queryFn.(*query.NamedContextFunction); isChained {
			err := fmt.Errorf("it would be in poor taste to capture the same context under both '%v' and '%v'", name, chained.Name())
			return Fail(NewFatalError(input, err), input)
		}

		res.Payload = query.NewNamedContextFunction(name, queryFn)
		return res
	}
}
