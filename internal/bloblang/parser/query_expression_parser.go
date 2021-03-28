package parser

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func matchCaseParser(pCtx Context) Func {
	whitespace := SpacesAndTabs()

	p := Sequence(
		OneOf(
			Sequence(
				Expect(
					Char('_'),
					"match case",
				),
				Optional(whitespace),
				Term("=>"),
			),
			Sequence(
				Expect(
					queryParser(pCtx),
					"match case",
				),
				Optional(whitespace),
				Term("=>"),
			),
		),
		Optional(whitespace),
		queryParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		var caseFn query.Function
		switch t := seqSlice[0].([]interface{})[0].(type) {
		case query.Function:
			if lit, isLiteral := t.(*query.Literal); isLiteral {
				caseFn = query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
					v := ctx.Value()
					if v == nil {
						return false, nil
					}
					return *v == lit.Value, nil
				}, nil)
			} else {
				caseFn = t
			}
		case string:
			caseFn = query.NewLiteralFunction(true)
		}

		return Success(
			query.NewMatchCase(caseFn, seqSlice[2].(query.Function)),
			res.Remaining,
		)
	}
}

func matchExpressionParser(pCtx Context) Func {
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	return func(input []rune) Result {
		res := Sequence(
			Term("match"),
			SpacesAndTabs(),
			Optional(queryParser(pCtx)),
			whitespace,
			MustBe(
				DelimitedPattern(
					Sequence(
						Char('{'),
						whitespace,
					),
					matchCaseParser(pCtx),
					Sequence(
						Discard(SpacesAndTabs()),
						OneOf(
							Char(','),
							NewlineAllowComment(),
						),
						whitespace,
					),
					Sequence(
						whitespace,
						Char('}'),
					),
					true,
				),
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		contextFn, _ := seqSlice[2].(query.Function)

		cases := []query.MatchCase{}
		for _, caseVal := range seqSlice[4].([]interface{}) {
			cases = append(cases, caseVal.(query.MatchCase))
		}

		res.Payload = query.NewMatchFunction(contextFn, cases...)
		return res
	}
}

func ifExpressionParser(pCtx Context) Func {
	optionalWhitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)

	return func(input []rune) Result {
		res := Sequence(
			Term("if"),
			SpacesAndTabs(),
			MustBe(queryParser(pCtx)),
			optionalWhitespace,
			MustBe(Char('{')),
			optionalWhitespace,
			MustBe(queryParser(pCtx)),
			optionalWhitespace,
			MustBe(Char('}')),
			Optional(
				Sequence(
					optionalWhitespace,
					Term("else"),
					optionalWhitespace,
					MustBe(Char('{')),
					optionalWhitespace,
					MustBe(queryParser(pCtx)),
					optionalWhitespace,
					MustBe(Char('}')),
				),
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		queryFn := seqSlice[2].(query.Function)
		ifFn := seqSlice[6].(query.Function)

		var elseFn query.Function
		elseSlice, _ := seqSlice[9].([]interface{})
		if len(elseSlice) > 0 {
			elseFn, _ = elseSlice[5].(query.Function)
		}

		res.Payload = query.NewIfFunction(queryFn, ifFn, elseFn)
		return res
	}
}

func bracketsExpressionParser(pCtx Context) Func {
	whitespace := DiscardAll(
		OneOf(
			SpacesAndTabs(),
			NewlineAllowComment(),
		),
	)
	return func(input []rune) Result {
		res := Sequence(
			Expect(
				Char('('),
				"function",
			),
			whitespace,
			queryParser(pCtx),
			whitespace,
			MustBe(Expect(Char(')'), "closing bracket")),
		)(input)
		if res.Err != nil {
			return res
		}
		res.Payload = res.Payload.([]interface{})[2]
		return res
	}
}

func lambdaExpressionParser(pCtx Context) Func {
	contextNameParser := Expect(
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
		"context name",
	)

	return func(input []rune) Result {
		res := Expect(
			Sequence(
				contextNameParser,
				SpacesAndTabs(),
				Term("->"),
				SpacesAndTabs(),
			),
			"function",
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
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
