package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func matchCaseParser() Func {
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
					ParseQuery,
					"match case",
				),
				Optional(whitespace),
				Term("=>"),
			),
		),
		Optional(whitespace),
		ParseQuery,
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

func matchExpressionParser() Func {
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
			Optional(ParseQuery),
			whitespace,
			MustBe(
				DelimitedPattern(
					Sequence(
						Char('{'),
						whitespace,
					),
					matchCaseParser(),
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
					true, false,
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

func ifExpressionParser() Func {
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
			ParseQuery,
			optionalWhitespace,
			Char('{'),
			optionalWhitespace,
			ParseQuery,
			optionalWhitespace,
			Char('}'),
			Optional(
				Sequence(
					optionalWhitespace,
					Term("else"),
					optionalWhitespace,
					Char('{'),
					optionalWhitespace,
					ParseQuery,
					optionalWhitespace,
					Char('}'),
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

func bracketsExpressionParser() Func {
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
			ParseQuery,
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
