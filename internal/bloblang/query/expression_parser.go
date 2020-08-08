package query

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
)

func matchCaseParser() parser.Type {
	whitespace := parser.SpacesAndTabs()

	p := parser.Sequence(
		parser.OneOf(
			parser.Sequence(
				parser.Expect(
					parser.Char('_'),
					"match case",
				),
				parser.Optional(whitespace),
				parser.Term("=>"),
			),
			parser.Sequence(
				parser.Expect(
					Parse,
					"match case",
				),
				parser.Optional(whitespace),
				parser.Term("=>"),
			),
		),
		parser.Optional(whitespace),
		Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})

		var caseFn Function
		switch t := seqSlice[0].([]interface{})[0].(type) {
		case Function:
			if lit, isLiteral := t.(*literal); isLiteral {
				caseFn = closureFn(func(ctx FunctionContext) (interface{}, error) {
					if ctx.Value == nil {
						return false, nil
					}
					return *ctx.Value == lit.Value, nil
				})
			} else {
				caseFn = t
			}
		case string:
			caseFn = literalFunction(true)
		}

		return parser.Result{
			Payload: matchCase{
				caseFn:  caseFn,
				queryFn: seqSlice[2].(Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func matchExpressionParser() parser.Type {
	whitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		res := parser.Sequence(
			parser.Term("match"),
			parser.SpacesAndTabs(),
			parser.Optional(Parse),
			whitespace,
			parser.MustBe(
				parser.DelimitedPattern(
					parser.Sequence(
						parser.Char('{'),
						whitespace,
					),
					matchCaseParser(),
					parser.Sequence(
						parser.Discard(parser.SpacesAndTabs()),
						parser.OneOf(
							parser.Char(','),
							parser.NewlineAllowComment(),
						),
						whitespace,
					),
					parser.Sequence(
						whitespace,
						parser.Char('}'),
					),
					true, false,
				),
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		contextFn, ok := seqSlice[2].(Function)
		if !ok {
			contextFn = closureFn(func(ctx FunctionContext) (interface{}, error) {
				var value interface{}
				if ctx.Value != nil {
					value = *ctx.Value
				}
				return value, nil
			})
		}

		cases := []matchCase{}
		for _, caseVal := range seqSlice[4].([]interface{}) {
			cases = append(cases, caseVal.(matchCase))
		}

		res.Payload = matchFunction(contextFn, cases)
		return res
	}
}

func ifExpressionParser() parser.Type {
	optionalWhitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		res := parser.Sequence(
			parser.Term("if"),
			parser.SpacesAndTabs(),
			Parse,
			optionalWhitespace,
			parser.Char('{'),
			optionalWhitespace,
			Parse,
			optionalWhitespace,
			parser.Char('}'),
			parser.Optional(
				parser.Sequence(
					optionalWhitespace,
					parser.Term("else"),
					optionalWhitespace,
					parser.Char('{'),
					optionalWhitespace,
					Parse,
					optionalWhitespace,
					parser.Char('}'),
				),
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Payload.([]interface{})
		queryFn := seqSlice[2].(Function)
		ifFn := seqSlice[6].(Function)

		var elseFn Function
		elseSlice, _ := seqSlice[9].([]interface{})
		if len(elseSlice) > 0 {
			elseFn, _ = elseSlice[5].(Function)
		}

		res.Payload = ifFunction(queryFn, ifFn, elseFn)
		return res
	}
}

func bracketsExpressionParser() parser.Type {
	whitespace := parser.DiscardAll(
		parser.OneOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)
	return func(input []rune) parser.Result {
		res := parser.Sequence(
			parser.Expect(
				parser.Char('('),
				"function",
			),
			whitespace,
			Parse,
			whitespace,
			parser.MustBe(parser.Expect(parser.Char(')'), "closing bracket")),
		)(input)
		if res.Err != nil {
			return res
		}
		res.Payload = res.Payload.([]interface{})[2]
		return res
	}
}
