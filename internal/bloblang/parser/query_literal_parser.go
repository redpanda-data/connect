package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func dynamicArrayParser(pCtx Context) Func {
	begin, comma, end := Char('['), Char(','), Char(']')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)
	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				begin,
				whitespace,
			), "array"),
			Expect(queryParser(pCtx), "object"),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				end,
			),
			true,
		)(input)
		if res.Err != nil {
			return res
		}

		res.Payload = query.NewArrayLiteral(res.Payload.([]any)...)
		return res
	}
}

func dynamicObjectParser(pCtx Context) Func {
	begin, comma, end := Char('{'), Char(','), Char('}')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)

	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				begin,
				whitespace,
			), "object"),
			Sequence(
				OneOf(
					QuotedString(),
					Expect(queryParser(pCtx), "object"),
				),
				Discard(SpacesAndTabs()),
				Char(':'),
				Discard(whitespace),
				Expect(queryParser(pCtx), "object"),
			),
			Sequence(
				Discard(SpacesAndTabs()),
				comma,
				whitespace,
			),
			Sequence(
				whitespace,
				end,
			),
			true,
		)(input)
		if res.Err != nil {
			return res
		}

		values := [][2]any{}

		for _, sequenceValue := range res.Payload.([]any) {
			slice := sequenceValue.([]any)
			values = append(values, [2]any{slice[0], slice[4]})
		}

		lit, err := query.NewMapLiteral(values)
		if err != nil {
			res.Err = NewFatalError(input, err)
			res.Remaining = input
		} else {
			res.Payload = lit
		}
		return res
	}
}

func literalValueParser(pCtx Context) Func {
	p := OneOf(
		Boolean(),
		Number(),
		TripleQuoteString(),
		QuotedString(),
		Null(),
		dynamicArrayParser(pCtx),
		dynamicObjectParser(pCtx),
	)

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		if _, isFunction := res.Payload.(query.Function); isFunction {
			return res
		}

		res.Payload = query.NewLiteralFunction("", res.Payload)
		return res
	}
}
