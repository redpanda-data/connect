package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func dynamicArrayParser() Func {
	open, comma, close := Char('['), Char(','), Char(']')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)
	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				open,
				whitespace,
			), "array"),
			OneOf(
				dynamicLiteralValueParser(),
				Expect(ParseQuery, "object"),
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
		if res.Err != nil {
			return res
		}

		res.Payload = query.NewArrayLiteral(res.Payload.([]interface{})...)
		return res
	}
}

func dynamicObjectParser() Func {
	open, comma, close := Char('{'), Char(','), Char('}')
	whitespace := DiscardAll(
		OneOf(
			NewlineAllowComment(),
			SpacesAndTabs(),
		),
	)

	return func(input []rune) Result {
		res := DelimitedPattern(
			Expect(Sequence(
				open,
				whitespace,
			), "object"),
			Sequence(
				OneOf(
					QuotedString(),
					Expect(ParseQuery, "object"),
				),
				Discard(SpacesAndTabs()),
				Char(':'),
				Discard(whitespace),
				OneOf(
					dynamicLiteralValueParser(),
					Expect(ParseQuery, "object"),
				),
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
		if res.Err != nil {
			return res
		}

		values := [][2]interface{}{}

		for _, sequenceValue := range res.Payload.([]interface{}) {
			slice := sequenceValue.([]interface{})
			values = append(values, [2]interface{}{slice[0], slice[4]})
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

func dynamicLiteralValueParser() Func {
	return OneOf(
		Boolean(),
		Number(),
		TripleQuoteString(),
		QuotedString(),
		Null(),
		dynamicArrayParser(),
		dynamicObjectParser(),
	)
}

func literalValueParser() Func {
	p := dynamicLiteralValueParser()

	return func(input []rune) Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		if _, isFunction := res.Payload.(query.Function); isFunction {
			return res
		}

		res.Payload = query.NewLiteralFunction(res.Payload)
		return res
	}
}
