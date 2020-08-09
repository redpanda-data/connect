package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func dynamicArrayParser() Type {
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

		isDynamic := false
		values := res.Payload.([]interface{})
		for _, v := range values {
			if _, isFunction := v.(query.Function); isFunction {
				isDynamic = true
			}
		}
		if !isDynamic {
			return res
		}

		res.Payload = query.ClosureFunction(func(ctx query.FunctionContext) (interface{}, error) {
			dynArray := make([]interface{}, len(values))
			var err error
			for i, v := range values {
				if fn, isFunction := v.(query.Function); isFunction {
					fnRes, fnErr := fn.Exec(ctx)
					if fnErr != nil {
						if recovered, ok := fnErr.(*query.ErrRecoverable); ok {
							dynArray[i] = recovered.Recovered
							err = fnErr
						}
						return nil, fnErr
					}
					dynArray[i] = fnRes
				} else {
					dynArray[i] = v
				}
			}
			if err != nil {
				return nil, &query.ErrRecoverable{
					Recovered: dynArray,
					Err:       err,
				}
			}
			return dynArray, nil
		})
		return res
	}
}

func dynamicObjectParser() Type {
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

func dynamicLiteralValueParser() Type {
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

func literalValueParser() Type {
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
