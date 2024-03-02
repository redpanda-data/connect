package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func dynamicArrayParser(pCtx Context) Func[any] {
	return func(input []rune) Result[any] {
		res := DelimitedPattern(
			Expect(Sequence(
				charSquareOpen,
				DiscardedWhitespaceNewlineComments,
			), "array"),
			Expect(queryParser(pCtx), "object"),
			Sequence(
				Discard(SpacesAndTabs),
				charComma,
				DiscardedWhitespaceNewlineComments,
			),
			Sequence(
				DiscardedWhitespaceNewlineComments,
				charSquareClose,
			),
		)(input)
		if res.Err != nil {
			return Fail[any](res.Err, input)
		}
		return Success[any](query.NewArrayLiteral(res.Payload...), res.Remaining)
	}
}

func dynamicObjectParser(pCtx Context) Func[any] {
	return func(input []rune) Result[any] {
		res := DelimitedPattern(
			Expect(Sequence(
				charSquigOpen,
				DiscardedWhitespaceNewlineComments,
			), "object"),
			Sequence(
				OneOf(
					FuncAsAny(QuotedString),
					FuncAsAny(Expect(queryParser(pCtx), "object")),
				),
				FuncAsAny(Discard(SpacesAndTabs)),
				FuncAsAny(charColon),
				FuncAsAny(DiscardedWhitespaceNewlineComments),
				FuncAsAny(Expect(queryParser(pCtx), "object")),
			),
			Sequence(
				Discard(SpacesAndTabs),
				charComma,
				DiscardedWhitespaceNewlineComments,
			),
			Sequence(
				DiscardedWhitespaceNewlineComments,
				charSquigClose,
			),
		)(input)
		if res.Err != nil {
			return Fail[any](res.Err, input)
		}

		values := [][2]any{}

		for _, sequenceValue := range res.Payload {
			values = append(values, [2]any{sequenceValue[0], sequenceValue[4]})
		}

		lit, err := query.NewMapLiteral(values)
		if err != nil {
			return Fail[any](NewFatalError(input, err), input)
		}

		return Success(lit, res.Remaining)
	}
}

func literalValueParser(pCtx Context) Func[query.Function] {
	p := OneOf(
		FuncAsAny(Boolean),
		FuncAsAny(Number),
		FuncAsAny(TripleQuoteString),
		FuncAsAny(QuotedString),
		FuncAsAny(Null),
		FuncAsAny(dynamicArrayParser(pCtx)),
		FuncAsAny(dynamicObjectParser(pCtx)),
	)

	return func(input []rune) Result[query.Function] {
		res := p(input)
		if res.Err != nil {
			return Fail[query.Function](res.Err, res.Remaining)
		}

		if f, isFunction := res.Payload.(query.Function); isFunction {
			return Success(f, res.Remaining)
		}

		return Success[query.Function](query.NewLiteralFunction("", res.Payload), res.Remaining)
	}
}
