package query

import (
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
)

func matchCaseParser() parser.Type {
	whitespace := parser.SpacesAndTabs()
	p := parser.Sequence(
		parser.InterceptExpectedError(
			parser.AnyOf(
				parser.Match("_ "),
				createParser(false),
			),
			"match-case",
		),
		parser.Optional(whitespace),
		parser.Match("=> "),
		parser.Optional(whitespace),
		createParser(false),
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Result.([]interface{})

		var caseFn Function
		switch t := seqSlice[0].(type) {
		case Function:
			caseFn = t
		case string:
			caseFn = literalFunction(true)
		}

		return parser.Result{
			Result: matchCase{
				caseFn:  caseFn,
				queryFn: seqSlice[4].(Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func matchExpressionParser() parser.Type {
	whitespace := parser.DiscardAll(
		parser.AnyOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		res := parser.Sequence(
			parser.Match("match"),
			parser.Discard(parser.SpacesAndTabs()),
			parser.Optional(createParser(false)),
			whitespace,
			parser.DelimitedPattern(
				parser.Sequence(
					parser.Char('{'),
					whitespace,
				),
				matchCaseParser(),
				parser.Sequence(
					parser.Discard(parser.SpacesAndTabs()),
					parser.NewlineAllowComment(),
					whitespace,
				),
				parser.Sequence(
					whitespace,
					parser.Char('}'),
				),
				true,
			),
		)(input)
		if res.Err != nil {
			return res
		}

		seqSlice := res.Result.([]interface{})
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

		res.Result = matchFunction(contextFn, cases)
		return res
	}
}
