package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

func queryParser(pCtx Context) func(input []rune) Result {
	rootParser := parseWithTails(Expect(
		OneOf(
			matchExpressionParser(pCtx),
			ifExpressionParser(pCtx),
			lambdaExpressionParser(pCtx),
			bracketsExpressionParser(pCtx),
			literalValueParser(pCtx),
			functionParser(pCtx),
			variableLiteralParser(),
			fieldLiteralRootParser(pCtx),
		),
		"query",
	), pCtx)
	return func(input []rune) Result {
		res := SpacesAndTabs()(input)
		return arithmeticParser(rootParser)(res.Remaining)
	}
}

// ParseDeprecatedQuery parses an input into a query.Function, but permits
// deprecated function interpolations. In order to support old functions this
// parser does not include field literals.
//
// TODO: V4 Remove this
func ParseDeprecatedQuery(pCtx Context, isDeprecated *bool) Func {
	return func(input []rune) Result {
		rootParser := OneOf(
			matchExpressionParser(pCtx),
			ifExpressionParser(pCtx),
			parseWithTails(bracketsExpressionParser(pCtx), pCtx),
			parseWithTails(literalValueParser(pCtx), pCtx),
			parseWithTails(functionParser(pCtx), pCtx),
			parseDeprecatedFunction(isDeprecated),
		)

		res := SpacesAndTabs()(input)

		res = arithmeticParser(rootParser)(res.Remaining)
		if res.Err != nil {
			return Fail(res.Err, input)
		}

		result := res.Payload
		res = SpacesAndTabs()(res.Remaining)
		return Success(result, res.Remaining)
	}
}

func tryParseQuery(expr string, deprecated bool) (query.Function, *Error) {
	pCtx := Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	}
	var res Result
	if deprecated {
		var isDep bool
		res = ParseDeprecatedQuery(pCtx, &isDep)([]rune(expr))
	} else {
		res = queryParser(Context{
			Functions: query.AllFunctions,
			Methods:   query.AllMethods,
		})([]rune(expr))
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(query.Function), nil
}
