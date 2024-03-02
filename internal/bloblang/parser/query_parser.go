package parser

import (
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
)

func queryParser(pCtx Context) Func[query.Function] {
	rootParser := parseWithTails(Expect(
		OneOf(
			matchExpressionParser(pCtx),
			ifExpressionParser(pCtx),
			lambdaExpressionParser(pCtx),
			bracketsExpressionParser(pCtx),
			literalValueParser(pCtx),
			functionParser(pCtx),
			metadataReferenceParser,
			variableReferenceParser,
			fieldReferenceRootParser(pCtx),
		),
		"query",
	), pCtx)
	return func(input []rune) Result[query.Function] {
		res := SpacesAndTabs(input)
		return arithmeticParser(rootParser)(res.Remaining)
	}
}

func tryParseQuery(expr string) (query.Function, *Error) {
	res := queryParser(Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	})([]rune(expr))
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload, nil
}
