package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

//------------------------------------------------------------------------------

// NewQuery parses a new query function from a query string.
func NewQuery(queryStr string) (query.Function, *Error) {
	res := ParseQuery([]rune(queryStr))
	if res.Err != nil {
		return nil, res.Err
	}
	fn := res.Payload.(query.Function)

	// Remove all tailing whitespace and ensure no remaining input.
	res = DiscardAll(OneOf(SpacesAndTabs(), Newline()))(res.Remaining)
	if len(res.Remaining) > 0 {
		return nil, NewError(res.Remaining, "end of input")
	}
	return fn, nil
}

// ParseQuery parses an input into a query.Function.
func ParseQuery(input []rune) Result {
	rootParser := parseWithTails(Expect(
		OneOf(
			matchExpressionParser(),
			ifExpressionParser(),
			bracketsExpressionParser(),
			literalValueParser(),
			functionParser(),
			variableLiteralParser(),
			fieldLiteralRootParser(),
		),
		"query",
	))
	res := SpacesAndTabs()(input)
	return arithmeticParser(rootParser)(res.Remaining)
}

// ParseDeprecatedQuery parses an input into a query.Function, but permits
// deprecated function interpolations. In order to support old functions this
// parser does not include field literals.
func ParseDeprecatedQuery(input []rune) Result {
	rootParser := OneOf(
		matchExpressionParser(),
		ifExpressionParser(),
		parseWithTails(bracketsExpressionParser()),
		parseWithTails(literalValueParser()),
		parseWithTails(functionParser()),
		parseDeprecatedFunction,
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

func tryParseQuery(expr string, deprecated bool) (query.Function, *Error) {
	var res Result
	if deprecated {
		res = ParseDeprecatedQuery([]rune(expr))
	} else {
		res = ParseQuery([]rune(expr))
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Payload.(query.Function), nil
}

//------------------------------------------------------------------------------
