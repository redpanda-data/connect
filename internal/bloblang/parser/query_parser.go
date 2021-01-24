package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// FunctionSet provides constructors to the functions available in this query.
type FunctionSet interface {
	Init(string, ...interface{}) (query.Function, error)
}

// MethodSet provides constructors to the methods available in this query.
type MethodSet interface {
	Init(string, query.Function, ...interface{}) (query.Function, error)
}

// Context contains context used throughout a Bloblang parser for
// accessing function and method constructors.
type Context struct {
	Functions FunctionSet
	Methods   MethodSet
}

// InitFunction attempts to initialise a function from the available
// constructors of the parser context.
func (pCtx Context) InitFunction(name string, args ...interface{}) (query.Function, error) {
	return pCtx.Functions.Init(name, args...)
}

// InitMethod attempts to initialise a method from the available constructors of
// the parser context.
func (pCtx Context) InitMethod(name string, target query.Function, args ...interface{}) (query.Function, error) {
	return pCtx.Methods.Init(name, target, args...)
}

func queryParser(pCtx Context) func(input []rune) Result {
	rootParser := parseWithTails(Expect(
		OneOf(
			matchExpressionParser(pCtx),
			ifExpressionParser(pCtx),
			bracketsExpressionParser(pCtx),
			literalValueParser(pCtx),
			functionParser(pCtx),
			variableLiteralParser(),
			fieldLiteralRootParser(),
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
func ParseDeprecatedQuery(input []rune) Result {
	pCtx := Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	}
	rootParser := OneOf(
		matchExpressionParser(pCtx),
		ifExpressionParser(pCtx),
		parseWithTails(bracketsExpressionParser(pCtx), pCtx),
		parseWithTails(literalValueParser(pCtx), pCtx),
		parseWithTails(functionParser(pCtx), pCtx),
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

//------------------------------------------------------------------------------
