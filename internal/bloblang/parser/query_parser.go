package parser

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
)

// FunctionSet provides constructors to the functions available in this query.
type FunctionSet interface {
	Params(name string) (query.Params, error)
	Init(name string, args *query.ParsedParams) (query.Function, error)
}

// MethodSet provides constructors to the methods available in this query.
type MethodSet interface {
	Params(name string) (query.Params, error)
	Init(name string, target query.Function, args *query.ParsedParams) (query.Function, error)
}

// Context contains context used throughout a Bloblang parser for
// accessing function and method constructors.
type Context struct {
	Functions    FunctionSet
	Methods      MethodSet
	namedContext *namedContext
}

// GlobalContext returns a parser context with globally defined functions and
// methods.
func GlobalContext() Context {
	return Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	}
}

type namedContext struct {
	name string
	next *namedContext
}

// WithNamedContext returns a Context with a named execution context.
func (pCtx Context) WithNamedContext(name string) Context {
	next := pCtx.namedContext
	pCtx.namedContext = &namedContext{name, next}
	return pCtx
}

// HasNamedContext returns true if a given name exists as a named context.
func (pCtx Context) HasNamedContext(name string) bool {
	tmp := pCtx.namedContext
	for tmp != nil {
		if tmp.name == name {
			return true
		}
		tmp = tmp.next
	}
	return false
}

// InitFunction attempts to initialise a function from the available
// constructors of the parser context.
func (pCtx Context) InitFunction(name string, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Functions.Init(name, args)
}

// InitMethod attempts to initialise a method from the available constructors of
// the parser context.
func (pCtx Context) InitMethod(name string, target query.Function, args *query.ParsedParams) (query.Function, error) {
	return pCtx.Methods.Init(name, target, args)
}

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
func ParseDeprecatedQuery(pCtx Context) Func {
	return func(input []rune) Result {
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
}

func tryParseQuery(expr string, deprecated bool) (query.Function, *Error) {
	pCtx := Context{
		Functions: query.AllFunctions,
		Methods:   query.AllMethods,
	}
	var res Result
	if deprecated {
		res = ParseDeprecatedQuery(pCtx)([]rune(expr))
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
