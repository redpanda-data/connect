package query

import (
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

// FunctionContext provides access to a root message, its index within the batch, and
type FunctionContext struct {
	Value  *interface{}
	Vars   map[string]interface{}
	Index  int
	Msg    Message
	Legacy bool
}

// Function takes a set of contextual parameters and returns the result of the
// query.
type Function interface {
	// Execute this function for a message of a batch.
	Exec(ctx FunctionContext) (interface{}, error)

	// Execute this function for a message of a batch and return the result
	// marshalled into a byte slice.
	ToBytes(ctx FunctionContext) []byte

	// Execute this function for a message of a batch and return the result
	// marshalled into a string.
	ToString(ctx FunctionContext) string
}

//------------------------------------------------------------------------------

// Parse parses an input into a query.Function.
func Parse(input []rune) parser.Result {
	rootParser := parser.AnyOf(
		matchExpressionParser(),
		parseWithTails(bracketsExpressionParser()),
		parseWithTails(literalValueParser()),
		parseWithTails(functionParser()),
		parseWithTails(fieldLiteralRootParser()),
	)
	res := parser.SpacesAndTabs()(input)
	i := len(input) - len(res.Remaining)
	if res = arithmeticParser(rootParser)(res.Remaining); res.Err != nil {
		res.Err = parser.ErrAtPosition(i, res.Err)
	}
	return res
}

// ParseDeprecated parses an input into a query.Function, but permits deprecated
// function interpolations. In order to support old functions this parser does
// not include field literals.
func ParseDeprecated(input []rune) parser.Result {
	rootParser := parser.AnyOf(
		matchExpressionParser(),
		parseWithTails(bracketsExpressionParser()),
		parseWithTails(literalValueParser()),
		parseWithTails(functionParser()),
		parseDeprecatedFunction,
	)
	res := arithmeticParser(rootParser)(input)
	if res.Err != nil {
		return res
	}

	result := res.Result
	res = parser.SpacesAndTabs()(res.Remaining)
	return parser.Result{
		Result:    result,
		Remaining: res.Remaining,
	}
}

func tryParse(expr string, deprecated bool) (Function, error) {
	var res parser.Result
	if deprecated {
		res = ParseDeprecated([]rune(expr))
	} else {
		res = Parse([]rune(expr))
	}
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Result.(Function), nil
}

//------------------------------------------------------------------------------
