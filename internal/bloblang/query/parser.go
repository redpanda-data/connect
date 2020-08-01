package query

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// MessageBatch is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a Benthos message batch.
type MessageBatch interface {
	Get(p int) types.Part
	Len() int
}

// FunctionContext provides access to a root message, its index within the batch, and
type FunctionContext struct {
	Value    *interface{}
	Maps     map[string]Function
	Vars     map[string]interface{}
	Index    int
	MsgBatch MessageBatch
	Legacy   bool
}

// Function takes a set of contextual parameters and returns the result of the
// query.
type Function interface {
	// Execute this function for a message of a batch.
	Exec(ctx FunctionContext) (interface{}, error)
}

// ExecToString returns a string from a function exection.
func ExecToString(fn Function, ctx FunctionContext) string {
	v, err := fn.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToString(rec.Recovered)
		}
		return ""
	}
	return IToString(v)
}

// ExecToBytes returns a byte slice from a function exection.
func ExecToBytes(fn Function, ctx FunctionContext) []byte {
	v, err := fn.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToBytes(rec.Recovered)
		}
		return nil
	}
	return IToBytes(v)
}

//------------------------------------------------------------------------------

// New creates a new query function from a query string.
func New(query string) (Function, error) {
	res := Parse([]rune(query))
	if res.Err != nil {
		return nil, res.Err
	}
	fn := res.Payload.(Function)

	// Remove all tailing whitespace and ensure no remaining input.
	res = parser.DiscardAll(parser.OneOf(parser.SpacesAndTabs(), parser.Newline()))(res.Remaining)
	if len(res.Remaining) > 0 {
		i := len(query) - len(res.Remaining)
		return nil, parser.ErrAtPosition(i, parser.ExpectedError{"end-of-input"})
	}
	return fn, nil
}

// Parse parses an input into a query.Function.
func Parse(input []rune) parser.Result {
	rootParser := parseWithTails(parser.OneOf(
		matchExpressionParser(),
		ifExpressionParser(),
		bracketsExpressionParser(),
		literalValueParser(),
		functionParser(),
		variableLiteralParser(),
		fieldLiteralRootParser(),
	))
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
	rootParser := parser.OneOf(
		matchExpressionParser(),
		ifExpressionParser(),
		parseWithTails(bracketsExpressionParser()),
		parseWithTails(literalValueParser()),
		parseWithTails(functionParser()),
		parseDeprecatedFunction,
	)

	res := parser.SpacesAndTabs()(input)

	i := len(input) - len(res.Remaining)
	res = arithmeticParser(rootParser)(res.Remaining)
	if res.Err != nil {
		return parser.Result{
			Err:       parser.ErrAtPosition(i, res.Err),
			Remaining: input,
		}
	}

	result := res.Payload
	res = parser.SpacesAndTabs()(res.Remaining)
	return parser.Result{
		Payload:   result,
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
	return res.Payload.(Function), nil
}

//------------------------------------------------------------------------------
