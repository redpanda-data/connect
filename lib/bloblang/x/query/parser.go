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

func createParser(deprecated bool) parser.Type {
	opParser := arithmeticOpParser()
	openBracket := parser.Char('(')
	closeBracket := parser.Char(')')

	fieldVersusFunction := functionParser()
	if !deprecated {
		fieldVersusFunction = parser.BestMatch(
			fieldLiteralParser(nil, true),
			matchExpressionParser(),
			fieldVersusFunction,
		)
	}
	nextSegment := parser.AnyOf(
		openBracket,
		literalParser(),
		fieldVersusFunction,
	)

	whitespace := parser.DiscardAll(
		parser.AnyOf(
			parser.SpacesAndTabs(),
			parser.NewlineAllowComment(),
		),
	)

	return func(input []rune) parser.Result {
		var fns []Function
		var ops []arithmeticOp

		res := parser.SpacesAndTabs()(input)
		for {
			i := len(input) - len(res.Remaining)
			res = nextSegment(res.Remaining)
			if res.Err != nil {
				res.Err = parser.ErrAtPosition(i, res.Err)
				res.Remaining = input
				if i == 0 && deprecated {
					resDeprecated := parseDeprecatedFunction(input)
					if resDeprecated.Err == nil {
						return resDeprecated
					}
				}
				return res
			}
			switch t := res.Result.(type) {
			case Function:
				fns = append(fns, t)
			case string:
				// ASSUMPTION: Must be open bracket
				res = whitespace(res.Remaining)
				i = len(input) - len(res.Remaining)
				res = Parse(res.Remaining)
				if res.Err != nil {
					res.Err = parser.ErrAtPosition(i, res.Err)
					res.Remaining = input
					return res
				}

				bracketFn := res.Result.(Function)
				res = whitespace(res.Remaining)
				i = len(input) - len(res.Remaining)
				res = closeBracket(res.Remaining)
				if res.Err != nil {
					res.Err = parser.ErrAtPosition(i, res.Err)
					res.Remaining = input
					return res
				}

			bracketTails:
				for {
					res = parser.Char('.')(res.Remaining)
					if res.Err != nil {
						break bracketTails
					}

					i = len(input) - len(res.Remaining)
					res = parseFunctionTail(bracketFn)(res.Remaining)
					if res.Err != nil {
						res.Err = parser.ErrAtPosition(i, res.Err)
						res.Remaining = input
						return res
					}
					bracketFn = res.Result.(Function)
				}
				fns = append(fns, bracketFn)
			}

			res = parser.SpacesAndTabs()(res.Remaining)
			if len(res.Remaining) == 0 {
				break
			}

			i = len(input) - len(res.Remaining)
			res = opParser(res.Remaining)
			if res.Err != nil {
				if len(fns) == 0 {
					res.Err = parser.ErrAtPosition(i, res.Err)
					res.Remaining = input
					return res
				}
				break
			}
			ops = append(ops, res.Result.(arithmeticOp))
			res = whitespace(res.Remaining)
		}

		fn, err := resolveArithmetic(fns, ops)
		if err != nil {
			return parser.Result{
				Err:       err,
				Remaining: input,
			}
		}
		return parser.Result{
			Result:    fn,
			Remaining: res.Remaining,
		}
	}
}

// Parse parses an input into a query.Function.
func Parse(input []rune) parser.Result {
	return createParser(false)(input)
}

// ParseDeprecated parses an input into a query.Function, but permits deprecated
// function interpolations. In order to support old functions this parser does
// not include field literals.
func ParseDeprecated(input []rune) parser.Result {
	return createParser(true)(input)
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
