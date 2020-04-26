package mapping

import (
	"bytes"
	"errors"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

//------------------------------------------------------------------------------

type mappingStatement struct {
	assignment Assignment
	query      query.Function
}

// Executor is a parsed bloblang mapping that can be executed on a Benthos
// message.
type Executor struct {
	statements []mappingStatement
}

// MapPart executes the bloblang map on a particular message index of a batch.
// Returns an error if the mapping fails to execute, in which case the message
// will remain unchanged.
func (e *Executor) MapPart(index int, msg Message) error {
	return errors.New("not implemented")
}

//------------------------------------------------------------------------------'

// Parse parses an input into a bloblang mapping executor. Returns an *Executor
// unless a parsing error occurs.
func Parse(input []rune) parser.Result {
	return parser.Result{
		Remaining: input,
		Err:       errors.New("not implemented"),
	}
}

//------------------------------------------------------------------------------

func pathLiteralParser() parser.Type {
	fieldPathParser := parser.AnyOf(
		parser.InRange('a', 'z'),
		parser.InRange('A', 'Z'),
		parser.InRange('0', '9'),
		parser.InRange('*', '-'),
		parser.Char('.'),
		parser.Char('_'),
		parser.Char('~'),
	)

	return func(input []rune) parser.Result {
		var buf bytes.Buffer
		res := parser.Result{
			Remaining: input,
		}
		for {
			if res = fieldPathParser(res.Remaining); res.Err != nil {
				break
			}
			buf.WriteString(res.Result.(string))
		}
		if buf.Len() == 0 {
			return res
		}
		return parser.Result{
			Result:    buf.String(),
			Remaining: res.Remaining,
		}
	}
}

func assignmentParser() parser.Type {
	let := parser.Match("let ")
	meta := parser.Match("meta ")

	literal := parser.QuotedString()
	path := pathLiteralParser()
	eq := parser.Char('=')
	whitespace := parser.SpacesAndTabs()

	return func(input []rune) parser.Result {
		var assignmentCat, assignmentTarget string

		res := parser.AnyOf(let, meta)(input)
		if res.Err == nil {
			assignmentCat = strings.TrimSpace(res.Result.(string))
		}
		res = whitespace(res.Remaining)
		i := len(input) - len(res.Remaining)
		if res = parser.AnyOf(literal, path)(input); res.Err == nil {
			assignmentTarget = res.Result.(string)
		}

		var statement mappingStatement
		switch assignmentCat {
		case "let":
			if len(assignmentTarget) == 0 {
				return parser.Result{
					Err: parser.ErrAtPosition(i, parser.ExpectedError{
						"target-path",
					}),
					Remaining: input,
				}
			}
			statement.assignment = &varAssignment{
				Name: assignmentTarget,
			}
		case "meta":
			statement.assignment = &metaAssignment{
				Key: assignmentTarget,
			}
		default:
			if len(assignmentTarget) == 0 {
				return parser.Result{
					Err: parser.ErrAtPosition(i, parser.ExpectedError{
						"target-path",
					}),
					Remaining: input,
				}
			}
			statement.assignment = &jsonAssignment{
				Path: gabs.DotPathToSlice(assignmentTarget),
			}
		}

		res = whitespace(res.Remaining)
		i = len(input) - len(res.Remaining)
		if res = eq(res.Remaining); res.Err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, res.Err),
				Remaining: input,
			}
		}

		res = whitespace(res.Remaining)
		i = len(input) - len(res.Remaining)
		res = query.Parse(res.Remaining)
		if res.Err != nil {
			return parser.Result{
				Err:       parser.ErrAtPosition(i, res.Err),
				Remaining: input,
			}
		}

		statement.query = res.Result.(query.Function)
		return parser.Result{
			Result:    statement,
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
