package mapping

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/parser"
	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"golang.org/x/xerrors"
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

// MapPart executes the bloblang mapping on a particular message index of a
// batch. The message is parsed as a JSON document in order to provide the
// mapping context. Returns an error if any stage of the mapping fails to
// execute.
//
// Note that the message is mutated in situ and therefore the contents may be
// modified even if an error is returned.
func (e *Executor) MapPart(index int, msg Message) error {
	vars := map[string]interface{}{}
	meta := msg.Get(index).Metadata()
	jObj, err := msg.Get(index).JSON()
	if err != nil {
		return xerrors.Errorf("failed to parse message as JSON: %w", err)
	}

	var newObj interface{} = map[string]interface{}{}
	for i, stmt := range e.statements {
		res, err := stmt.query.Exec(query.FunctionContext{
			Value: &jObj,
			Vars:  vars,
			Index: index,
			Msg:   msg,
		})
		if err != nil {
			return xerrors.Errorf("failed to execute mapping assignment %v: %v", i, err)
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Vars:  vars,
			Meta:  meta,
			Value: &newObj,
		}); err != nil {
			return xerrors.Errorf("failed to assign mapping result %v: %v", i, err)
		}
	}
	if err = msg.Get(index).SetJSON(newObj); err != nil {
		return xerrors.Errorf("failed to set result of mapping: %w", err)
	}
	return nil
}

// NewExecutor parses a bloblang mapping and returns an executor to run it, or
// an error if the parsing fails.
func NewExecutor(mapping string) (*Executor, error) {
	res := ParseExecutor([]rune(mapping))
	if res.Err != nil {
		return nil, xerrors.Errorf("failed to parse mapping: %w", res.Err)
	}
	if len(res.Remaining) > 0 {
		i := len(mapping) - len(res.Remaining)
		return nil, parser.ErrAtPosition(i, fmt.Errorf("unexpected content at the end of mapping: %v", string(res.Remaining)))
	}
	return res.Result.(*Executor), nil
}

//------------------------------------------------------------------------------'

// ParseExecutor implements parser.Type and parses an input into a bloblang
// mapping executor. Returns an *Executor unless a parsing error occurs.
func ParseExecutor(input []rune) parser.Result {
	var statements []mappingStatement
	stmtParse := statementParser()
	whitespace := parser.SpacesAndTabs()
	nl := parser.AnyOf(
		parser.Char('\n'),
		query.CommentParser(),
	)
	whitespaceGobbler := parser.DiscardAll(parser.AnyOf(whitespace, nl))

	res := whitespaceGobbler(input)

	i := len(input) - len(res.Remaining)
	res = stmtParse(res.Remaining)

statementLoop:
	for res.Err == nil {
		statements = append(statements, res.Result.(mappingStatement))

		res = whitespace(res.Remaining)
		i = len(input) - len(res.Remaining)
		if res = nl(res.Remaining); res.Err == nil {
			res = whitespaceGobbler(res.Remaining)
			i = len(input) - len(res.Remaining)
			if len(res.Remaining) == 0 {
				break statementLoop
			}
			res = stmtParse(res.Remaining)
		}
	}
	if len(statements) == 0 {
		if res.Err == nil {
			res.Err = errors.New("no mapping statements were parsed")
		} else {
			res.Err = parser.ErrAtPosition(i, res.Err)
		}
		return parser.Result{
			Err:       res.Err,
			Remaining: input,
		}
	}

	return parser.Result{
		Remaining: res.Remaining,
		Result:    &Executor{statements},
	}
}

//------------------------------------------------------------------------------

func pathLiteralParser() parser.Type {
	// TODO: Parse root.foo.bar
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

func statementParser() parser.Type {
	let := parser.Match("let ")
	meta := parser.Match("meta ")

	literal := parser.QuotedString()
	path := pathLiteralParser()
	eq := parser.Char('=')
	whitespace := parser.SpacesAndTabs()

	return func(input []rune) parser.Result {
		var assignmentCat, assignmentTarget string
		var targetSet bool

		res := parser.AnyOf(let, meta)(input)
		if res.Err == nil {
			assignmentCat = strings.TrimSpace(res.Result.(string))
		}
		res = whitespace(res.Remaining)
		i := len(input) - len(res.Remaining)
		if res = parser.AnyOf(literal, path)(res.Remaining); res.Err == nil {
			assignmentTarget = res.Result.(string)
			targetSet = true
		}

		var statement mappingStatement
		switch assignmentCat {
		case "let":
			if !targetSet {
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
			metaAssignment := &metaAssignment{}
			if targetSet {
				metaAssignment.Key = &assignmentTarget
			}
			statement.assignment = metaAssignment
		default:
			if !targetSet {
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
