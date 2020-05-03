package mapping

import (
	"fmt"

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

	var valuePtr *interface{}
	if jObj, err := msg.Get(index).JSON(); err == nil {
		valuePtr = &jObj
	}

	var newObj interface{} = query.Nothing(nil)
	for i, stmt := range e.statements {
		res, err := stmt.query.Exec(query.FunctionContext{
			Value: valuePtr,
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

	if _, notMapped := newObj.(query.Nothing); !notMapped {
		if err := msg.Get(index).SetJSON(newObj); err != nil {
			return xerrors.Errorf("failed to set result of mapping: %w", err)
		}
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
	executor := res.Result.(*Executor)

	// Remove all trailing whitespace and comments.
	res = parser.DiscardAll(
		parser.AnyOf(
			parser.NewlineAllowComment(),
			parser.SpacesAndTabs(),
		),
	)(res.Remaining)

	// And if there's content remaining report it as an error.
	if len(res.Remaining) > 0 {
		i := len(mapping) - len(res.Remaining)
		return nil, parser.ErrAtPosition(i, fmt.Errorf("unexpected content at the end of mapping: %v", string(res.Remaining)))
	}
	return executor, nil
}

//------------------------------------------------------------------------------'

// ParseExecutor implements parser.Type and parses an input into a bloblang
// mapping executor. Returns an *Executor unless a parsing error occurs.
func ParseExecutor(input []rune) parser.Result {
	newline := parser.NewlineAllowComment()
	whitespace := parser.SpacesAndTabs()
	allWhitespace := parser.DiscardAll(parser.AnyOf(whitespace, newline))

	statement := parser.AnyOf(
		letStatementParser(),
		metaStatementParser(),
		plainMappingStatementParser(),
	)

	res := parser.Sequence(
		allWhitespace,
		statement,
	)(input)
	if res.Err != nil {
		return res
	}
	statements := []mappingStatement{
		res.Result.([]interface{})[1].(mappingStatement),
	}

	if res = parser.AllOf(parser.Sequence(
		parser.Discard(whitespace),
		newline,
		allWhitespace,
		statement,
	))(res.Remaining); res.Err == nil {
		resSlice := res.Result.([]interface{})
		for _, seq := range resSlice {
			seqSlice := seq.([]interface{})
			statements = append(statements, seqSlice[3].(mappingStatement))
		}
	}

	return parser.Result{
		Remaining: res.Remaining,
		Result:    &Executor{statements},
	}
}

//------------------------------------------------------------------------------

func pathLiteralParser() parser.Type {
	return parser.JoinStringSliceResult(
		parser.AllOf(
			parser.AnyOf(
				parser.InRange('a', 'z'),
				parser.InRange('A', 'Z'),
				parser.InRange('0', '9'),
				parser.InRange('*', '-'),
				parser.Char('.'),
				parser.Char('_'),
				parser.Char('~'),
			),
		),
	)
}

func letStatementParser() parser.Type {
	p := parser.Sequence(
		parser.Match("let"),
		parser.SpacesAndTabs(),
		// Prevents a missing path from being captured by the next parser
		parser.MustBe(
			parser.InterceptExpectedError(
				parser.AnyOf(
					parser.QuotedString(),
					pathLiteralParser(),
				),
				"variable-name",
			),
		),
		parser.SpacesAndTabs(),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Result.([]interface{})
		return parser.Result{
			Result: mappingStatement{
				assignment: &varAssignment{
					Name: resSlice[2].(string),
				},
				query: resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func metaStatementParser() parser.Type {
	p := parser.Sequence(
		parser.Match("meta"),
		parser.SpacesAndTabs(),
		parser.Optional(parser.AnyOf(
			parser.QuotedString(),
			pathLiteralParser(),
		)),
		parser.Optional(parser.SpacesAndTabs()),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Result.([]interface{})

		var keyPtr *string
		if key, set := resSlice[2].(string); set {
			keyPtr = &key
		}

		return parser.Result{
			Result: mappingStatement{
				assignment: &metaAssignment{Key: keyPtr},
				query:      resSlice[6].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

func plainMappingStatementParser() parser.Type {
	p := parser.Sequence(
		parser.InterceptExpectedError(
			parser.AnyOf(
				parser.QuotedString(),
				pathLiteralParser(),
			),
			"target-path",
		),
		parser.SpacesAndTabs(),
		parser.Char('='),
		parser.SpacesAndTabs(),
		query.Parse,
	)

	return func(input []rune) parser.Result {
		res := p(input)
		if res.Err != nil {
			return res
		}
		resSlice := res.Result.([]interface{})
		path := gabs.DotPathToSlice(resSlice[0].(string))
		if len(path) > 0 && path[0] == "root" {
			path = path[1:]
		}
		return parser.Result{
			Result: mappingStatement{
				assignment: &jsonAssignment{
					Path: path,
				},
				query: resSlice[4].(query.Function),
			},
			Remaining: res.Remaining,
		}
	}
}

//------------------------------------------------------------------------------
