package mapping

import (
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) types.Part
	Len() int
}

//------------------------------------------------------------------------------

// LineAndColOf returns the line and column position of a tailing clip from an
// input.
func LineAndColOf(input, clip []rune) (int, int) {
	line, char := 0, len(input)-len(clip)

	lines := strings.Split(string(input), "\n")
	for ; line < len(lines); line++ {
		if char < (len(lines[line]) + 1) {
			break
		}
		char = char - len(lines[line]) - 1
	}

	return line + 1, char + 1
}

//------------------------------------------------------------------------------

// Statement describes an isolated mapping statement, where the result of a
// query function is to be mapped according to an Assignment.
type Statement struct {
	input      []rune
	assignment Assignment
	query      query.Function
}

// NewStatement initialises a new mapping statement from an Assignment and
// query.Function. The input parameter is an optional slice pointing to the
// parsed expression that created the statement.
func NewStatement(input []rune, assignment Assignment, query query.Function) Statement {
	return Statement{
		input, assignment, query,
	}
}

//------------------------------------------------------------------------------

// Executor is a parsed bloblang mapping that can be executed on a Benthos
// message.
type Executor struct {
	input      []rune
	maps       map[string]query.Function
	statements []Statement
}

// NewExecutor initialises a new mapping executor from a map of query functions,
// and a list of assignments to be executed on each mapping. The input parameter
// is an optional slice pointing to the parsed expression that created the
// executor.
func NewExecutor(input []rune, maps map[string]query.Function, statements ...Statement) *Executor {
	return &Executor{
		input, maps, statements,
	}
}

// Maps returns any map definitions contained within the mapping.
func (e *Executor) Maps() map[string]query.Function {
	return e.maps
}

// MapPart executes the bloblang mapping on a particular message index of a
// batch. The message is parsed as a JSON document in order to provide the
// mapping context. Returns an error if any stage of the mapping fails to
// execute.
//
// A resulting mapped message part is returned, unless the mapping results in a
// query.Delete value, in which case nil is returned and the part should be
// discarded.
func (e *Executor) MapPart(index int, msg Message) (types.Part, error) {
	return e.mapPart(nil, index, msg)
}

// MapOnto maps into an existing message part, where mappings are appended to
// the message rather than being used to construct a new message.
func (e *Executor) MapOnto(part types.Part, index int, msg Message) (types.Part, error) {
	return e.mapPart(part, index, msg)
}

// MapInto an existing message. If append is set to true then mappings are
// appended to the existing message, otherwise the newly mapped object will
// begin empty.
func (e *Executor) mapPart(appendTo types.Part, index int, reference Message) (types.Part, error) {
	var valuePtr *interface{}
	var parseErr error
	if jObj, err := reference.Get(index).JSON(); err == nil {
		valuePtr = &jObj
	} else {
		parseErr = err
	}

	var newPart types.Part
	var newObj interface{} = query.Nothing(nil)
	var newMeta types.Metadata

	if appendTo == nil {
		newPart = reference.Get(index).Copy()
	} else {
		newPart = appendTo
		if appendObj, err := appendTo.JSON(); err == nil {
			newObj = appendObj
		}
	}
	newMeta = newPart.Metadata()

	vars := map[string]interface{}{}

	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(query.FunctionContext{
			Maps:     e.maps,
			Value:    valuePtr,
			Vars:     vars,
			Index:    index,
			MsgBatch: reference,
		})
		if err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			if parseErr != nil && errors.Is(err, query.ErrNoContext) {
				err = fmt.Errorf("failed to parse message as JSON: %w", parseErr)
			}
			return nil, fmt.Errorf("failed to execute mapping query at line %v: %w", line, err)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Maps:  e.maps,
			Vars:  vars,
			Meta:  newMeta,
			Value: &newObj,
		}); err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			return nil, fmt.Errorf("failed to assign query result at line %v: %w", line, err)
		}
	}

	switch newObj.(type) {
	case query.Delete:
		// Return nil (filter the message part)
		return nil, nil
	case query.Nothing:
		// Do not change the original contents
	default:
		switch t := newObj.(type) {
		case string:
			newPart.Set([]byte(t))
		case []byte:
			newPart.Set(t)
		default:
			if err := newPart.SetJSON(newObj); err != nil {
				return nil, fmt.Errorf("failed to set result of mapping: %w", err)
			}
		}
	}
	return newPart, nil
}

// QueryTargets returns a slice of all targets referenced by queries within the
// mapping.
func (e *Executor) QueryTargets(ctx query.TargetsContext) []query.TargetPath {
	// Reset maps to our own.
	ctx.Maps = e.maps

	var paths []query.TargetPath
	for _, stmt := range e.statements {
		paths = append(paths, stmt.query.QueryTargets(ctx)...)
	}
	return paths
}

// AssignmentTargets returns a slice of all targets assigned to by statements
// within the mapping.
func (e *Executor) AssignmentTargets() []TargetPath {
	var paths []TargetPath
	for _, stmt := range e.statements {
		paths = append(paths, stmt.assignment.Target())
	}
	return paths
}

// Exec this function with a context struct.
func (e *Executor) Exec(ctx query.FunctionContext) (interface{}, error) {
	var newObj interface{} = query.Nothing(nil)
	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(ctx)
		if err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			return nil, fmt.Errorf("failed to execute mapping assignment at line %v: %w", line, err)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Maps: e.maps,
			Vars: ctx.Vars,
			// Meta: meta, Prevented for now due to .from(int)
			Value: &newObj,
		}); err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			return nil, fmt.Errorf("failed to assign mapping result at line %v: %w", line, err)
		}
	}

	return newObj, nil
}

// ToBytes executes this function for a message of a batch and returns the
// result marshalled into a byte slice.
func (e *Executor) ToBytes(ctx query.FunctionContext) []byte {
	v, err := e.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*query.ErrRecoverable); ok {
			return query.IToBytes(rec.Recovered)
		}
		return nil
	}
	return query.IToBytes(v)
}

// ToString executes this function for a message of a batch and returns the
// result marshalled into a string.
func (e *Executor) ToString(ctx query.FunctionContext) string {
	v, err := e.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*query.ErrRecoverable); ok {
			return query.IToString(rec.Recovered)
		}
		return ""
	}
	return query.IToString(v)
}

//------------------------------------------------------------------------------
