package mapping

import (
	"errors"
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

//------------------------------------------------------------------------------

// Message is an interface type to be given to a query function, it allows the
// function to resolve fields and metadata from a message.
type Message interface {
	Get(p int) *message.Part
	Len() int
}

//------------------------------------------------------------------------------

// LineAndColOf returns the line and column position of a tailing clip from an
// input.
func LineAndColOf(input, clip []rune) (line, col int) {
	char := len(input) - len(clip)

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
		input:      input,
		assignment: assignment,
		query:      query,
	}
}

//------------------------------------------------------------------------------

// Executor is a parsed bloblang mapping that can be executed on a Benthos
// message.
type Executor struct {
	annotation string
	input      []rune
	maps       map[string]query.Function
	statements []Statement

	maxMapStacks int
}

const defaultMaxMapStacks = 5000

// NewExecutor initialises a new mapping executor from a map of query functions,
// and a list of assignments to be executed on each mapping. The input parameter
// is an optional slice pointing to the parsed expression that created the
// executor.
func NewExecutor(annotation string, input []rune, maps map[string]query.Function, statements ...Statement) *Executor {
	return &Executor{
		annotation:   annotation,
		input:        input,
		maps:         maps,
		statements:   statements,
		maxMapStacks: defaultMaxMapStacks,
	}
}

// SetMaxMapRecursion configures the maximum recursion allowed for maps, if the
// execution of this mapping matches this number of recursive map calls the
// mapping will error out.
func (e *Executor) SetMaxMapRecursion(m int) {
	e.maxMapStacks = m
}

// Annotation returns a string annotation that describes the mapping executor.
func (e *Executor) Annotation() string {
	return e.annotation
}

// Maps returns any map definitions contained within the mapping.
func (e *Executor) Maps() map[string]query.Function {
	return e.maps
}

// QueryPart executes the bloblang mapping on a particular message index of a
// batch. The message is parsed as a JSON document in order to provide the
// mapping context. The result of the mapping is expected to be a boolean value
// at the root, this is not the case, or if any stage of the mapping fails to
// execute, an error is returned.
func (e *Executor) QueryPart(index int, msg Message) (bool, error) {
	newPart, err := e.MapPart(index, msg)
	if err != nil {
		return false, err
	}
	if newPart == nil {
		return false, errors.New("query mapping resulted in deleted message, expected a boolean value")
	}
	newValue, err := newPart.AsStructured()
	if err != nil {
		return false, err
	}
	if b, ok := newValue.(bool); ok {
		return b, nil
	}
	return false, query.NewTypeErrorFrom("mapping", newValue, query.ValueBool)
}

// MapPart executes the bloblang mapping on a particular message index of a
// batch. The message is parsed as a JSON document in order to provide the
// mapping context. Returns an error if any stage of the mapping fails to
// execute.
//
// A resulting mapped message part is returned, unless the mapping results in a
// query.Delete value, in which case nil is returned and the part should be
// discarded.
func (e *Executor) MapPart(index int, msg Message) (*message.Part, error) {
	return e.mapPart(nil, index, msg)
}

// MapOnto maps into an existing message part, where mappings are appended to
// the message rather than being used to construct a new message.
func (e *Executor) MapOnto(part *message.Part, index int, msg Message) (*message.Part, error) {
	return e.mapPart(part, index, msg)
}

func (e *Executor) mapPart(appendTo *message.Part, index int, reference Message) (*message.Part, error) {
	var valuePtr *any
	var parseErr error

	lazyValue := func() *any {
		if valuePtr == nil && parseErr == nil {
			if jObj, err := reference.Get(index).AsStructured(); err == nil {
				valuePtr = &jObj
			} else {
				if errors.Is(err, message.ErrMessagePartNotExist) {
					parseErr = errors.New("message is empty")
				} else {
					parseErr = fmt.Errorf("parse as json: %w", err)
				}
			}
		}
		return valuePtr
	}

	var newPart *message.Part
	var newValue any = query.Nothing(nil)

	if appendTo == nil {
		newPart = reference.Get(index).ShallowCopy()
	} else {
		newPart = appendTo
		if appendObj, err := appendTo.AsStructuredMut(); err == nil {
			newValue = appendObj
		}
	}

	vars := map[string]any{}

	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(query.FunctionContext{
			Maps:     e.maps,
			Vars:     vars,
			Index:    index,
			MsgBatch: reference,
			NewMeta:  newPart,
			NewValue: &newValue,
		}.WithValueFunc(lazyValue))
		if err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			var ctxErr query.ErrNoContext
			if parseErr != nil && errors.As(err, &ctxErr) {
				if ctxErr.FieldName != "" {
					err = fmt.Errorf("unable to reference message as structured (with 'this.%v'): %w", ctxErr.FieldName, parseErr)
				} else {
					err = fmt.Errorf("unable to reference message as structured (with 'this'): %w", parseErr)
				}
			}
			return nil, fmt.Errorf("failed assignment (line %v): %w", line, err)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Vars:  vars,
			Meta:  newPart,
			Value: &newValue,
		}); err != nil {
			var line int
			if len(e.input) > 0 && len(stmt.input) > 0 {
				line, _ = LineAndColOf(e.input, stmt.input)
			}
			return nil, fmt.Errorf("failed to assign result (line %v): %w", line, err)
		}
	}

	switch newValue.(type) {
	case query.Delete:
		// Return nil (filter the message part)
		return nil, nil
	case query.Nothing:
		// Do not change the original contents
	default:
		switch t := newValue.(type) {
		case string:
			newPart.SetBytes([]byte(t))
		case []byte:
			newPart.SetBytes(t)
		default:
			newPart.SetStructuredMut(newValue)
		}
	}
	return newPart, nil
}

// QueryTargets returns a slice of all targets referenced by queries within the
// mapping.
func (e *Executor) QueryTargets(ctx query.TargetsContext) (query.TargetsContext, []query.TargetPath) {
	// Reset maps to our own.
	childCtx := ctx
	childCtx.Maps = e.maps

	var paths []query.TargetPath
	for _, stmt := range e.statements {
		_, tmpPaths := stmt.query.QueryTargets(childCtx)
		paths = append(paths, tmpPaths...)
	}

	return ctx, paths
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
func (e *Executor) Exec(ctx query.FunctionContext) (any, error) {
	ctx, stackCount := ctx.IncrStackCount()
	if stackCount > e.maxMapStacks {
		return nil, &errStacks{annotation: e.annotation, maxStacks: e.maxMapStacks}
	}

	var newObj any = query.Nothing(nil)
	ctx.NewValue = &newObj

	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(ctx)
		if err != nil {
			return nil, formatExecErr(err, true, e.input, stmt.input)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, AssignmentContext{
			Vars: ctx.Vars,
			// Meta: meta, Prevented for now due to .from(int)
			Value: &newObj,
		}); err != nil {
			return nil, formatExecErr(err, false, e.input, stmt.input)
		}
	}

	return newObj, nil
}

// ExecOnto a provided assignment context.
func (e *Executor) ExecOnto(ctx query.FunctionContext, onto AssignmentContext) error {
	for _, stmt := range e.statements {
		res, err := stmt.query.Exec(ctx)
		if err != nil {
			return formatExecErr(err, true, e.input, stmt.input)
		}
		if _, isNothing := res.(query.Nothing); isNothing {
			// Skip assignment entirely
			continue
		}
		if err = stmt.assignment.Apply(res, onto); err != nil {
			return formatExecErr(err, false, e.input, stmt.input)
		}
	}
	return nil
}

// ToBytes executes this function for a message of a batch and returns the
// result marshalled into a byte slice.
func (e *Executor) ToBytes(ctx query.FunctionContext) ([]byte, error) {
	v, err := e.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return query.IToBytes(v), nil
}

// ToString executes this function for a message of a batch and returns the
// result marshalled into a string.
func (e *Executor) ToString(ctx query.FunctionContext) (string, error) {
	v, err := e.Exec(ctx)
	if err != nil {
		return "", err
	}
	return query.IToString(v), nil
}

//------------------------------------------------------------------------------

type failedAssignmentErr struct {
	line   int
	onExec bool
	err    error
}

func (f *failedAssignmentErr) Unwrap() error {
	return f.err
}

func (f *failedAssignmentErr) Error() string {
	if f.onExec {
		return fmt.Sprintf("failed assignment (line %v): %v", f.line, f.err)
	}
	return fmt.Sprintf("failed to assign result (line %v): %v", f.line, f.err)
}

type errStacks struct {
	annotation string
	maxStacks  int
}

func (e *errStacks) Error() string {
	return fmt.Sprintf("entering %v exceeded maximum allowed stacks of %v, this could be due to unbounded recursion", e.annotation, e.maxStacks)
}

func formatExecErr(err error, onExec bool, input, stmtInput []rune) error {
	var u *failedAssignmentErr
	if errors.As(err, &u) {
		return u
	}

	var line int
	if len(input) > 0 && len(stmtInput) > 0 {
		line, _ = LineAndColOf(input, stmtInput)
	}

	var e *errStacks
	if errors.As(err, &e) {
		err = e
	}

	return &failedAssignmentErr{
		line:   line,
		onExec: onExec,
		err:    err,
	}
}
