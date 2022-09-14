package bloblang

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Executor stores a parsed Bloblang mapping and provides APIs for executing it.
type Executor struct {
	exec              *mapping.Executor
	emptyQueryMessage message.Batch
}

func newExecutor(exec *mapping.Executor) *Executor {
	return &Executor{
		exec:              exec,
		emptyQueryMessage: message.QuickBatch(nil),
	}
}

// ErrRootDeleted is returned by a Bloblang query when the mapping results in
// the root being deleted. It might be considered correct to do this in
// situations where filtering is allowed or expected.
var ErrRootDeleted = errors.New("root was deleted")

// Query executes a Bloblang mapping against a value and returns the result. The
// argument and return values can be structured using the same
// map[string]interface{} and []interface{} types as would be returned by the Go
// standard json package unmarshaler.
//
// If the mapping results in the root of the new document being deleted then
// ErrRootDeleted is returned, which can be used as a signal to filter rather
// than fail the mapping.
func (e *Executor) Query(value any) (any, error) {
	res, err := e.exec.Exec(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     map[string]any{},
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
	}.WithValue(value))
	if err != nil {
		return nil, err
	}

	switch res.(type) {
	case query.Delete:
		return nil, ErrRootDeleted
	case query.Nothing:
		return value, nil
	}
	return res, nil
}

// Overlay executes a Bloblang mapping against a value, where assignments are
// overlayed onto an existing structure.
//
// If the mapping results in the root of the new document being deleted then
// ErrRootDeleted is returned, which can be used as a signal to filter rather
// than fail the mapping.
func (e *Executor) Overlay(value any, onto *any) error {
	vars := map[string]any{}

	if err := e.exec.ExecOnto(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     vars,
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
		NewValue: onto,
	}.WithValue(value), mapping.AssignmentContext{
		Vars:  vars,
		Value: onto,
	}); err != nil {
		return err
	}

	switch (*onto).(type) {
	case query.Delete:
		return ErrRootDeleted
	case query.Nothing:
		*onto = nil
	}
	return nil
}
