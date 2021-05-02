package bloblang

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Executor stores a parsed Bloblang mapping and provides APIs for executing it.
type Executor struct {
	exec              *mapping.Executor
	emptyVars         map[string]interface{}
	emptyQueryMessage types.Message
}

func newExecutor(exec *mapping.Executor) *Executor {
	return &Executor{
		exec:              exec,
		emptyVars:         map[string]interface{}{},
		emptyQueryMessage: message.New(nil),
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
func (e *Executor) Query(value interface{}) (interface{}, error) {
	for k := range e.emptyVars {
		delete(e.emptyVars, k)
	}

	res, err := e.exec.Exec(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     e.emptyVars,
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
func (e *Executor) Overlay(value interface{}, onto *interface{}) error {
	for k := range e.emptyVars {
		delete(e.emptyVars, k)
	}

	if err := e.exec.ExecOnto(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     e.emptyVars,
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
	}.WithValue(value), mapping.AssignmentContext{
		Vars:  e.emptyVars,
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
