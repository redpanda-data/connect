package bloblang

import (
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

// Query executes a Bloblang mapping against a value and returns the result. The
// argument and return values can be structured using the same
// map[string]interface{} and []interface{} types as would be returned by the Go
// standard json package unmarshaler.
func (e *Executor) Query(value interface{}) (interface{}, error) {
	for k := range e.emptyVars {
		delete(e.emptyVars, k)
	}

	return e.exec.Exec(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     e.emptyVars,
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
	}.WithValue(value))
}

// Overlay executes a Bloblang mapping against a value, where assignments are
// overlayed onto an existing structure.
func (e *Executor) Overlay(value interface{}, onto *interface{}) error {
	for k := range e.emptyVars {
		delete(e.emptyVars, k)
	}

	return e.exec.ExecOnto(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     e.emptyVars,
		Index:    0,
		MsgBatch: e.emptyQueryMessage,
	}.WithValue(value), mapping.AssignmentContext{
		Vars:  e.emptyVars,
		Value: onto,
	})
}
