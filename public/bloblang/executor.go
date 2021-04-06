package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
)

// Executor stores a parsed Bloblang mapping and provides APIs for executing it.
type Executor struct {
	exec *mapping.Executor
}

// Query executes a Bloblang mapping against a value and returns the result. The
// argument and return values can be structured using the same
// map[string]interface{} and []interface{} types as would be returned by the Go
// standard json package unmarshaler.
func (e *Executor) Query(v interface{}) (interface{}, error) {
	msg := message.New(nil)
	part := message.NewPart(nil)

	if err := part.SetJSON(v); err != nil {
		return nil, err
	}
	msg.Append(part)

	return e.exec.Exec(query.FunctionContext{
		Maps:     e.exec.Maps(),
		Vars:     map[string]interface{}{},
		Index:    0,
		MsgBatch: msg,
	}.WithValue(v))
}
