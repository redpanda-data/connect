// Package query provides a parser for the right-hand side query part of the
// bloblang spec. This is useful as a separate package as it is used in
// isolation within interpolation functions.
package query

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// ErrRecoverable represents a function execution error that can optionally be
// recovered into a zero-value.
type ErrRecoverable struct {
	Recovered interface{}
	Err       error
}

// Unwrap the error.
func (e *ErrRecoverable) Unwrap() error {
	return e.Err
}

// Error implements the standard error interface.
func (e *ErrRecoverable) Error() string {
	return e.Err.Error()
}

//------------------------------------------------------------------------------

type badFunctionErr string

func (e badFunctionErr) Error() string {
	return fmt.Sprintf("unrecognised function '%v'", string(e))
}

type badMethodErr string

func (e badMethodErr) Error() string {
	return fmt.Sprintf("unrecognised method '%v'", string(e))
}

//------------------------------------------------------------------------------

// MessageBatch is an interface type to be given to a query function, it allows
// the function to resolve fields and metadata from a Benthos message batch.
type MessageBatch interface {
	Get(p int) types.Part
	Len() int
}

// FunctionContext provides access to a range of query targets for functions to
// reference.
type FunctionContext struct {
	Value    *interface{}
	Maps     map[string]Function
	Vars     map[string]interface{}
	Index    int
	MsgBatch MessageBatch
	Legacy   bool
}

//------------------------------------------------------------------------------

// Function takes a set of contextual arguments and returns the result of the
// query.
type Function interface {
	// Execute this function for a message of a batch.
	Exec(ctx FunctionContext) (interface{}, error)

	// Return a map of target types to path segments for any targets that this
	// query function references.
	QueryTargets() []TargetPath
}

// ExecToString returns a string from a function exection.
func ExecToString(fn Function, ctx FunctionContext) string {
	v, err := fn.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToString(rec.Recovered)
		}
		return ""
	}
	return IToString(v)
}

// ExecToBytes returns a byte slice from a function exection.
func ExecToBytes(fn Function, ctx FunctionContext) []byte {
	v, err := fn.Exec(ctx)
	if err != nil {
		if rec, ok := err.(*ErrRecoverable); ok {
			return IToBytes(rec.Recovered)
		}
		return nil
	}
	return IToBytes(v)
}

//------------------------------------------------------------------------------
