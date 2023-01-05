// Package query provides a parser for the right-hand side query part of the
// bloblang spec. This is useful as a separate package as it is used in
// isolation within interpolation functions.
package query

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/message"
)

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
	Get(p int) *message.Part
	Len() int
}

// MetaMsg provides access to the metadata of a message.
type MetaMsg interface {
	MetaSetMut(key string, value any)
	MetaGetStr(key string) string
	MetaGetMut(key string) (any, bool)
	MetaDelete(key string)
	MetaIterMut(f func(k string, v any) error) error
	MetaIterStr(f func(k, v string) error) error
}

// FunctionContext provides access to a range of query targets for functions to
// reference.
type FunctionContext struct {
	Maps     map[string]Function
	Vars     map[string]any
	Index    int
	MsgBatch MessageBatch

	// Reference new message being mapped
	NewMeta  MetaMsg
	NewValue *any

	valueFn    func() *any
	value      *any
	nextValue  *any
	namedValue *namedContextValue

	// Used to track how many maps we've entered.
	stackCount int
}

type namedContextValue struct {
	name  string
	value any
	next  *namedContextValue
}

// IncrStackCount increases the count stored in the function context of how many
// maps we've entered and returns the current count.
func (ctx FunctionContext) IncrStackCount() (FunctionContext, int) { //nolint: gocritic // Ignore unnamedResult false positive
	ctx.stackCount++
	return ctx, ctx.stackCount
}

// NamedValue returns the value of a named context if it exists.
func (ctx FunctionContext) NamedValue(name string) (any, bool) {
	current := ctx.namedValue
	for current != nil {
		if current.name == name {
			return current.value, true
		}
		current = current.next
	}
	return nil, false
}

// WithNamedValue returns a FunctionContext with a named value.
func (ctx FunctionContext) WithNamedValue(name string, value any) FunctionContext {
	previous := ctx.namedValue
	ctx.namedValue = &namedContextValue{
		name:  name,
		value: value,
		next:  previous,
	}
	return ctx
}

// Value returns a lazily evaluated context value. A context value is not always
// available and can therefore be nil.
func (ctx FunctionContext) Value() *any {
	if ctx.value != nil {
		return ctx.value
	}
	if ctx.valueFn == nil {
		return nil
	}
	return ctx.valueFn()
}

// WithValueFunc returns a function context with a new value func.
func (ctx FunctionContext) WithValueFunc(fn func() *any) FunctionContext {
	ctx.valueFn = fn
	return ctx
}

// WithValue returns a function context with a new value.
func (ctx FunctionContext) WithValue(value any) FunctionContext {
	ctx.nextValue = ctx.value
	ctx.value = &value
	return ctx
}

// PopValue returns the current default value, and a function context with the
// top value removed from the context stack. If the value returned is the
// absolute root value function then the context returned is unchanged. If there
// is no current default value then a nil value is returned and the context
// returned is unchanged.
func (ctx FunctionContext) PopValue() (*any, FunctionContext) {
	retValue := ctx.Value()

	if ctx.nextValue != nil {
		ctx.value = ctx.nextValue
		ctx.nextValue = nil
	} else {
		ctx.value = nil
	}

	return retValue, ctx
}

//------------------------------------------------------------------------------

// ExecToString returns a string from a function execution.
func ExecToString(fn Function, ctx FunctionContext) (string, error) {
	v, err := fn.Exec(ctx)
	if err != nil {
		return "", err
	}
	return IToString(v), nil
}

// ExecToBytes returns a byte slice from a function execution.
func ExecToBytes(fn Function, ctx FunctionContext) ([]byte, error) {
	v, err := fn.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return IToBytes(v), nil
}
