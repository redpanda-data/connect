// Package batch contains internal utilities for interacting with message
// batches.
package batch

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/message"
)

// Error is an error type that also allows storing granular errors for each
// message of a batch.
type Error struct {
	err        error
	source     *message.Batch
	partErrors map[int]error
}

// NewError creates a new batch-wide error, where it's possible to add granular
// errors for individual messages of the batch.
func NewError(msg *message.Batch, err error) *Error {
	if berr, ok := err.(*Error); ok {
		err = berr.Unwrap()
	}
	return &Error{
		err:    err,
		source: msg,
	}
}

// Failed stores an error state for a particular message of a batch. Returns a
// pointer to the underlying error, allowing with method to be chained.
//
// If Failed is not called then all messages are assumed to have failed. If it
// is called at least once then all message indexes that aren't explicitly
// failed are assumed to have been processed successfully.
func (e *Error) Failed(i int, err error) *Error {
	if e.partErrors == nil {
		e.partErrors = make(map[int]error)
	}
	e.partErrors[i] = err
	return e
}

// IndexedErrors returns the number of indexed errors that have been registered
// for the batch.
func (e *Error) IndexedErrors() int {
	return len(e.partErrors)
}

// WalkableError is an interface implemented by batch errors that allows you to
// walk the messages of the batch and dig into the individual errors.
type WalkableError interface {
	WalkParts(fn func(int, *message.Part, error) bool)
	IndexedErrors() int
	error
}

// WalkParts applies a closure to each message that was part of the request that
// caused this error. The closure is provided the message part index, a pointer
// to the part, and its individual error, which may be nil if the message itself
// was processed successfully. The closure returns a bool which indicates
// whether the iteration should be continued.
func (e *Error) WalkParts(fn func(int, *message.Part, error) bool) {
	_ = e.source.Iter(func(i int, p *message.Part) error {
		var err error
		if e.partErrors == nil {
			err = e.err
		} else {
			err = e.partErrors[i]
		}
		if !fn(i, p, err) {
			return errors.New("stop")
		}
		return nil
	})
}

// Error implements the common error interface.
func (e *Error) Error() string {
	return e.err.Error()
}

// Unwrap returns the underlying common error.
func (e *Error) Unwrap() error {
	return e.err
}
