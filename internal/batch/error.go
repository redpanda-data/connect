// Package batch contains internal utilities for interacting with message
// batches.
package batch

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// Error is an error type that also allows storing granular errors for each
// message of a batch.
type Error struct {
	err          error
	erroredBatch message.Batch
	partErrors   map[int]error
}

// NewError creates a new batch-wide error, where it's possible to add granular
// errors for individual messages of the batch.
func NewError(msg message.Batch, err error) *Error {
	if berr, ok := err.(*Error); ok {
		err = berr.Unwrap()
	}
	return &Error{
		err:          err,
		erroredBatch: msg,
	}
}

// Failed stores an error state for a particular message of a batch. Returns a
// pointer to the underlying error, allowing the method to be chained.
//
// If Failed is not called then all messages are assumed to have failed. If it
// is called at least once then all message indexes that aren't explicitly
// failed are assumed to have been processed successfully.
func (e *Error) Failed(i int, err error) *Error {
	if len(e.erroredBatch) <= i {
		return e
	}
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

// XErroredBatch returns the underlying batch associated with the error.
func (e *Error) XErroredBatch() message.Batch {
	return e.erroredBatch
}

// WalkParts applies a closure to each message that was part of the request that
// caused this error. The closure is provided the message part index, a pointer
// to the part, and its individual error, which may be nil if the message itself
// was processed successfully. The closure returns a bool which indicates
// whether the iteration should be continued.
//
// Important! The order to parts walked is not guaranteed to match that of the
// source batch.
func (e *Error) WalkParts(sourceSortGroup *message.SortGroup, sourceBatch message.Batch, fn func(int, *message.Part, error) bool) {
	_ = e.erroredBatch.Iter(func(i int, p *message.Part) error {
		index := sourceSortGroup.GetIndex(p)
		if index < 0 || index >= len(sourceBatch) {
			return nil
		}

		var err error
		if e.partErrors == nil {
			err = e.err
		} else {
			err = e.partErrors[i]
		}
		if !fn(index, sourceBatch[index], err) {
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
