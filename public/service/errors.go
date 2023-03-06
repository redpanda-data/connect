package service

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// BatchError groups the errors that were encountered while processing a
// collection (usually a batch) of messages and provides methods to iterate
// over these errors.
type BatchError struct {
	wrapped *batch.Error
}

// NewBatchError creates a BatchError that can be returned by batched outputs.
// The advantage of doing so is that nacks and retries can potentially be
// granularly dealt out in cases where only a subset of the batch failed.
//
// A headline error must be supplied which will be exposed when upstream
// components do not support granular batch errors.
func NewBatchError(b MessageBatch, headline error) *BatchError {
	ibatch := make(message.Batch, len(b))
	for i, m := range b {
		ibatch[i] = m.part
	}

	batchErr := batch.NewError(ibatch, headline)
	return &BatchError{wrapped: batchErr}
}

// Failed stores an error state for a particular message of a batch. Returns a
// pointer to the underlying error, allowing the method to be chained.
//
// If Failed is not called then all messages are assumed to have failed. If it
// is called at least once then all message indexes that aren't explicitly
// failed are assumed to have been processed successfully.
func (err *BatchError) Failed(i int, merr error) *BatchError {
	_ = err.wrapped.Failed(i, merr)
	return err
}

// WalkMessages applies a closure to each message that was part of the request
// that caused this error. The closure is provided the message index, a pointer
// to the message, and its individual error, which may be nil if the message
// itself was processed successfully. The closure should return a bool which
// indicates whether the iteration should be continued.
func (err *BatchError) WalkMessages(fn func(int, *Message, error) bool) {
	sortGroup, iBatch := message.NewSortGroup(err.wrapped.XErroredBatch())
	err.wrapped.WalkParts(sortGroup, iBatch, func(i int, p *message.Part, err error) bool {
		return fn(i, &Message{part: p}, err)
	})
}

// IndexedErrors returns the number of indexed errors that have been registered
// within a walkable error.
func (err *BatchError) IndexedErrors() int {
	return err.wrapped.IndexedErrors()
}

// Error returns the underlying error message
func (err *BatchError) Error() string {
	return err.wrapped.Error()
}

// If the provided error is not nil and can be cast to an internal batch error
// we return a public batch error.
func toPublicBatchError(err error) error {
	var bErr *batch.Error
	if err != nil && errors.As(err, &bErr) {
		err = &BatchError{wrapped: bErr}
	}
	return err
}

// If the provided error is not nil and can be cast to a public batch error we
// return the internal batch error.
func fromPublicBatchError(err error) error {
	var bErr *BatchError
	if err != nil && errors.As(err, &bErr) {
		err = bErr.wrapped
	}
	return err
}
