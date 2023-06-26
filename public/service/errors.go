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
	group   *message.SortGroup
	wrapped *batch.Error
}

// NewBatchError creates a BatchError that can be returned by batched outputs.
// The advantage of doing so is that nacks and retries can potentially be
// granularly dealt out in cases where only a subset of the batch failed.
//
// A headline error must be supplied which will be exposed when upstream
// components do not support granular batch errors.
func NewBatchError(b MessageBatch, headline error) *BatchError {
	ib := make(message.Batch, len(b))
	for i, m := range b {
		ib[i] = m.part
	}

	group, ibatch := message.NewSortGroup(ib)
	batchErr := batch.NewError(ibatch, headline)

	return &BatchError{group: group, wrapped: batchErr}
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
	b := err.wrapped.XErroredBatch()
	err.wrapped.WalkParts(err.group, b, func(i int, p *message.Part, err error) bool {
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

func (err *BatchError) Unwrap() error {
	return err.wrapped
}

// If the provided error is not nil and can be cast to an internal batch error
// we return a public batch error.
func toPublicBatchError(err error) error {
	// Modern Benthos components that use the public service API will return a
	// *service.BatchError type. We will preserve this if we encounter it.
	var target *BatchError
	if ok := errors.As(err, &target); ok {
		return err
	}

	var bErr *batch.Error
	if err != nil && errors.As(err, &bErr) {
		err = &BatchError{wrapped: bErr}
	}
	return err
}
