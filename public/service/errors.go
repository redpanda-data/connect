package service

import (
	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// WalkableError groups the errors that were encountered while processing a
// collection (usually a batch) of messages and provides methods to iterate
// over these errors.
type WalkableError struct {
	wrapped batch.WalkableError
}

// MockWalkableError creates a WalkableError that can be used for testing
// purposes. The list of message errors can contain nil values to skip over
// messages that shouldn't have errors. Additionally, the list of errors does
// not have to be the same length as the number of messages in the batch. If
// there are more errors than batch messages, then the extra errors are
// discarded.
func MockWalkableError(b MessageBatch, headline error, messageErrors ...error) *WalkableError {
	ibatch := make(message.Batch, len(b))
	for i, m := range b {
		ibatch[i] = m.part
	}

	batchErr := batch.NewError(ibatch, headline)
	for i, merr := range messageErrors {
		if i >= len(b) {
			break
		}
		if merr != nil {
			batchErr.Failed(i, merr)
		}
	}

	return &WalkableError{wrapped: batchErr}
}

// WalkMessages applies a closure to each message that was part of the request
// that caused this error. The closure is provided the message index, a pointer
// to the message, and its individual error, which may be nil if the message
// itself was processed successfully. The closure returns a bool which indicates
// whether the iteration should be continued.
func (err *WalkableError) WalkMessages(fn func(int, *Message, error) bool) {
	err.wrapped.WalkParts(func(i int, p *message.Part, err error) bool {
		return fn(i, &Message{part: p}, err)
	})
}

// IndexedErrors returns the number of indexed errors that have been registered
// within a walkable error.
func (err *WalkableError) IndexedErrors() int {
	return err.wrapped.IndexedErrors()
}

// Error returns the underlying error message
func (err *WalkableError) Error() string {
	return err.wrapped.Error()
}
