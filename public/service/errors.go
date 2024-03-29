package service

import (
	"errors"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var (
	// ErrNotConnected is returned by inputs and outputs when their Read or
	// Write methods are called and the connection that they maintain is lost.
	// This error prompts the upstream component to call Connect until the
	// connection is re-established.
	ErrNotConnected = errors.New("not connected")

	// ErrEndOfInput is returned by inputs that have exhausted their source of
	// data to the point where subsequent Read calls will be ineffective. This
	// error prompts the upstream component to gracefully terminate the
	// pipeline.
	ErrEndOfInput = errors.New("end of input")

	// ErrEndOfBuffer is returned by a buffer Read/ReadBatch method when the
	// contents of the buffer has been emptied and the source of the data is
	// ended (as indicated by EndOfInput). This error prompts the upstream
	// component to gracefully terminate the pipeline.
	ErrEndOfBuffer = errors.New("end of buffer")
)

// ErrBackOff is an error that plugins can optionally wrap another error with
// which instructs upstream components to wait for a specified period of time
// before retrying the errored call.
//
// Not all plugin methods support this error, for a list refer to the
// documentation of NewErrBackOff.
type ErrBackOff struct {
	Err  error
	Wait time.Duration
}

// NewErrBackOff wraps an error with a specified time to wait. For specific
// plugin methods this will instruct upstream components to wait by the
// specified amount of time before re-attempting the errored call.
//
// NOTE: ErrBackOff is opt-in for upstream components and therefore only a
// subset of plugin calls will respect this error. Currently the following
// methods are known to support ErrBackOff:
//
// - Input.Connect
// - BatchInput.Connect
// - Output.Connect
// - BatchOutput.Connect.
func NewErrBackOff(err error, wait time.Duration) *ErrBackOff {
	return &ErrBackOff{err, wait}
}

// Error returns the Error string.
func (e *ErrBackOff) Error() string {
	return e.Err.Error()
}

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
	ib := make(message.Batch, len(b))
	for i, m := range b {
		ib[i] = m.part
	}
	batchErr := batch.NewError(ib, headline)
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

// WalkMessagesIndexedBy applies a closure to each message of a batch that is
// included in this batch error. A batch error represents errors that occurred
// to only a subset of a batch of messages, in which case it is possible to use
// this error in order to avoid re-processing or re-delivering messages that
// didn't fail.
//
// However, the shape of the batch of messages at the time the errors occurred
// may differ significantly from the batch known by the component receiving this
// error. For example a processor that dispatches a batch to a list of child
// processors may receive a batch error that occurred after filtering and
// re-ordering has occurred to the batch. In such cases it is not possible to
// simply inspect the indexes of errored messages in order to associate them
// with the original batch as those indexes could have changed.
//
// Therefore, in order to solve this particular use case it is possible to
// create a batch indexer before dispatching the batch to the child components.
// Then, when a batch error is received WalkMessagesIndexedBy can be used as way
// to walk the errored messages with the indexes (and message contents) of the
// original batch.
//
// Important! The order of messages walked is not guaranteed to match that of
// the source batch. It is also possible for any given index to be represented
// zero, one or more times.
func (err *BatchError) WalkMessagesIndexedBy(s *Indexer, fn func(int, *Message, error) bool) {
	parts := make(message.Batch, len(s.sourceBatch))
	for i, m := range s.sourceBatch {
		parts[i] = m.part
	}
	err.wrapped.WalkPartsBySource(s.wrapped, parts, func(i int, p *message.Part, err error) bool {
		var m *Message
		if i >= 0 && i < len(s.sourceBatch) {
			m = s.sourceBatch[i]
		} else {
			m = &Message{part: p}
		}
		return fn(i, m, err)
	})
}

// WalkMessages applies a closure to each message that was part of the request
// that caused this error. The closure is provided the message index, a pointer
// to the message, and its individual error, which may be nil if the message
// itself was processed successfully. The closure should return a bool which
// indicates whether the iteration should be continued.
//
// Deprecated: This method is harmful and should be avoided as indexes are not
// guaranteed to match a hypothetical origin batch that they might be compared
// against. Use WalkMessagesIndexedBy instead.
func (err *BatchError) WalkMessages(fn func(int, *Message, error) bool) {
	err.wrapped.WalkPartsNaively(func(i int, p *message.Part, err error) bool {
		return fn(i, &Message{part: p}, err)
	})
}

// IndexedErrors returns the number of indexed errors that have been registered
// within a walkable error.
func (err *BatchError) IndexedErrors() int {
	return err.wrapped.IndexedErrors()
}

// Error returns the underlying error message.
func (err *BatchError) Error() string {
	return err.wrapped.Error()
}

func (err *BatchError) Unwrap() error {
	return err.wrapped
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

func publicToInternalErr(err error) error {
	if err == nil {
		return nil
	}

	var e *ErrBackOff
	if errors.As(err, &e) {
		return &component.ErrBackOff{Err: publicToInternalErr(e.Err), Wait: e.Wait}
	}

	var bErr *BatchError
	if errors.As(err, &bErr) {
		return bErr.wrapped
	}

	if errors.Is(err, ErrEndOfInput) {
		return component.ErrTypeClosed
	}
	if errors.Is(err, ErrEndOfBuffer) {
		return component.ErrTypeClosed
	}
	if errors.Is(err, ErrNotConnected) {
		return component.ErrNotConnected
	}
	return err
}
