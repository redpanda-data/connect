package types

import (
	"errors"
	"fmt"
)

//------------------------------------------------------------------------------

// Errors used throughout the codebase
var (
	ErrTimeout    = errors.New("action timed out")
	ErrChanClosed = errors.New("channel was closed unexpectedly")
	ErrTypeClosed = errors.New("type was closed")

	ErrNotConnected = errors.New("not connected to target source or sink")

	ErrInvalidProcessorType = errors.New("processor type was not recognised")
	ErrInvalidCacheType     = errors.New("cache type was not recognised")
	ErrInvalidRateLimitType = errors.New("rate_limit type was not recognised")
	ErrInvalidConditionType = errors.New("condition type was not recognised")
	ErrInvalidBufferType    = errors.New("buffer type was not recognised")
	ErrInvalidInputType     = errors.New("input type was not recognised")
	ErrInvalidOutputType    = errors.New("output type was not recognised")

	ErrInvalidZMQType        = errors.New("invalid ZMQ socket type")
	ErrInvalidScaleProtoType = errors.New("invalid Scalability Protocols socket type")

	// ErrAlreadyStarted is returned when an input or output type gets started a
	// second time.
	ErrAlreadyStarted = errors.New("type has already been started")

	ErrMessagePartNotExist = errors.New("target message part does not exist")
	ErrBadMessageBytes     = errors.New("serialised message bytes were in unexpected format")
	ErrBlockCorrupted      = errors.New("serialised messages block was in unexpected format")

	ErrNoAck = errors.New("failed to receive acknowledgement")
)

//------------------------------------------------------------------------------

// BatchError represents a general error across an entire batch of messages that
// can optionally be broken down into errors at the individual message level,
// identified by their index in the batch.
type BatchError struct {
	err         error
	indexErrors map[int]error
}

// NewBatchError creates a fresh batch error from a general error message. Under
// normal use this root error will be provided. Once a batch error is
// initialized it is possible to add index specific errors with AddErrAt.
func NewBatchError(err error) *BatchError {
	return &BatchError{
		err:         err,
		indexErrors: map[int]error{},
	}
}

// AddErrAt adds an error for a message at a specific index. If an error for the
// given index already exists it is overridden. A reference to the BatchError is
// returned for convenient chaining.
func (b *BatchError) AddErrAt(i int, err error) *BatchError {
	b.indexErrors[i] = err
	return b
}

// IndexedErrors returns a map of message indexes to their individual errors.
func (b *BatchError) IndexedErrors() map[int]error {
	return b.indexErrors
}

// Error implements the common error interface.
func (b *BatchError) Error() string {
	return b.err.Error()
}

// Unwrap returns the underlying common error.
func (b *BatchError) Unwrap() error {
	return b.err
}

//------------------------------------------------------------------------------

// Manager errors
var (
	ErrInputNotFound     = errors.New("input not found")
	ErrCacheNotFound     = errors.New("cache not found")
	ErrConditionNotFound = errors.New("condition not found")
	ErrProcessorNotFound = errors.New("processor not found")
	ErrRateLimitNotFound = errors.New("rate limit not found")
	ErrOutputNotFound    = errors.New("output not found")
	ErrPluginNotFound    = errors.New("plugin not found")
	ErrKeyAlreadyExists  = errors.New("key already exists")
	ErrKeyNotFound       = errors.New("key does not exist")
	ErrPipeNotFound      = errors.New("pipe was not found")
)

//------------------------------------------------------------------------------

// Buffer errors
var (
	ErrMessageTooLarge = errors.New("message body larger than buffer space")
)

//------------------------------------------------------------------------------

// ErrUnexpectedHTTPRes is an error returned when an HTTP request returned an
// unexpected response.
type ErrUnexpectedHTTPRes struct {
	Code int
	S    string
}

// Error returns the Error string.
func (e ErrUnexpectedHTTPRes) Error() string {
	return fmt.Sprintf("HTTP request returned unexpected response code (%v): %v", e.Code, e.S)
}

//------------------------------------------------------------------------------
