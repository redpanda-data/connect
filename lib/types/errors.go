package types

import (
	"errors"
	"fmt"
	"strings"
)

// FailFlagKey is a metadata key used for flagging processor errors in Benthos.
// If a message part has any non-empty value for this metadata key then it will
// be interpretted as having failed a processor step somewhere in the pipeline.
//
// TODO: V4 stop hiding this as a metadata field
var FailFlagKey = "benthos_processing_failed"

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

	// ErrAlreadyStarted is returned when an input or output type gets started a
	// second time.
	ErrAlreadyStarted = errors.New("type has already been started")

	ErrNoAck = errors.New("failed to receive acknowledgement")
)

//------------------------------------------------------------------------------

// Manager errors
var (
	ErrInputNotFound     = errors.New("input not found")
	ErrCacheNotFound     = errors.New("cache not found")
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
	Body []byte
}

// Error returns the Error string.
func (e ErrUnexpectedHTTPRes) Error() string {
	body := strings.ReplaceAll(string(e.Body), "\n", "")
	return fmt.Sprintf("HTTP request returned unexpected response code (%v): %v, Error: %v", e.Code, e.S, body)
}
