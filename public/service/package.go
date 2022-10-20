// Package service provides a high level API for registering custom plugin
// components and executing either a standard Benthos CLI, or programmatically
// building isolated pipelines with a StreamBuilder API.
//
// For a video guide on Benthos plugins check out: https://youtu.be/uH6mKw-Ly0g
// And an example repo containing component plugins and tests can be found at:
// https://github.com/benthosdev/benthos-plugin-example
//
// In order to add custom Bloblang functions and methods use the
// ./public/bloblang package.
package service

import (
	"context"
	"errors"
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

	// ErrEmptyRead is returned by inputs that have attemtped to read from their
	// underlying source but received no new data. Unlike ErrEndOfInput, this
	// does not mean that the input exhausted its source of data and instead it's
	// worth attempting subsequent reads.
	ErrEmptyRead = errors.New("empty read from input")

	// ErrEndOfBuffer is returned by a buffer Read/ReadBatch method when the
	// contents of the buffer has been emptied and the source of the data is
	// ended (as indicated by EndOfInput). This error prompts the upstream
	// component to gracefully terminate the pipeline.
	ErrEndOfBuffer = errors.New("end of buffer")
)

// Closer is implemented by components that support stopping and cleaning up
// their underlying resources.
type Closer interface {
	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
