// Package service provides a high level API for creating a Benthos service,
// registering custom plugin components, customizing the availability of native
// components, and running it.
//
// In order to add custom Bloblang functions and methods with plugins use the
// bloblang package.
//
// WARNING: THIS PACKAGE IS EXPERIMENTAL, AND THEREFORE SUBJECT TO BREAKING
// CHANGES OUTSIDE OF MAJOR VERSION RELEASES.
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
)

// Closer is implemented by components that support stopping and cleaning up
// their underlying resources.
type Closer interface {
	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
