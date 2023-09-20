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
)

// Closer is implemented by components that support stopping and cleaning up
// their underlying resources.
type Closer interface {
	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
