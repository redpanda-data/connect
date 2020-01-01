// Package log contains utilities for logging in a modular interface. This
// package can be used to wrap a third party library.
package log

import "errors"

// Errors used throughout the package.
var (
	ErrClientNil = errors.New("the client pointer was nil")
)
