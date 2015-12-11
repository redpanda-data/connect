/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package types

import (
	"errors"
	"fmt"
)

//--------------------------------------------------------------------------------------------------

// Errors used throughout the codebase
var (
	ErrTimeout    = errors.New("action timed out")
	ErrChanClosed = errors.New("channel was closed unexpectedly")
	ErrTypeClosed = errors.New("type was closed")

	ErrInvalidBufferType = errors.New("buffer type was not recognised")
	ErrInvalidInputType  = errors.New("input type was not recognised")
	ErrInvalidOutputType = errors.New("output type was not recognised")

	ErrInvalidZMQType = errors.New("invalid ZMQ socket type")

	// ErrAlreadyStarted - When an input or output type gets started a second time.
	ErrAlreadyStarted = errors.New("type has already been started")

	ErrBadMessageBytes = errors.New("serialised message bytes were in unexpected format")
	ErrBlockCorrupted  = errors.New("serialised messages block was in unexpected format")
)

//--------------------------------------------------------------------------------------------------

// ErrUnexpectedHTTPRes - Error returned when an HTTP request returned an unexpected response.
type ErrUnexpectedHTTPRes struct {
	Code int
	S    string
}

// Error - Returns the Error string.
func (e ErrUnexpectedHTTPRes) Error() string {
	return fmt.Sprintf("HTTP request returned unexpected response code (%v): %v", e.Code, e.S)
}

//--------------------------------------------------------------------------------------------------
