// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package types

import "fmt"

//------------------------------------------------------------------------------

// Response is a response from an output, agent or broker that confirms the
// input of successful message receipt.
type Response interface {
	Error() error
	ErrorMap() map[int]error
}

//------------------------------------------------------------------------------

// MappedResponse is returned by a broker to provide a map of errors
// representing agent errors.
type MappedResponse struct {
	Errors map[int]error
}

// Error returns nil if no errors are present, otherwise a concatenated blob of
// errors.
func (b MappedResponse) Error() error {
	if len(b.Errors) > 0 {
		return fmt.Errorf("%v", b.Errors)
	}
	return nil
}

// ErrorMap returns a map of errors returned by agents, represented by index.
func (b MappedResponse) ErrorMap() map[int]error {
	if len(b.Errors) > 0 {
		return b.Errors
	}
	return nil
}

// NewMappedResponse returns a response tailored for a broker (with n agents).
func NewMappedResponse() MappedResponse {
	return MappedResponse{
		Errors: make(map[int]error),
	}
}

//------------------------------------------------------------------------------

// SimpleResponse is returned by an output or agent to provide a single return
// message.
type SimpleResponse struct {
	err error
}

// Error returns the underlying error.
func (o SimpleResponse) Error() error {
	return o.err
}

// ErrorMap returns nil.
func (o SimpleResponse) ErrorMap() map[int]error {
	return nil
}

// NewSimpleResponse returns a response with an error (nil error signals
// successful receipt).
func NewSimpleResponse(err error) SimpleResponse {
	return SimpleResponse{
		err: err,
	}
}

//------------------------------------------------------------------------------
