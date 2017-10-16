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

//------------------------------------------------------------------------------

// Response is a response from an output, agent or broker that confirms the
// input of successful message receipt.
type Response interface {
	// Error returns a non-nil error if the message failed to reach its
	// destination.
	Error() error

	// SkipAck indicates that even though there may not have been an error in
	// processing a message, it should not be acknowledged. If SkipAck is false
	// and Error is nil then all unacknowledged messages should be acknowledged
	// also.
	SkipAck() bool
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

// SkipAck indicates whether a successful message should be acknowledged.
func (o SimpleResponse) SkipAck() bool {
	return false
}

// NewSimpleResponse returns a response with an error (nil error signals
// successful receipt).
func NewSimpleResponse(err error) SimpleResponse {
	return SimpleResponse{
		err: err,
	}
}

//------------------------------------------------------------------------------

// UnacknowledgedResponse is a response type that indicates the message has
// reached its destination but should not be acknowledged.
type UnacknowledgedResponse struct{}

// Error returns the underlying error.
func (u UnacknowledgedResponse) Error() error { return nil }

// SkipAck indicates whether a successful message should be acknowledged.
func (u UnacknowledgedResponse) SkipAck() bool {
	return true
}

// NewUnacknowledgedResponse returns an UnacknowledgedResponse.
func NewUnacknowledgedResponse() UnacknowledgedResponse {
	return UnacknowledgedResponse{}
}

//------------------------------------------------------------------------------
