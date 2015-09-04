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

package agent

import (
	"time"

	"github.com/jeffail/benthos/output"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// Unbuffered - An agent that wraps an output without buffering.
type Unbuffered struct {
	output output.Type

	messages chan types.Message
}

// NewUnbuffered - Create a new Unbuffered agent type.
func NewUnbuffered(out output.Type) *Unbuffered {
	u := Unbuffered{
		output:   out,
		messages: make(chan types.Message),
	}

	out.SetReadChan(u.messages)

	return &u
}

//--------------------------------------------------------------------------------------------------

// MessageChan - Returns the messages channel.
func (u *Unbuffered) MessageChan() chan<- types.Message {
	return u.messages
}

// ResponseChan - Returns the response channel.
func (u *Unbuffered) ResponseChan() <-chan output.Response {
	return u.output.ResponseChan()
}

// ErrorsChan - Returns nil since asynchronous errors won't occur.
func (u *Unbuffered) ErrorsChan() <-chan []error {
	return nil
}

// CloseAsync - Shuts down the unbuffered output and stops processing messages.
func (u *Unbuffered) CloseAsync() {
	u.output.CloseAsync()
	close(u.messages)
}

// WaitForClose - Blocks until the unbuffered output has closed down.
func (u *Unbuffered) WaitForClose(timeout time.Duration) error {
	return u.output.WaitForClose(timeout)
}

//--------------------------------------------------------------------------------------------------
