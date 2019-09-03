// Copyright (c) 2019 Ashley Jeffs
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

package roundtrip

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Writer is a writer implementation that adds messages to a ResultStore located
// in the context of the first message part of each batch. This is essentially a
// mechanism that returns the result of a pipeline directly back to the origin
// of the message.
type Writer struct{}

// Connect is a noop.
func (s Writer) Connect() error {
	return nil
}

// Write a message batch to a ResultStore located in the first message of the
// batch.
func (s Writer) Write(msg types.Message) error {
	return SetAsResponse(msg)
}

// CloseAsync is a noop.
func (s Writer) CloseAsync() {}

// WaitForClose is a noop.
func (s Writer) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
