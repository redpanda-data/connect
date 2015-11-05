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

package buffer

import (
	"time"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// MockType - Implements the buffer.Type interface.
type MockType struct {
	Messages  chan types.Message
	Responses chan types.Response
	Errors    chan []error
}

// SetResponseChan - Sets the channel used for reading responses.
func (m *MockType) SetResponseChan(res <-chan types.Response) {
}

// SetMessageChan - Sets the read channel. This implementation is NOT thread safe.
func (m *MockType) SetMessageChan(msgs <-chan types.Message) {
}

// ResponseChan - Returns the errors channel.
func (m *MockType) ResponseChan() <-chan types.Response {
	return m.Responses
}

// MessageChan - Returns the messages channel.
func (m *MockType) MessageChan() <-chan types.Message {
	return m.Messages
}

// ErrorsChan - Returns the errors channel
func (m *MockType) ErrorsChan() <-chan []error {
	return m.Errors
}

// CloseAsync - Does nothing.
func (m MockType) CloseAsync() {
	// Do nothing
}

// WaitForClose - Does nothing.
func (m MockType) WaitForClose(time.Duration) error {
	// Do nothing
	return nil
}

//--------------------------------------------------------------------------------------------------
