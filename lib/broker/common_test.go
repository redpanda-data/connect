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

package broker

import (
	"errors"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/log"
)

//------------------------------------------------------------------------------

var logConfig = log.LoggerConfig{
	LogLevel: "NONE",
}

//------------------------------------------------------------------------------

// MockInputType implements the input.Type interface.
type MockInputType struct {
	MsgChan chan types.Message
	ResChan <-chan types.Response
}

// StartListening sets the channel used for reading responses.
func (m *MockInputType) StartListening(resChan <-chan types.Response) error {
	m.ResChan = resChan
	return nil
}

// MessageChan returns the messages channel.
func (m *MockInputType) MessageChan() <-chan types.Message {
	return m.MsgChan
}

// CloseAsync does nothing.
func (m MockInputType) CloseAsync() {
	close(m.MsgChan)
}

// WaitForClose does nothing.
func (m MockInputType) WaitForClose(t time.Duration) error {
	select {
	case _, open := <-m.MsgChan:
		if open {
			return errors.New("received unexpected message")
		}
	case <-time.After(t):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// MockOutputType implements the output.Type interface.
type MockOutputType struct {
	ResChan chan types.Response
	MsgChan <-chan types.Message
}

// StartReceiving sets the read channel. This implementation is NOT thread safe.
func (m *MockOutputType) StartReceiving(msgs <-chan types.Message) error {
	m.MsgChan = msgs
	return nil
}

// ResponseChan returns the errors channel.
func (m *MockOutputType) ResponseChan() <-chan types.Response {
	return m.ResChan
}

// CloseAsync does nothing.
func (m MockOutputType) CloseAsync() {
	close(m.ResChan)
}

// WaitForClose does nothing.
func (m MockOutputType) WaitForClose(t time.Duration) error {
	select {
	case _, open := <-m.ResChan:
		if open {
			return errors.New("received unexpected message")
		}
	case <-time.After(t):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
