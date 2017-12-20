// Copyright (c) 2017 Ashley Jeffs
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

package output

import (
	"errors"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

type mockOutput struct {
	msgs <-chan types.Message
	res  chan types.Response
}

func (m *mockOutput) StartReceiving(msgs <-chan types.Message) error {
	m.msgs = msgs
	return nil
}

func (m *mockOutput) ResponseChan() <-chan types.Response {
	return m.res
}

func (m *mockOutput) CloseAsync() {
	// NOT EXPECTING TO HIT THIS
}

func (m *mockOutput) WaitForClose(dur time.Duration) error {
	select {
	case _, open := <-m.msgs:
		if open {
			return errors.New("Messages chan still open")
		}
		close(m.res)
	case <-time.After(dur):
		return errors.New("timed out")
	}
	return nil
}

//------------------------------------------------------------------------------

type mockPipe struct {
	msgsIn <-chan types.Message

	msgs chan types.Message
	res  chan types.Response

	resBack <-chan types.Response
}

func (m *mockPipe) StartListening(res <-chan types.Response) error {
	m.resBack = res
	return nil
}

func (m *mockPipe) StartReceiving(msgs <-chan types.Message) error {
	m.msgsIn = msgs
	return nil
}

func (m *mockPipe) MessageChan() <-chan types.Message {
	return m.msgs
}

func (m *mockPipe) ResponseChan() <-chan types.Response {
	return m.res
}

func (m *mockPipe) CloseAsync() {
	close(m.msgs)
	close(m.res)
}

func (m *mockPipe) WaitForClose(time.Duration) error {
	return errors.New("Not expecting to see this")
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockOut := &mockOutput{res: make(chan types.Response)}
	mockPi := &mockPipe{
		msgs: make(chan types.Message),
		res:  make(chan types.Response),
	}

	newOutput, err := WrapWithPipeline(mockOut, func() (pipeline.Type, error) {
		return nil, errors.New("Nope")
	})

	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newOutput, err = WrapWithPipeline(mockOut, func() (pipeline.Type, error) {
		return mockPi, nil
	})

	if newOutput.ResponseChan() != mockPi.res {
		t.Error("Wrong response chan in new output type")
	}

	dudMsgChan := make(chan types.Message)
	if err = newOutput.StartReceiving(dudMsgChan); err != nil {
		t.Error(err)
	}

	if mockPi.msgsIn != dudMsgChan {
		t.Error("Wrong message chan in mock pipe")
	}

	if mockOut.res != mockPi.resBack {
		t.Error("Wrong response chan in mock output")
	}

	if mockOut.msgs != mockPi.msgs {
		t.Error("Wrong messages chan in mock pipe")
	}

	newOutput.CloseAsync()
	if err = newOutput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOut.res:
		if open {
			t.Error("mock output is still open after close")
		}
	default:
		t.Error("neither type was closed")
	}
}

//------------------------------------------------------------------------------
