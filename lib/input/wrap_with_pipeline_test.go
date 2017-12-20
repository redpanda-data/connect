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

package input

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

type mockInput struct {
	msgs chan types.Message
	res  <-chan types.Response
}

func (m *mockInput) StartListening(res <-chan types.Response) error {
	m.res = res
	return nil
}

func (m *mockInput) MessageChan() <-chan types.Message {
	return m.msgs
}

func (m *mockInput) CloseAsync() {
	close(m.msgs)
}

func (m *mockInput) WaitForClose(time.Duration) error {
	return errors.New("wasnt expecting to ever see this tbh")
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
	return nil
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockIn := &mockInput{msgs: make(chan types.Message)}
	mockPi := &mockPipe{
		msgs: make(chan types.Message),
		res:  make(chan types.Response),
	}

	newInput, err := WrapWithPipeline(mockIn, func() (pipeline.Type, error) {
		return nil, errors.New("Nope")
	})

	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err = WrapWithPipeline(mockIn, func() (pipeline.Type, error) {
		return mockPi, nil
	})

	if newInput.MessageChan() != mockPi.msgs {
		t.Error("Wrong message chan in new input type")
	}

	dudResChan := make(chan types.Response)
	if err = newInput.StartListening(dudResChan); err != nil {
		t.Error(err)
	}

	if mockPi.resBack != dudResChan {
		t.Error("Wrong response chan in mock pipe")
	}

	if mockIn.res != mockPi.res {
		t.Error("Wrong response chan in mock input")
	}

	if mockIn.msgs != mockPi.msgsIn {
		t.Error("Wrong messages chan in mock pipe")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockIn.msgs:
		if open {
			t.Error("mock input is still open after close")
		}
	case _, open := <-mockPi.msgs:
		if !open {
			t.Error("mock pipe is not open after close")
		}
	default:
		t.Error("neither type was closed")
	}
}

func TestBasicWrapMultiPipelines(t *testing.T) {
	mockIn := &mockInput{msgs: make(chan types.Message)}
	mockPi1 := &mockPipe{
		msgs: make(chan types.Message),
		res:  make(chan types.Response),
	}
	mockPi2 := &mockPipe{
		msgs: make(chan types.Message),
		res:  make(chan types.Response),
	}

	newInput, err := WrapWithPipelines(mockIn, func() (pipeline.Type, error) {
		return nil, errors.New("Nope")
	})

	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err = WrapWithPipelines(mockIn, func() (pipeline.Type, error) {
		return mockPi1, nil
	}, func() (pipeline.Type, error) {
		return mockPi2, nil
	})

	if newInput.MessageChan() != mockPi2.msgs {
		t.Error("Wrong message chan in new input type")
	}
	if mockPi2.msgsIn != mockPi1.msgs {
		t.Error("Wrong message chan in mock pipe 2")
	}

	dudResChan := make(chan types.Response)
	if err = newInput.StartListening(dudResChan); err != nil {
		t.Error(err)
	}

	if mockPi2.resBack != dudResChan {
		t.Error("Wrong response chan in mock pipe 2")
	}

	if mockIn.res != mockPi1.res {
		t.Error("Wrong response chan in mock input")
	}
	if mockPi2.res != mockPi1.resBack {
		t.Error("Wrong response chan in mock pipe 1")
	}

	if mockIn.msgs != mockPi1.msgsIn {
		t.Error("Wrong messages chan in mock pipe 1")
	}
	if mockPi1.msgs != mockPi2.msgsIn {
		t.Error("Wrong messages chan in mock pipe 2")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockIn.msgs:
		if open {
			t.Error("mock input is still open after close")
		}
	case _, open := <-mockPi1.msgs:
		if !open {
			t.Error("mock pipe is not open after close")
		}
	case _, open := <-mockPi2.msgs:
		if !open {
			t.Error("mock pipe is not open after close")
		}
	default:
		t.Error("neither type was closed")
	}
}

//------------------------------------------------------------------------------

var errMockProc = errors.New("mock proc error")

type mockProc struct {
	value string
}

func (m mockProc) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	if string(msg.Parts[0]) == m.value {
		return nil, types.NewSimpleResponse(errMockProc), false
	}
	return msg, nil, true
}

//------------------------------------------------------------------------------

func TestBasicWrapProcessors(t *testing.T) {
	mockIn := &mockInput{msgs: make(chan types.Message)}

	l := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	s := metrics.DudType{}

	pipe1 := pipeline.NewProcessor(l, s, mockProc{value: "foo"})
	pipe2 := pipeline.NewProcessor(l, s, mockProc{value: "bar"})

	newInput, err := WrapWithPipelines(mockIn, func() (pipeline.Type, error) {
		return pipe1, nil
	}, func() (pipeline.Type, error) {
		return pipe2, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan types.Response)
	if err = newInput.StartListening(resChan); err != nil {
		t.Error(err)
	}

	msg := types.NewMessage()
	msg.Parts = [][]byte{[]byte("foo")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	msg = types.NewMessage()
	msg.Parts = [][]byte{[]byte("bar")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should also be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	msg = types.NewMessage()
	msg.Parts = [][]byte{[]byte("baz")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should not be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		t.Errorf("Unexpected response: %v", res.Error())
	case newMsg, open := <-newInput.MessageChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(newMsg.Parts[0]); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")
	select {
	case resChan <- types.NewSimpleResponse(errFailed):
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if exp, act := errFailed, res.Error(); exp != act {
			t.Errorf("Unexpected response: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestBasicWrapDoubleProcessors(t *testing.T) {
	mockIn := &mockInput{msgs: make(chan types.Message)}

	l := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	s := metrics.DudType{}

	pipe1 := pipeline.NewProcessor(l, s, mockProc{value: "foo"}, mockProc{value: "bar"})

	newInput, err := WrapWithPipelines(mockIn, func() (pipeline.Type, error) {
		return pipe1, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan types.Response)
	if err = newInput.StartListening(resChan); err != nil {
		t.Error(err)
	}

	msg := types.NewMessage()
	msg.Parts = [][]byte{[]byte("foo")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	msg = types.NewMessage()
	msg.Parts = [][]byte{[]byte("bar")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should also be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	msg = types.NewMessage()
	msg.Parts = [][]byte{[]byte("baz")}

	select {
	case mockIn.msgs <- msg:
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should not be discarded
	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		t.Errorf("Unexpected response: %v", res.Error())
	case newMsg, open := <-newInput.MessageChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(newMsg.Parts[0]); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")
	select {
	case resChan <- types.NewSimpleResponse(errFailed):
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	select {
	case res, open := <-mockIn.res:
		if !open {
			t.Error("Channel was closed")
		}
		if exp, act := errFailed, res.Error(); exp != act {
			t.Errorf("Unexpected response: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
