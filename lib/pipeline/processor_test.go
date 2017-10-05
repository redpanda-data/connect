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

package pipeline

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

type mockMsgProcessor struct {
	drop bool
}

func (m *mockMsgProcessor) ProcessMessage(msg *types.Message) (*types.Message, bool) {
	if m.drop {
		return nil, false
	}
	newMsg := types.NewMessage()
	newMsg.Parts = [][]byte{
		[]byte("foo"),
		[]byte("bar"),
	}
	return &newMsg, true
}

func TestMemoryBuffer(t *testing.T) {
	mockProc := &mockMsgProcessor{drop: true}

	proc := NewProcessor(
		mockProc,
		log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
		metrics.DudType{},
	)

	msgChan, resChan := make(chan types.Message), make(chan types.Response)

	if err := proc.StartListening(resChan); err != nil {
		t.Error(err)
	}
	if err := proc.StartListening(resChan); err == nil {
		t.Error("Expected error from dupe listening")
	}
	if err := proc.StartReceiving(msgChan); err != nil {
		t.Error(err)
	}
	if err := proc.StartReceiving(msgChan); err == nil {
		t.Error("Expected error from dupe receiving")
	}

	msg := types.NewMessage()
	msg.Parts = [][]byte{
		[]byte(`one`),
		[]byte(`two`),
	}

	// First message should be dropped and return immediately
	select {
	case msgChan <- msg:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case _, open := <-proc.MessageChan():
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Message was not dropped")
		}
	case res, open := <-proc.ResponseChan():
		if !open {
			t.Error("Closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Do not drop next message
	mockProc.drop = false

	// Send message
	select {
	case msgChan <- msg:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case procMsg, open := <-proc.MessageChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procMsg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case res, open := <-proc.ResponseChan():
		if !open {
			t.Error("Closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		} else {
			t.Error("Message was dropped")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond with error
	errTest := errors.New("This is a test")
	select {
	case resChan <- types.NewSimpleResponse(errTest):
	case _, open := <-proc.ResponseChan():
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Premature response prop")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Receive error
	select {
	case res, open := <-proc.ResponseChan():
		if !open {
			t.Error("Closed early")
		} else if exp, act := errTest, res.Error(); exp != act {
			t.Errorf("Wrong response returned: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Send message
	select {
	case msgChan <- msg:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case procMsg, open := <-proc.MessageChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procMsg.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond without error
	select {
	case resChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Receive error
	select {
	case res, open := <-proc.ResponseChan():
		if !open {
			t.Error("Closed early")
		} else if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
