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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestPoolBasic(t *testing.T) {
	mockProc := &mockMsgProcessor{dropChan: make(chan bool)}

	go func() {
		mockProc.dropChan <- true
	}()

	constr := func() (Type, error) {
		return NewProcessor(
			log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
			metrics.DudType{},
			mockProc,
		), nil
	}

	proc, err := NewPool(
		constr, 1,
		log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
		metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err := proc.StartReceiving(tChan); err != nil {
		t.Fatal(err)
	}
	if err := proc.StartReceiving(tChan); err == nil {
		t.Error("Expected error from dupe receiving")
	}

	msg := types.NewMessage()
	msg.Parts = [][]byte{
		[]byte(`one`),
		[]byte(`two`),
	}

	// First message should be dropped and return immediately
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}
	select {
	case _, open := <-proc.TransactionChan():
		if !open {
			t.Fatal("Closed early")
		} else {
			t.Fatal("Message was not dropped")
		}

	case res, open := <-resChan:
		if !open {
			t.Fatal("Closed early")
		}
		if res.Error() != nil {
			// We don't expect our own error back since the workers are
			// decoupled
			t.Error(res.Error())
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Do not drop next message
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Receive message
	var procT types.Transaction
	var open bool
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Receive decoupled response
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else if res.Error() != nil {
			t.Error(res.Error())
		}
		// Expect decoupled response
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Respond with error
	errTest := errors.New("This is a test")
	select {
	case procT.ResponseChan <- types.NewSimpleResponse(errTest):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Receive message second attempt
	select {
	case procT, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Respond with no error this time
	select {
	case procT.ResponseChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Do not drop next message again
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Receive new message
	select {
	case procT, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.Parts; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Receive decoupled response
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		}
		if res.Error() != nil {
			t.Error(res.Error())
		}
		// Expect decoupled response
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	// Respond without error
	select {
	case procT.ResponseChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second * 5):
		t.Fatal("Timed out")
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

type mockMultiMsgProcessor struct {
	N int
}

func (m *mockMultiMsgProcessor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	var msgs []types.Message
	for i := 0; i < m.N; i++ {
		newMsg := types.NewMessage()
		newMsg.Parts = [][]byte{
			[]byte("foo"),
			[]byte("bar"),
		}
		msgs = append(msgs, newMsg)
	}
	return msgs, nil
}

func TestPoolMultiMsgs(t *testing.T) {
	mockProc := &mockMultiMsgProcessor{N: 3}

	constr := func() (Type, error) {
		return NewProcessor(
			log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
			metrics.DudType{},
			mockProc,
		), nil
	}

	proc, err := NewPool(
		constr, 1,
		log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}),
		metrics.DudType{},
	)
	if err != nil {
		t.Fatal(err)
	}

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)
	if err := proc.StartReceiving(tChan); err != nil {
		t.Fatal(err)
	}

	msg := types.NewMessage()
	msg.Parts = [][]byte{
		[]byte(`one`),
		[]byte(`two`),
	}

	for j := 0; j < 10; j++ {
		// Send message
		select {
		case tChan <- types.NewTransaction(msg, resChan):
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}

		// Receive decoupled response
		select {
		case res, open := <-resChan:
			if !open {
				t.Error("Closed early")
			} else if res.Error() != nil {
				t.Error(res.Error())
			}
			// Expect decoupled response
		case <-time.After(time.Second * 5):
			t.Fatal("Timed out")
		}

		for i := 0; i < mockProc.N; i++ {
			// Receive messages
			var procT types.Transaction
			var open bool
			select {
			case procT, open = <-proc.TransactionChan():
				if !open {
					t.Error("Closed early")
				}
				if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.Parts; !reflect.DeepEqual(exp, act) {
					t.Errorf("Wrong message received: %s != %s", act, exp)
				}
			case <-time.After(time.Second * 5):
				t.Fatal("Timed out")
			}

			// Respond with no error
			select {
			case procT.ResponseChan <- types.NewSimpleResponse(nil):
			case <-time.After(time.Second * 5):
				t.Fatal("Timed out")
			}
		}
	}

	proc.CloseAsync()
	if err := proc.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
