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
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

var errMockProc = errors.New("this is an error from mock processor")

type mockMsgProcessor struct {
	dropChan chan bool
}

func (m *mockMsgProcessor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	if drop := <-m.dropChan; drop {
		return nil, types.NewSimpleResponse(errMockProc)
	}
	newMsg := types.NewMessage([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

func TestProcessorPipeline(t *testing.T) {
	mockProc := &mockMsgProcessor{dropChan: make(chan bool)}

	// Drop first message
	go func() {
		mockProc.dropChan <- true
	}()

	proc := NewProcessor(
		log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
		metrics.DudType{},
		mockProc,
	)

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}
	if err := proc.Consume(tChan); err == nil {
		t.Error("Expected error from dupe listening")
	}

	msg := types.NewMessage([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	// First message should be dropped and return immediately
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case _, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Message was not dropped")
		}
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		}
		if res.Error() != errMockProc {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Do not drop next message
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var procT types.Transaction
	var open bool
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.GetAll(); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case res, open := <-resChan:
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
	case procT.ResponseChan <- types.NewSimpleResponse(errTest):
	case _, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Premature response prop")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Receive again
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.GetAll(); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case res, open := <-resChan:
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

	// Respond without error
	select {
	case procT.ResponseChan <- types.NewSimpleResponse(nil):
	case _, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Premature response prop")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Receive response
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Do not drop next message
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, procT.Payload.GetAll(); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond without error
	select {
	case procT.ResponseChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Receive error
	select {
	case res, open := <-resChan:
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

type mockMultiMsgProcessor struct {
	N int
}

func (m *mockMultiMsgProcessor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	var msgs []types.Message
	for i := 0; i < m.N; i++ {
		newMsg := types.NewMessage([][]byte{
			[]byte(fmt.Sprintf("test%v", i)),
		})
		msgs = append(msgs, newMsg)
	}
	return msgs, nil
}

func TestProcessorMultiMsgs(t *testing.T) {
	mockProc := &mockMultiMsgProcessor{N: 3}

	proc := NewProcessor(
		log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
		metrics.DudType{},
		mockProc,
	)

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}

	// Send message
	select {
	case tChan <- types.NewTransaction(types.NewMessage(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	expMsgs := map[string]struct{}{}
	for i := 0; i < mockProc.N; i++ {
		expMsgs[fmt.Sprintf("test%v", i)] = struct{}{}
	}

	resChans := []chan<- types.Response{}

	// Receive N messages
	for i := 0; i < mockProc.N; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			if !open {
				t.Error("Closed early")
			}
			act := string(procT.Payload.Get(0))
			if _, exists := expMsgs[act]; !exists {
				t.Errorf("Unexpected result: %v", act)
			} else {
				delete(expMsgs, act)
			}
			resChans = append(resChans, procT.ResponseChan)
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	if len(expMsgs) != 0 {
		t.Errorf("Expected messages were not received: %v", expMsgs)
	}

	// Respond without error N times
	for i := 0; i < mockProc.N; i++ {
		select {
		case resChans[i] <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	// Receive error
	select {
	case res, open := <-resChan:
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

func TestProcessorMultiMsgsOddSync(t *testing.T) {
	mockProc := &mockMultiMsgProcessor{N: 3}

	proc := NewProcessor(
		log.New(os.Stdout, log.Config{LogLevel: "NONE"}),
		metrics.DudType{},
		mockProc,
	)

	tChan, resChan := make(chan types.Transaction), make(chan types.Response)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}

	expMsgs := map[string]struct{}{}
	for i := 0; i < mockProc.N; i++ {
		expMsgs[fmt.Sprintf("test%v", i)] = struct{}{}
	}

	// Send message
	select {
	case tChan <- types.NewTransaction(types.NewMessage(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var errResChan chan<- types.Response

	// Receive 1 message
	select {
	case procT, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		act := string(procT.Payload.Get(0))
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected result: %v", act)
		}
		errResChan = procT.ResponseChan
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond with 1 error
	select {
	case errResChan <- types.NewSimpleResponse(errors.New("foo")):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	resChans := []chan<- types.Response{}

	// Receive N messages
	for i := 0; i < mockProc.N; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			if !open {
				t.Error("Closed early")
			}
			act := string(procT.Payload.Get(0))
			if _, exists := expMsgs[act]; !exists {
				t.Errorf("Unexpected result: %v", act)
			} else {
				delete(expMsgs, act)
			}
			resChans = append(resChans, procT.ResponseChan)
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	if len(expMsgs) != 0 {
		t.Errorf("Expected messages were not received: %v", expMsgs)
	}

	// Respond without error N times
	for i := 0; i < mockProc.N; i++ {
		select {
		case resChans[i] <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	// Receive error
	select {
	case res, open := <-resChan:
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
