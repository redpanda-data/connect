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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func TestFanOutInterfaces(t *testing.T) {
	f := &FanOut{}
	if types.Consumer(f) == nil {
		t.Errorf("FanOut: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("FanOut: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func TestBasicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []types.Consumer{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.Message{Parts: content}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		for j := 0; j < nOutputs; j++ {
			select {
			case msg := <-mockOutputs[j].MsgChan:
				if string(msg.Parts[0]) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", msg.Parts[0], content[0])
				}
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
		}
		for j := 0; j < nOutputs; j++ {
			select {
			case mockOutputs[j].ResChan <- types.NewSimpleResponse(nil):
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to broker")
				return
			}
		}
		select {
		case res := <-oTM.ResponseChan():
			if res.Error() != nil {
				t.Errorf("Received unexpected errors from broker: %v", res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestFanOutAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	mockTwo := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}

	outputs := []types.Consumer{&mockOne, &mockTwo}
	readChan := make(chan types.Message)

	conf := NewFanOutConfig()
	oTM, err := NewFanOut(
		conf, outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err == nil {
		t.Error("Expected error on duplicate receive call")
	}

	select {
	case readChan <- types.Message{Parts: [][]byte{[]byte("hello world")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}
	select {
	case <-mockOne.MsgChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case <-mockTwo.MsgChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
		return
	}
	select {
	case mockOne.ResChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case mockTwo.ResChan <- types.NewSimpleResponse(errors.New("this is a test")):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case <-mockOne.MsgChan:
		t.Error("Received duplicate message to mockOne")
	case <-mockTwo.MsgChan:
	case <-oTM.ResponseChan():
		t.Error("Received premature response from broker")
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockTwo")
		return
	}
	select {
	case mockTwo.ResChan <- types.NewSimpleResponse(nil):
	case <-time.After(time.Second):
		t.Error("Timed out responding to broker")
		return
	}
	select {
	case res := <-oTM.ResponseChan():
		if res.Error() != nil {
			t.Errorf("Fan out returned error %v", res.Error())
		}
	case <-time.After(time.Second):
		t.Errorf("Timed out responding to broker")
		return
	}

	close(readChan)

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestFanOutShutDown(t *testing.T) {
	nOutputs := 10

	outputs := []types.Consumer{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	for _, mockOut := range mockOutputs {
		close(mockOut.ResChan)
		select {
		case <-mockOut.MsgChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for msg rcv")
		}
	}

	select {
	case <-oTM.ResponseChan():
	case <-time.After(time.Second * 5):
		t.Error("fan out failed to close")
	}
}

func TestFanOutShutDownFromErrorResponse(t *testing.T) {
	outputs := []types.Consumer{}
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.MsgChan:
		if !open {
			t.Error("fan out output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	select {
	case mockOutput.ResChan <- types.NewSimpleResponse(errors.New("test")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for res send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.MsgChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromReceive(t *testing.T) {
	outputs := []types.Consumer{}
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.MsgChan:
		if !open {
			t.Error("fan out output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.MsgChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestFanOutShutDownFromSend(t *testing.T) {
	outputs := []types.Consumer{}
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs = append(outputs, mockOutput)
	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.Message{}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.MsgChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------

func BenchmarkBasicFanOut(b *testing.B) {
	nOutputs, nMsgs := 3, b.N

	outputs := []types.Consumer{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan types.Message)

	oTM, err := NewFanOut(
		NewFanOutConfig(), outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		b.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		b.Error(err)
		return
	}

	content := [][]byte{[]byte("hello world")}

	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- types.Message{Parts: content}
		for j := 0; j < nOutputs; j++ {
			<-mockOutputs[j].MsgChan
		}
		for j := 0; j < nOutputs; j++ {
			mockOutputs[j].ResChan <- types.NewSimpleResponse(nil)
		}
		res := <-oTM.ResponseChan()
		if res.Error() != nil {
			b.Errorf("Received unexpected errors from broker: %v", res.Error())
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
