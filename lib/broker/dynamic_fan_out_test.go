// Copyright (c) 2018 Ashley Jeffs
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
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func TestDynamicFanOutInterfaces(t *testing.T) {
	f := &DynamicFanOut{}
	if types.Consumer(f) == nil {
		t.Errorf("DynamicFanOut: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("DynamicFanOut: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func TestBasicDynamicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := map[string]DynamicOutput{}
	mockOutputs := []*MockOutputType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &MockOutputType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs[fmt.Sprintf("out-%v", i)] = mockOutputs[i]
	}

	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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
		wg := sync.WaitGroup{}
		wg.Add(nOutputs)
		for j := 0; j < nOutputs; j++ {
			go func(index int) {
				defer wg.Done()
				select {
				case msg := <-mockOutputs[index].MsgChan:
					if string(msg.Parts[0]) != string(content[0]) {
						t.Errorf("Wrong content returned %s != %s", msg.Parts[0], content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case mockOutputs[index].ResChan <- types.NewSimpleResponse(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(j)
		}
		select {
		case readChan <- types.Message{Parts: content}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		wg.Wait()
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

func TestDynamicFanOutChangeOutputs(t *testing.T) {
	nOutputs := 10

	outputs := map[string]*MockOutputType{}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.StartReceiving(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		newOutput := &MockOutputType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		}
		newOutputName := fmt.Sprintf("output-%v", i)

		outputs[newOutputName] = newOutput
		if err := oTM.SetOutput(newOutputName, newOutput, time.Second); err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *MockOutputType) {
				defer wg.Done()
				select {
				case msg := <-out.MsgChan:
					if string(msg.Parts[0]) != string(content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, msg.Parts[0], content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case out.ResChan <- types.NewSimpleResponse(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.Message{Parts: content}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

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

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		wg := sync.WaitGroup{}
		wg.Add(len(outputs))
		for k, v := range outputs {
			go func(name string, out *MockOutputType) {
				defer wg.Done()
				select {
				case msg := <-out.MsgChan:
					if string(msg.Parts[0]) != string(content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, msg.Parts[0], content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case out.ResChan <- types.NewSimpleResponse(nil):
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.Message{Parts: content}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

		select {
		case res := <-oTM.ResponseChan():
			if res.Error() != nil {
				t.Errorf("Received unexpected errors from broker: %v", res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}

		oldOutputName := fmt.Sprintf("output-%v", i)
		if err := oTM.SetOutput(oldOutputName, nil, time.Second); err != nil {
			t.Fatal(err)
		}
		delete(outputs, oldOutputName)
	}

	oTM.CloseAsync()

	if err := oTM.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestDynamicFanOutAtLeastOnce(t *testing.T) {
	mockOne := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	mockTwo := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}

	outputs := map[string]DynamicOutput{
		"first":  &mockOne,
		"second": &mockTwo,
	}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		select {
		case <-mockOne.MsgChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case mockOne.ResChan <- types.NewSimpleResponse(nil):
		case <-mockOne.MsgChan:
			t.Error("Received duplicate message to mockOne")
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()
	go func() {
		defer wg.Done()
		select {
		case <-mockTwo.MsgChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case mockTwo.ResChan <- types.NewSimpleResponse(errors.New("this is a test")):
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
		select {
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
	}()

	select {
	case readChan <- types.Message{Parts: [][]byte{[]byte("hello world")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}

	wg.Wait()

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

func TestDynamicFanOutTermination(t *testing.T) {
	mockOne := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	mockTwo := MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}

	outputs := map[string]DynamicOutput{
		"first":  &mockOne,
		"second": &mockTwo,
	}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case <-mockOne.MsgChan:
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
		case <-mockOne.MsgChan:
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
	}()

	select {
	case readChan <- types.Message{Parts: [][]byte{[]byte("hello world")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}

	mockTwo.CloseAsync()
	select {
	case <-mockTwo.MsgChan:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for mockOne")
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

	select {
	case readChan <- types.Message{Parts: [][]byte{[]byte("hello world")}}:
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}

	wg.Wait()

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

func TestDynamicFanOutShutDownFromErrorResponse(t *testing.T) {
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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

func TestDynamicFanOutShutDownFromReceive(t *testing.T) {
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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

func TestDynamicFanOutShutDownFromSend(t *testing.T) {
	mockOutput := &MockOutputType{
		ResChan: make(chan types.Response),
		MsgChan: make(chan types.Message),
	}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Message)

	oTM, err := NewDynamicFanOut(
		outputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{},
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
