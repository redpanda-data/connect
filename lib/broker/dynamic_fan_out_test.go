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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
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
		mockOutputs = append(mockOutputs, &MockOutputType{})
		outputs[fmt.Sprintf("out-%v", i)] = mockOutputs[i]
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewDynamicFanOut(
		outputs, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
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
				var ts types.Transaction
				select {
				case ts = <-mockOutputs[index].TChan:
					if string(ts.Payload.Get(0).Get()) != string(content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(j)
		}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		wg.Wait()
		select {
		case res := <-resChan:
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
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewDynamicFanOut(
		nil, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nOutputs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}

		newOutput := &MockOutputType{}
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
				var ts types.Transaction
				select {
				case ts = <-out.TChan:
					if string(ts.Payload.Get(0).Get()) != string(content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

		select {
		case res := <-resChan:
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
				var ts types.Transaction
				select {
				case ts = <-out.TChan:
					if string(ts.Payload.Get(0).Get()) != string(content[0]) {
						t.Errorf("Wrong content returned for output '%v': %s != %s", name, ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate")
					return
				}
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-time.After(time.Second):
					t.Errorf("Timed out responding to broker")
					return
				}
			}(k, v)
		}

		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		wg.Wait()

		select {
		case res := <-resChan:
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
	mockOne := MockOutputType{}
	mockTwo := MockOutputType{}

	outputs := map[string]DynamicOutput{
		"first":  &mockOne,
		"second": &mockTwo,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewDynamicFanOut(
		outputs, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err == nil {
		t.Error("Expected error on duplicate receive call")
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		var ts types.Transaction
		select {
		case ts = <-mockOne.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-mockOne.TChan:
			t.Error("Received duplicate message to mockOne")
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()
	go func() {
		defer wg.Done()
		var ts types.Transaction
		select {
		case ts = <-mockTwo.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockOne")
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(errors.New("this is a test")):
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
		select {
		case ts = <-mockTwo.TChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for mockTwo")
			return
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out responding to broker")
			return
		}
	}()

	select {
	case readChan <- types.NewTransaction(message.New([][]byte{[]byte("hello world")}), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for broker send")
		return
	}

	wg.Wait()

	select {
	case res := <-resChan:
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
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	outputAddedList := []string{}
	outputRemovedList := []string{}

	oTM, err := NewDynamicFanOut(
		outputs, log.New(os.Stdout, logConfig), metrics.DudType{},
		OptDynamicFanOutSetOnAdd(func(label string) {
			outputAddedList = append(outputAddedList, label)
		}), OptDynamicFanOutSetOnRemove(func(label string) {
			outputRemovedList = append(outputRemovedList, label)
		}),
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	var ts types.Transaction
	var open bool
	select {
	case ts, open = <-mockOutput.TChan:
		if !open {
			t.Error("fan out output closed early")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	select {
	case ts.ResponseChan <- response.NewError(errors.New("test")):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for res send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}

	if exp, act := []string{"test"}, outputAddedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of added outputs: %v != %v", act, exp)
	}
	if exp, act := []string{}, outputRemovedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of removed outputs: %v != %v", act, exp)
	}
}

func TestDynamicFanOutShutDownFromReceive(t *testing.T) {
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewDynamicFanOut(
		outputs, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	select {
	case _, open := <-mockOutput.TChan:
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
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

func TestDynamicFanOutShutDownFromSend(t *testing.T) {
	mockOutput := &MockOutputType{}
	outputs := map[string]DynamicOutput{
		"test": mockOutput,
	}
	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewDynamicFanOut(
		outputs, log.New(os.Stdout, logConfig), metrics.DudType{},
	)
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	select {
	case readChan <- types.NewTransaction(message.New(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg send")
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	select {
	case _, open := <-mockOutput.TChan:
		if open {
			t.Error("fan out output still open after closure")
		}
	case <-time.After(time.Second):
		t.Error("Timed out waiting for msg rcv")
	}
}

//------------------------------------------------------------------------------
