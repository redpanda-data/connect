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
	"reflect"
	"sort"
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

func TestStaticDynamicFanInInterfaces(t *testing.T) {
	f := &DynamicFanIn{}
	if types.Producer(f) == nil {
		t.Errorf("DynamicFanIn: nil types.Producer")
	}
	if types.Closable(f) == nil {
		t.Errorf("DynamicFanIn: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func TestStaticBasicDynamicFanIn(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := map[string]DynamicInput{}
	mockInputs := []*MockInputType{}
	resChan := make(chan types.Response)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := NewDynamicFanIn(Inputs, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- types.NewTransaction(message.New(content), resChan):
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			go func() {
				var ts types.Transaction
				select {
				case ts = <-fanIn.TransactionChan():
					if string(ts.Payload.Get(0).Get()) != string(content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
					return
				}
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for response to broker: %v, %v", i, j)
					return
				}
			}()
			select {
			case <-resChan:
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for response to input: %v, %v", i, j)
				return
			}
		}
	}

	fanIn.CloseAsync()

	if err := fanIn.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestBasicDynamicFanIn(t *testing.T) {
	nMsgs := 1000

	inputOne := &MockInputType{
		TChan: make(chan types.Transaction),
	}
	inputTwo := &MockInputType{
		TChan: make(chan types.Transaction),
	}

	fanIn, err := NewDynamicFanIn(nil, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.SetInput("foo", inputOne, time.Second); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	sendAllTestMessages := func(input *MockInputType, label string) {
		rChan := make(chan types.Response)
		for i := 0; i < nMsgs; i++ {
			content := [][]byte{[]byte(fmt.Sprintf("%v-%v", label, i))}
			input.TChan <- types.NewTransaction(message.New(content), rChan)
			select {
			case <-rChan:
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for response to input: %v", i)
				return
			}
		}
		wg.Done()
	}

	wg.Add(2)
	go sendAllTestMessages(inputOne, "inputOne")
	go sendAllTestMessages(inputTwo, "inputTwo")

	for i := 0; i < nMsgs; i++ {
		var ts types.Transaction
		expContent := fmt.Sprintf("inputOne-%v", i)
		select {
		case ts = <-fanIn.TransactionChan():
			if string(ts.Payload.Get(0).Get()) != expContent {
				t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	if err = fanIn.SetInput("foo", inputTwo, time.Second); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nMsgs; i++ {
		var ts types.Transaction
		expContent := fmt.Sprintf("inputTwo-%v", i)
		select {
		case ts = <-fanIn.TransactionChan():
			if string(ts.Payload.Get(0).Get()) != expContent {
				t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	wg.Wait()

	fanIn.CloseAsync()

	if err := fanIn.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestStaticDynamicFanInShutdown(t *testing.T) {
	nInputs := 10

	Inputs := map[string]DynamicInput{}
	mockInputs := []*MockInputType{}

	expInputAddedList := []string{}
	expInputRemovedList := []string{}
	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		label := fmt.Sprintf("testinput%v", i)
		Inputs[label] = mockInputs[i]
		expInputAddedList = append(expInputAddedList, label)
		expInputRemovedList = append(expInputRemovedList, label)
	}

	var mapMut sync.Mutex
	inputAddedList := []string{}
	inputRemovedList := []string{}

	fanIn, err := NewDynamicFanIn(
		Inputs, log.New(os.Stdout, logConfig), metrics.DudType{},
		OptDynamicFanInSetOnAdd(func(label string) {
			mapMut.Lock()
			inputAddedList = append(inputAddedList, label)
			mapMut.Unlock()
		}), OptDynamicFanInSetOnRemove(func(label string) {
			mapMut.Lock()
			inputRemovedList = append(inputRemovedList, label)
			mapMut.Unlock()
		}),
	)
	if err != nil {
		t.Error(err)
		return
	}

	for _, mockIn := range mockInputs {
		select {
		case _, open := <-mockIn.TransactionChan():
			if !open {
				t.Error("fan in closed early")
			} else {
				t.Error("fan in sent unexpected message")
			}
		default:
		}
	}

	fanIn.CloseAsync()

	// All inputs should be closed.
	for _, mockIn := range mockInputs {
		select {
		case _, open := <-mockIn.TransactionChan():
			if open {
				t.Error("fan in sent unexpected message")
			}
		case <-time.After(time.Second):
			t.Error("fan in failed to close an input")
		}
	}

	if err := fanIn.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	mapMut.Lock()

	sort.Strings(expInputAddedList)
	sort.Strings(inputAddedList)
	sort.Strings(expInputRemovedList)
	sort.Strings(inputRemovedList)

	if exp, act := expInputAddedList, inputAddedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of added inputs: %v != %v", act, exp)
	}
	if exp, act := expInputRemovedList, inputRemovedList; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong list of removed inputs: %v != %v", act, exp)
	}

	mapMut.Unlock()
}

func TestStaticDynamicFanInAsync(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := map[string]DynamicInput{}
	mockInputs := []*MockInputType{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := NewDynamicFanIn(Inputs, log.New(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(nInputs)

	for j := 0; j < nInputs; j++ {
		go func(index int) {
			rChan := make(chan types.Response)
			for i := 0; i < nMsgs; i++ {
				content := [][]byte{[]byte(fmt.Sprintf("hello world %v %v", i, index))}
				select {
				case mockInputs[index].TChan <- types.NewTransaction(message.New(content), rChan):
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-rChan:
					if expected, actual := string(content[0]), res.Error().Error(); expected != actual {
						t.Errorf("Wrong response: %v != %v", expected, actual)
					}
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for response to input: %v, %v", i, index)
					return
				}
			}
			wg.Done()
		}(j)
	}

	for i := 0; i < nMsgs*nInputs; i++ {
		var ts types.Transaction
		select {
		case ts = <-fanIn.TransactionChan():
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(errors.New(string(ts.Payload.Get(0).Get()))):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	wg.Wait()
}

//------------------------------------------------------------------------------
