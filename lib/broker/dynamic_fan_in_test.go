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
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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
			MsgChan: make(chan types.Message),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := NewDynamicFanIn(Inputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].MsgChan <- types.Message{Parts: content}:
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			select {
			case msg := <-fanIn.MessageChan():
				if string(msg.Parts[0]) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", msg.Parts[0], content[0])
				}
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				return
			}
			select {
			case resChan <- types.NewSimpleResponse(nil):
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for response to broker: %v, %v", i, j)
				return
			}
			select {
			case <-mockInputs[j].ResChan:
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
		MsgChan: make(chan types.Message),
	}
	inputTwo := &MockInputType{
		MsgChan: make(chan types.Message),
	}

	resChan := make(chan types.Response)

	fanIn, err := NewDynamicFanIn(nil, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.SetInput("foo", inputOne, time.Second); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	sendAllTestMessages := func(input *MockInputType, label string) {
		for i := 0; i < nMsgs; i++ {
			content := [][]byte{[]byte(fmt.Sprintf("%v-%v", label, i))}
			input.MsgChan <- types.Message{Parts: content}
			select {
			case <-input.ResChan:
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
		expContent := fmt.Sprintf("inputOne-%v", i)
		select {
		case msg := <-fanIn.MessageChan():
			if string(msg.Parts[0]) != expContent {
				t.Errorf("Wrong content returned %s != %s", msg.Parts[0], expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	if err = fanIn.SetInput("foo", inputTwo, time.Second); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nMsgs; i++ {
		expContent := fmt.Sprintf("inputTwo-%v", i)
		select {
		case msg := <-fanIn.MessageChan():
			if string(msg.Parts[0]) != expContent {
				t.Errorf("Wrong content returned %s != %s", msg.Parts[0], expContent)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case resChan <- types.NewSimpleResponse(nil):
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
	resChan := make(chan types.Response)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			MsgChan: make(chan types.Message),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := NewDynamicFanIn(Inputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	for _, mockIn := range mockInputs {
		select {
		case _, open := <-mockIn.MessageChan():
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
		case _, open := <-mockIn.MessageChan():
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
}

func TestStaticDynamicFanInAsync(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := map[string]DynamicInput{}
	mockInputs := []*MockInputType{}
	resChan := make(chan types.Response)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			MsgChan: make(chan types.Message),
		})
		Inputs[fmt.Sprintf("testinput%v", i)] = mockInputs[i]
	}

	fanIn, err := NewDynamicFanIn(Inputs, log.NewLogger(os.Stdout, logConfig), metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	if err = fanIn.StartListening(resChan); err != nil {
		t.Error(err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(nInputs)

	for j := 0; j < nInputs; j++ {
		go func(index int) {
			for i := 0; i < nMsgs; i++ {
				content := [][]byte{[]byte(fmt.Sprintf("hello world %v %v", i, index))}
				select {
				case mockInputs[index].MsgChan <- types.Message{Parts: content}:
				case <-time.After(time.Second):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-mockInputs[index].ResChan:
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
		var msg types.Message
		select {
		case msg = <-fanIn.MessageChan():
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case resChan <- types.NewSimpleResponse(errors.New(string(msg.Parts[0]))):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	wg.Wait()
}

//------------------------------------------------------------------------------
