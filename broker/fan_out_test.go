/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/jeffail/benthos/output"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

func TestFanOutInterfaces(t *testing.T) {
	f := &FanOut{}
	if types.Output(f) == nil {
		t.Errorf("FanOut: nil types.Output")
	}
	if types.Closable(f) == nil {
		t.Errorf("FanOut: nil types.Closable")
	}
}

//--------------------------------------------------------------------------------------------------

func TestBasicFanOut(t *testing.T) {
	nOutputs, nMsgs := 10, 1000

	outputs := []types.Output{}
	mockOutputs := []*output.MockType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &output.MockType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan types.Message)

	oTM, err := NewFanOut(outputs)
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
}

//--------------------------------------------------------------------------------------------------

func BenchmarkBasicFanOut(b *testing.B) {
	nOutputs, nMsgs := 3, 100000

	outputs := []types.Output{}
	mockOutputs := []*output.MockType{}

	for i := 0; i < nOutputs; i++ {
		mockOutputs = append(mockOutputs, &output.MockType{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		})
		outputs = append(outputs, mockOutputs[i])
	}

	readChan := make(chan types.Message)

	oTM, err := NewFanOut(outputs)
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

//--------------------------------------------------------------------------------------------------
