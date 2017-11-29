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
	"fmt"
	"testing"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func TestRoundRobinInterfaces(t *testing.T) {
	f := &RoundRobin{}
	if types.Consumer(f) == nil {
		t.Errorf("RoundRobin: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("RoundRobin: nil types.Closable")
	}
}

func TestRoundRobinDoubleClose(t *testing.T) {
	oTM, err := NewRoundRobin([]types.Consumer{}, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	// This shouldn't cause a panic
	oTM.CloseAsync()
	oTM.CloseAsync()
}

//------------------------------------------------------------------------------

func TestBasicRoundRobin(t *testing.T) {
	nMsgs := 1000

	outputs := []types.Consumer{}
	mockOutputs := []*MockOutputType{
		{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		},
		{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		},
		{
			ResChan: make(chan types.Response),
			MsgChan: make(chan types.Message),
		},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan types.Message)

	oTM, err := NewRoundRobin(outputs, metrics.DudType{})
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

		select {
		case msg := <-mockOutputs[i%3].MsgChan:
			if string(msg.Parts[0]) != string(content[0]) {
				t.Errorf("Wrong content returned %s != %s", msg.Parts[0], content[0])
			}
		case <-mockOutputs[(i+1)%3].MsgChan:
			t.Errorf("Received message in wrong order: %v != %v", i%3, (i+1)%3)
			return
		case <-mockOutputs[(i+2)%3].MsgChan:
			t.Errorf("Received message in wrong order: %v != %v", i%3, (i+2)%3)
			return
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker propagate")
			return
		}

		select {
		case mockOutputs[i%3].ResChan <- types.NewSimpleResponse(nil):
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
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

//------------------------------------------------------------------------------

func BenchmarkBasicRoundRobin(b *testing.B) {
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

	oTM, err := NewRoundRobin(outputs, metrics.DudType{})
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
		<-mockOutputs[i%3].MsgChan
		mockOutputs[i%3].ResChan <- types.NewSimpleResponse(nil)
		res := <-oTM.ResponseChan()
		if res.Error() != nil {
			b.Errorf("Received unexpected errors from broker: %v", res.Error())
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
