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

	"github.com/jeffail/benthos/agent"
	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

func TestBasicOneToMany(t *testing.T) {
	nAgents, nMsgs := 10, 1000

	agents := []agent.Type{}
	mockAgents := []*agent.MockType{}

	for i := 0; i < nAgents; i++ {
		mockAgents = append(mockAgents, &agent.MockType{
			ResChan:  make(chan types.Response),
			Messages: make(chan types.Message),
		})
		agents = append(agents, mockAgents[i])
	}

	readChan := make(chan types.Message)

	oTM := NewOneToMany(agents)
	oTM.SetReadChan(readChan)

	for i := 0; i < nMsgs; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.Message{Parts: content}:
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}
		for j := 0; j < nAgents; j++ {
			select {
			case msg := <-mockAgents[j].Messages:
				if string(msg.Parts[0]) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", msg.Parts[0], content[0])
				}
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}
			select {
			case mockAgents[j].ResChan <- nil:
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to broker")
				return
			}
		}
		select {
		case res := <-oTM.ResponseChan():
			if len(res) != 0 {
				t.Errorf("Received unexpected errors from broker: %v", res)
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}
}

//--------------------------------------------------------------------------------------------------

func BenchmarkBasicOneToMany(b *testing.B) {
	nAgents, nMsgs := 3, 100000

	agents := []agent.Type{}
	mockAgents := []*agent.MockType{}

	for i := 0; i < nAgents; i++ {
		mockAgents = append(mockAgents, &agent.MockType{
			ResChan:  make(chan types.Response),
			Messages: make(chan types.Message),
		})
		agents = append(agents, mockAgents[i])
	}

	readChan := make(chan types.Message)

	oTM := NewOneToMany(agents)
	oTM.SetReadChan(readChan)

	content := [][]byte{[]byte("hello world")}

	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- types.Message{Parts: content}
		for j := 0; j < nAgents; j++ {
			<-mockAgents[j].Messages
			mockAgents[j].ResChan <- nil
		}
		res := <-oTM.ResponseChan()
		if len(res) != 0 {
			b.Errorf("Received unexpected errors from broker: %v", res)
		}
	}

	b.StopTimer()
}

func BenchmarkBasicOneToManyNoSelect(b *testing.B) {
	nAgents, nMsgs := 3, 100000

	agents := []agent.Type{}
	mockAgents := []*agent.MockType{}

	for i := 0; i < nAgents; i++ {
		mockAgents = append(mockAgents, &agent.MockType{
			ResChan:  make(chan types.Response),
			Messages: make(chan types.Message),
		})
		agents = append(agents, mockAgents[i])
	}

	readChan := make(chan types.Message)

	oTM := newOneToManyNoSelect(agents)
	oTM.SetReadChan(readChan)

	content := [][]byte{[]byte("hello world")}

	b.StartTimer()

	for i := 0; i < nMsgs; i++ {
		readChan <- types.Message{Parts: content}
		for j := 0; j < nAgents; j++ {
			<-mockAgents[j].Messages
			mockAgents[j].ResChan <- nil
		}
		res := <-oTM.ResponseChan()
		if len(res) != 0 {
			b.Errorf("Received unexpected errors from broker: %v", res)
		}
	}

	b.StopTimer()
}

//--------------------------------------------------------------------------------------------------
