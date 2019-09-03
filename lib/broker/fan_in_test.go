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
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestFanInInterfaces(t *testing.T) {
	f := &FanIn{}
	if types.Producer(f) == nil {
		t.Errorf("FanIn: nil types.Producer")
	}
	if types.Closable(f) == nil {
		t.Errorf("FanIn: nil types.Closable")
	}
}

//------------------------------------------------------------------------------

func TestBasicFanIn(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	resChan := make(chan types.Response)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < nMsgs; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- types.NewTransaction(message.New(content), resChan):
			case <-time.After(time.Second * 5):
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
				case <-time.After(time.Second * 5):
					t.Fatalf("Timed out waiting for broker propagate: %v, %v", i, j)
				}
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-time.After(time.Second * 5):
					t.Fatalf("Timed out waiting for response to broker: %v, %v", i, j)
				}
			}()
			select {
			case <-resChan:
			case <-time.After(time.Second * 5):
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

func TestFanInShutdown(t *testing.T) {
	nInputs := 10

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.DudType{})
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
		close(mockIn.TChan)
	}

	select {
	case <-fanIn.TransactionChan():
	case <-time.After(time.Second * 5):
		t.Error("fan in failed to close")
	}
}

func TestFanInAsync(t *testing.T) {
	nInputs, nMsgs := 10, 1000

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.DudType{})
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
				case <-time.After(time.Second * 5):
					t.Errorf("Timed out waiting for broker send: %v, %v", i, index)
					return
				}
				select {
				case res := <-rChan:
					if expected, actual := string(content[0]), res.Error().Error(); expected != actual {
						t.Errorf("Wrong response: %v != %v", expected, actual)
					}
				case <-time.After(time.Second * 5):
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
		case <-time.After(time.Second * 5):
			t.Errorf("Timed out waiting for broker propagate: %v", i)
			return
		}
		select {
		case ts.ResponseChan <- response.NewError(errors.New(string(ts.Payload.Get(0).Get()))):
		case <-time.After(time.Second * 5):
			t.Errorf("Timed out waiting for response to broker: %v", i)
			return
		}
	}

	wg.Wait()
}

func BenchmarkBasicFanIn(b *testing.B) {
	nInputs := 10

	Inputs := []types.Producer{}
	mockInputs := []*MockInputType{}
	resChan := make(chan types.Response)

	for i := 0; i < nInputs; i++ {
		mockInputs = append(mockInputs, &MockInputType{
			TChan: make(chan types.Transaction),
		})
		Inputs = append(Inputs, mockInputs[i])
	}

	fanIn, err := NewFanIn(Inputs, metrics.DudType{})
	if err != nil {
		b.Error(err)
		return
	}

	defer func() {
		fanIn.CloseAsync()
		fanIn.WaitForClose(time.Second)
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < nInputs; j++ {
			content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
			select {
			case mockInputs[j].TChan <- types.NewTransaction(message.New(content), resChan):
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker send: %v, %v", i, j)
				return
			}
			var ts types.Transaction
			select {
			case ts = <-fanIn.TransactionChan():
				if string(ts.Payload.Get(0).Get()) != string(content[0]) {
					b.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for broker propagate: %v, %v", i, j)
				return
			}
			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for response to broker: %v, %v", i, j)
				return
			}
			select {
			case <-resChan:
			case <-time.After(time.Second * 5):
				b.Errorf("Timed out waiting for response to input: %v, %v", i, j)
				return
			}
		}
	}

	b.StopTimer()
}

//------------------------------------------------------------------------------
