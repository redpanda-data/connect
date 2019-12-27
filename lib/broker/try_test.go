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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestTryInterfaces(t *testing.T) {
	f := &Try{}
	if types.Consumer(f) == nil {
		t.Errorf("Try: nil types.Consumer")
	}
	if types.Closable(f) == nil {
		t.Errorf("Try: nil types.Closable")
	}
}

func TestTryDoubleClose(t *testing.T) {
	oTM, err := NewTry([]types.Output{&MockOutputType{}}, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	// This shouldn't cause a panic
	oTM.CloseAsync()
	oTM.CloseAsync()
}

//------------------------------------------------------------------------------

func TestTryHappyPath(t *testing.T) {
	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewTry(outputs, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		go func() {
			var ts types.Transaction
			select {
			case ts = <-mockOutputs[0].TChan:
				if string(ts.Payload.Get(0).Get()) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[1].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
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
		}()

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryHappyishPath(t *testing.T) {
	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewTry(outputs, metrics.DudType{})
	if err != nil {
		t.Error(err)
		return
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Errorf("Timed out waiting for broker send")
			return
		}

		go func() {
			var ts types.Transaction
			select {
			case ts = <-mockOutputs[0].TChan:
				if string(ts.Payload.Get(0).Get()) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[1].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
			case <-time.After(time.Second):
				t.Errorf("Timed out waiting for broker propagate")
				return
			}

			select {
			case ts.ResponseChan <- response.NewError(errors.New("test err")):
			case <-time.After(time.Second):
				t.Errorf("Timed out responding to broker")
				return
			}

			select {
			case ts = <-mockOutputs[1].TChan:
				if string(ts.Payload.Get(0).Get()) != string(content[0]) {
					t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
				}
			case <-mockOutputs[0].TChan:
				t.Error("Received message in wrong order")
				return
			case <-mockOutputs[2].TChan:
				t.Error("Received message in wrong order")
				return
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
		}()

		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Errorf("Timed out responding to broker")
			return
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryAllFail(t *testing.T) {
	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	oTM, err := NewTry(outputs, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		content := [][]byte{[]byte(fmt.Sprintf("hello world %v", i))}
		select {
		case readChan <- types.NewTransaction(message.New(content), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for broker send")
		}

		testErr := errors.New("test error")
		go func() {
			for j := 0; j < 3; j++ {
				var ts types.Transaction
				select {
				case ts = <-mockOutputs[j%3].TChan:
					if string(ts.Payload.Get(0).Get()) != string(content[0]) {
						t.Errorf("Wrong content returned %s != %s", ts.Payload.Get(0).Get(), content[0])
					}
				case <-mockOutputs[(j+1)%3].TChan:
					t.Fatalf("Received message in wrong order: %v != %v", j%3, (j+1)%3)
				case <-mockOutputs[(j+2)%3].TChan:
					t.Fatalf("Received message in wrong order: %v != %v", j%3, (j+2)%3)
				case <-time.After(time.Second):
					t.Fatalf("Timed out waiting for broker propagate")
				}

				select {
				case ts.ResponseChan <- response.NewError(testErr):
				case <-time.After(time.Second):
					t.Fatalf("Timed out responding to broker")
				}
			}
		}()

		select {
		case res := <-resChan:
			if exp, act := testErr, res.Error(); exp != act {
				t.Errorf("Wrong error returned: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestTryAllFailParallel(t *testing.T) {
	outputs := []types.Output{}
	mockOutputs := []*MockOutputType{
		{},
		{},
		{},
	}

	for _, o := range mockOutputs {
		outputs = append(outputs, o)
	}

	readChan := make(chan types.Transaction)

	oTM, err := NewTry(outputs, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}
	if err = oTM.Consume(readChan); err != nil {
		t.Fatal(err)
	}

	resChans := make([]chan types.Response, 10)
	for i := range resChans {
		resChans[i] = make(chan types.Response)
	}

	tallies := [3]int32{}

	wg := sync.WaitGroup{}
	testErr := errors.New("test error")
	startChan := make(chan struct{})
	for _, resChan := range resChans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				var ts types.Transaction
				var index int
				select {
				case ts = <-mockOutputs[j%3].TChan:
					index = j % 3
				case ts = <-mockOutputs[(j+1)%3].TChan:
					index = (j + 1) % 3
				case ts = <-mockOutputs[(j+2)%3].TChan:
					index = (j + 2) % 3
				case <-time.After(time.Second):
					t.Fatalf("Timed out waiting for broker propagate")
				}
				atomic.AddInt32(&tallies[index], 1)

				<-startChan

				select {
				case ts.ResponseChan <- response.NewError(testErr):
				case <-time.After(time.Second):
					t.Fatalf("Timed out responding to broker")
				}
			}
		}()
		select {
		case readChan <- types.NewTransaction(message.New([][]byte{[]byte("foo")}), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for broker send")
		}
	}
	close(startChan)

	for _, resChan := range resChans {
		select {
		case res := <-resChan:
			if exp, act := testErr, res.Error(); exp != act {
				t.Errorf("Wrong error returned: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Fatal("Timed out responding to broker")
		}
	}

	wg.Wait()
	for _, tally := range tallies {
		if int(tally) != len(resChans) {
			t.Errorf("Wrong count of propagated messages: %v", tally)
		}
	}

	oTM.CloseAsync()
	if err := oTM.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
