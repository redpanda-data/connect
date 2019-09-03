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

package buffer

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/buffer/parallel"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestParallelBatcherBasic(t *testing.T) {
	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	wrap := NewParallelWrapper(
		NewConfig(), parallel.NewMemory(10000),
		log.Noop(), metrics.Noop(),
	)
	policyConf := batch.NewPolicyConfig()
	policyConf.Count = 4
	batcher, err := batch.NewPolicy(policyConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	b := NewParallelBatcher(batcher, wrap, log.Noop(), metrics.Noop())
	if err := b.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	var firstBatchExpected [][]byte
	var secondBatchExpected [][]byte
	var finalBatchExpected [][]byte
	for i := 0; i < 10; i++ {
		inputBytes := []byte(fmt.Sprintf("foo %v", i))
		if i < 4 {
			firstBatchExpected = append(firstBatchExpected, inputBytes)
		} else if i < 8 {
			secondBatchExpected = append(secondBatchExpected, inputBytes)
		} else {
			finalBatchExpected = append(finalBatchExpected, inputBytes)
		}

		select {
		case tChan <- types.NewTransaction(message.New([][]byte{inputBytes}), resChan):
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for message %v send", i)
		}

		// Instant response from buffer
		select {
		case res := <-resChan:
			if res.Error() != nil {
				t.Error(res.Error())
			}
		case <-time.After(time.Second):
			t.Fatalf("Timed out waiting for unbuffered message %v response", i)
		}
	}

	// Receive first batch on output
	var outTr types.Transaction
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered message read")
	}
	if exp, act := firstBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	// Return response
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered response send back")
	}

	// Receive second batch on output
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered message read")
	}
	if exp, act := secondBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	// Return response
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered response send back")
	}

	// Check for empty buffer
	select {
	case <-b.TransactionChan():
		t.Error("Unexpected batch")
	case <-time.After(time.Millisecond * 100):
	}

	b.StopConsuming()

	// Receive final batch on output
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered message read")
	}
	if exp, act := finalBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	// Return response
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered response send back")
	}

	b.CloseAsync()
	if err = b.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	close(resChan)
	close(tChan)
}

func TestParallelBatcherTimed(t *testing.T) {
	tChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	wrap := NewParallelWrapper(
		NewConfig(), parallel.NewMemory(10000),
		log.Noop(), metrics.Noop(),
	)
	policyConf := batch.NewPolicyConfig()
	policyConf.Period = "100ms"
	batcher, err := batch.NewPolicy(policyConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	b := NewParallelBatcher(batcher, wrap, log.Noop(), metrics.Noop())
	if err := b.Consume(tChan); err != nil {
		t.Fatal(err)
	}

	batchExpected := [][]byte{
		[]byte("foo1"),
		[]byte("foo2"),
		[]byte("foo3"),
	}

	select {
	case tChan <- types.NewTransaction(message.New(batchExpected), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message send")
	}

	// Instant response from buffer
	select {
	case res := <-resChan:
		if res.Error() != nil {
			t.Error(res.Error())
		}
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered message response")
	}

	// Receive first batch on output
	var outTr types.Transaction
	select {
	case outTr = <-b.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered message read")
	}
	if exp, act := batchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	// Return response
	select {
	case outTr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for unbuffered response send back")
	}

	select {
	case tChan <- types.NewTransaction(message.New(batchExpected), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message send")
	}

	b.StopConsuming()
	b.CloseAsync()
	if err = b.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	close(resChan)
	close(tChan)
}

//------------------------------------------------------------------------------
