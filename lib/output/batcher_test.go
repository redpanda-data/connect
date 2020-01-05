package output

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func TestBatcherBasic(t *testing.T) {
	tInChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	policyConf := batch.NewPolicyConfig()
	policyConf.Count = 4
	batcher, err := batch.NewPolicy(policyConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	out := &mockOutput{}

	b := NewBatcher(batcher, out, log.Noop(), metrics.Noop())
	if err := b.Consume(tInChan); err != nil {
		t.Fatal(err)
	}

	tOutChan := out.ts

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
	}

	firstErr := errors.New("first error")
	secondErr := errors.New("second error")
	finalErr := errors.New("final error")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, batch := range firstBatchExpected {
			select {
			case tInChan <- types.NewTransaction(message.New([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		}
		for range firstBatchExpected {
			var act error
			select {
			case actRes := <-resChan:
				act = actRes.Error()
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
			if exp := firstErr; exp != act {
				t.Errorf("Unexpected response: %v != %v", act, exp)
			}
		}
		for _, batch := range secondBatchExpected {
			select {
			case tInChan <- types.NewTransaction(message.New([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		}
		for range secondBatchExpected {
			var act error
			select {
			case actRes := <-resChan:
				act = actRes.Error()
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
			if exp := secondErr; exp != act {
				t.Errorf("Unexpected response: %v != %v", act, exp)
			}
		}
		for _, batch := range finalBatchExpected {
			select {
			case tInChan <- types.NewTransaction(message.New([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
		}
		for range finalBatchExpected {
			var act error
			select {
			case actRes := <-resChan:
				act = actRes.Error()
			case <-time.After(time.Second):
				t.Fatal("timed out")
			}
			if exp := finalErr; exp != act {
				t.Errorf("Unexpected response: %v != %v", act, exp)
			}
		}
	}()

	// Receive first batch on output
	var outTr types.Transaction
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := firstBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}
	wg.Add(1)
	go func(rChan chan<- types.Response, err error) {
		defer wg.Done()
		select {
		case rChan <- response.NewError(err):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}(outTr.ResponseChan, firstErr)

	// Receive second batch on output
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := secondBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}
	wg.Add(1)
	go func(rChan chan<- types.Response, err error) {
		defer wg.Done()
		select {
		case rChan <- response.NewError(err):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}(outTr.ResponseChan, secondErr)

	// Check for empty buffer
	select {
	case <-tOutChan:
		t.Error("Unexpected batch")
	case <-time.After(time.Millisecond * 100):
	}

	b.CloseAsync()

	// Receive final batch on output
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := finalBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}
	wg.Add(1)
	go func(rChan chan<- types.Response, err error) {
		defer wg.Done()
		select {
		case rChan <- response.NewError(err):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}(outTr.ResponseChan, finalErr)

	if err = b.WaitForClose(time.Second * 10); err != nil {
		t.Error(err)
	}
	wg.Wait()
}

func TestBatcherTimed(t *testing.T) {
	tInChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	policyConf := batch.NewPolicyConfig()
	policyConf.Period = "100ms"
	batcher, err := batch.NewPolicy(policyConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	out := &mockOutput{}

	b := NewBatcher(batcher, out, log.Noop(), metrics.Noop())
	if err := b.Consume(tInChan); err != nil {
		t.Fatal(err)
	}

	tOutChan := out.ts

	batchExpected := [][]byte{
		[]byte("foo1"),
		[]byte("foo2"),
		[]byte("foo3"),
	}

	select {
	case tInChan <- types.NewTransaction(message.New(batchExpected), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message send")
	}

	// Receive first batch on output
	var outTr types.Transaction
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := batchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	b.CloseAsync()
	if err = b.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	close(resChan)
	close(tInChan)
}

//------------------------------------------------------------------------------
