package output

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	batchInternal "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

func TestBatcherEarlyTermination(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	policyConf := batch.NewPolicyConfig()
	policyConf.Count = 10
	policyConf.Period = "50ms"
	batcher, err := batch.NewPolicy(policyConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	out := &mockOutput{}

	b := NewBatcher(batcher, out, log.Noop(), metrics.Noop())
	require.NoError(t, b.Consume(tInChan))

	require.Error(t, b.WaitForClose(time.Millisecond*100))

	select {
	case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-time.After(time.Second):
		t.Error("unexpected")
	}

	require.Error(t, b.WaitForClose(time.Second))
}

func TestBatcherBasic(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	policyConf := batch.NewPolicyConfig()
	policyConf.Count = 4
	batcher, err := batch.NewPolicy(policyConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	out := &mockOutput{}

	b := NewBatcher(batcher, out, log.Noop(), metrics.Noop())
	require.NoError(t, b.Consume(tInChan))

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
			case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
		for range firstBatchExpected {
			select {
			case actRes := <-resChan:
				assert.Equal(t, firstErr, actRes.AckError())
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
		for _, batch := range secondBatchExpected {
			select {
			case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
		for range secondBatchExpected {
			select {
			case actRes := <-resChan:
				assert.Equal(t, secondErr, actRes.AckError())
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
		for _, batch := range finalBatchExpected {
			select {
			case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{batch}), resChan):
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
		close(tInChan)
		for range finalBatchExpected {
			select {
			case actRes := <-resChan:
				assert.Equal(t, finalErr, actRes.AckError())
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
	}()

	sendResponse := func(rChan chan<- response.Error, err error) {
		defer wg.Done()
		select {
		case rChan <- response.NewError(err):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}

	// Receive first batch on output
	var outTr message.Transaction
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := firstBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}
	wg.Add(1)
	go sendResponse(outTr.ResponseChan, firstErr)

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
	go sendResponse(outTr.ResponseChan, secondErr)

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
	go sendResponse(outTr.ResponseChan, finalErr)

	require.NoError(t, b.WaitForClose(time.Second*10))
	wg.Wait()
}

func TestBatcherBatchError(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	policyConf := batch.NewPolicyConfig()
	policyConf.Count = 4
	batcher, err := batch.NewPolicy(policyConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	out := &mockOutput{}

	b := NewBatcher(batcher, out, log.Noop(), metrics.Noop())
	require.NoError(t, b.Consume(tInChan))

	tOutChan := out.ts

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		firstErr := errors.New("first error")
		thirdErr := errors.New("third error")

		// Receive first batch on output
		var outTr message.Transaction
		select {
		case outTr = <-tOutChan:
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message read")
		}
		assert.Equal(t, [][]byte{
			[]byte("foo0"),
			[]byte("foo1"),
			[]byte("foo2"),
			[]byte("foo3"),
		}, message.GetAllBytes(outTr.Payload))

		batchErr := batchInternal.NewError(outTr.Payload, errors.New("foo")).
			Failed(0, firstErr).Failed(2, thirdErr)

		select {
		case outTr.ResponseChan <- response.NewError(batchErr):
		case <-time.After(time.Second):
			t.Error("timed out")
		}
	}()

	for i := 0; i < 4; i++ {
		data := []byte(fmt.Sprintf("foo%v", i))
		select {
		case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{data}), resChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
	}
	for i := 0; i < 4; i++ {
		var act error
		select {
		case actRes := <-resChan:
			act = actRes.AckError()
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
		switch i {
		case 0:
			assert.EqualError(t, act, "first error")
		case 2:
			assert.EqualError(t, act, "third error")
		default:
			assert.Nil(t, act)
		}
	}

	close(tInChan)
	b.CloseAsync()

	if err = b.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
	wg.Wait()
}

func TestBatcherTimed(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan response.Error)

	policyConf := batch.NewPolicyConfig()
	policyConf.Period = "100ms"
	batcher, err := batch.NewPolicy(policyConf, mock.NewManager(), log.Noop(), metrics.Noop())
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
	case tInChan <- message.NewTransaction(message.QuickBatch(batchExpected), resChan):
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message send")
	}

	// Receive first batch on output
	var outTr message.Transaction
	select {
	case outTr = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}
	if exp, act := batchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result from batch: %s != %s", act, exp)
	}

	close(tInChan)
	b.CloseAsync()
	if err = b.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	close(resChan)
}

//------------------------------------------------------------------------------
