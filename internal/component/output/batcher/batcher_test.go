package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchInternal "github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestBatcherEarlyTermination(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan error)

	policyConf := batchconfig.NewConfig()
	policyConf.Count = 10
	policyConf.Period = "50ms"
	batchPol, err := policy.New(policyConf, mock.NewManager())
	require.NoError(t, err)

	out := &mock.OutputChanneled{}

	b := batcher.New(batchPol, out, mock.NewManager())
	require.NoError(t, b.Consume(tInChan))

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	done()

	require.Error(t, b.WaitForClose(ctx))

	select {
	case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo")}), resChan):
	case <-time.After(time.Second):
		t.Error("unexpected")
	}

	ctx, done = context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.Error(t, b.WaitForClose(ctx))
}

func TestBatcherBasic(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan error)

	policyConf := batchconfig.NewConfig()
	policyConf.Count = 4
	batchPol, err := policy.New(policyConf, mock.NewManager())
	require.NoError(t, err)

	out := &mock.OutputChanneled{}

	b := batcher.New(batchPol, out, mock.NewManager())
	require.NoError(t, b.Consume(tInChan))

	tOutChan := out.TChan

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
				assert.Equal(t, firstErr, actRes)
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
				assert.Equal(t, secondErr, actRes)
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
				assert.Equal(t, finalErr, actRes)
			case <-time.After(time.Second):
				t.Error("timed out")
			}
		}
	}()

	sendResponse := func(tran message.Transaction, err error) {
		sCtx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		defer wg.Done()
		require.NoError(t, tran.Ack(sCtx, err))
	}

	// Receive first batch on output
	select {
	case outTr := <-tOutChan:
		if exp, act := firstBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result from batch: %s != %s", act, exp)
		}
		wg.Add(1)
		go sendResponse(outTr, firstErr)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}

	// Receive second batch on output
	select {
	case outTr := <-tOutChan:
		if exp, act := secondBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result from batch: %s != %s", act, exp)
		}
		wg.Add(1)
		go sendResponse(outTr, secondErr)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}

	// Receive final batch on output
	select {
	case outTr := <-tOutChan:
		if exp, act := finalBatchExpected, message.GetAllBytes(outTr.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong result from batch: %s != %s", act, exp)
		}
		wg.Add(1)
		go sendResponse(outTr, finalErr)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message read")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, b.WaitForClose(ctx))
	wg.Wait()
}

func TestBatcherMaxInFlight(t *testing.T) {
	timeOutCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tInChan := make(chan message.Transaction)

	policyConf := batchconfig.NewConfig()
	policyConf.Count = 2
	batchPol, err := policy.New(policyConf, mock.NewManager())
	require.NoError(t, err)

	out := &mock.OutputChanneled{}

	b := batcher.New(batchPol, out, mock.NewManager())
	require.NoError(t, b.Consume(tInChan))

	tOutChan := out.TChan
	resChanOne, resChanTwo := make(chan error), make(chan error)

	select {
	case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{
		[]byte("hello world 1"),
		[]byte("hello world 2"),
		[]byte("hello world 3"),
		[]byte("hello world 4"),
	}), resChanOne):
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	var tranOne message.Transaction
	select {
	case tranOne = <-tOutChan:
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	select {
	case tInChan <- message.NewTransaction(message.QuickBatch([][]byte{
		[]byte("hello world 5"),
		[]byte("hello world 6"),
		[]byte("hello world 7"),
		[]byte("hello world 8"),
	}), resChanTwo):
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	var tranTwo message.Transaction
	select {
	case tranTwo = <-tOutChan:
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	require.NoError(t, tranOne.Ack(timeOutCtx, nil))
	require.NoError(t, tranTwo.Ack(timeOutCtx, nil))

	select {
	case err := <-resChanOne:
		require.NoError(t, err)
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	select {
	case err := <-resChanTwo:
		require.NoError(t, err)
	case <-timeOutCtx.Done():
		t.Fatal("timed out")
	}

	b.TriggerCloseNow()
	require.NoError(t, b.WaitForClose(timeOutCtx))
}

func TestBatcherBatchError(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	tInChan := make(chan message.Transaction)
	resChan := make(chan error)

	policyConf := batchconfig.NewConfig()
	policyConf.Count = 4
	batchPol, err := policy.New(policyConf, mock.NewManager())
	require.NoError(t, err)

	out := &mock.OutputChanneled{}

	b := batcher.New(batchPol, out, mock.NewManager())
	require.NoError(t, b.Consume(tInChan))

	tOutChan := out.TChan

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

		require.NoError(t, outTr.Ack(tCtx, batchErr))
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
			act = actRes
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}
		switch i {
		case 0:
			assert.EqualError(t, act, "first error")
		case 2:
			assert.EqualError(t, act, "third error")
		default:
			assert.NoError(t, act)
		}
	}

	close(tInChan)
	b.TriggerCloseNow()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	require.NoError(t, b.WaitForClose(ctx))
	wg.Wait()
}

func TestBatcherTimed(t *testing.T) {
	tInChan := make(chan message.Transaction)
	resChan := make(chan error)

	policyConf := batchconfig.NewConfig()
	policyConf.Period = "100ms"
	batchPol, err := policy.New(policyConf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	out := &mock.OutputChanneled{}

	b := batcher.New(batchPol, out, mock.NewManager())
	if err := b.Consume(tInChan); err != nil {
		t.Fatal(err)
	}

	tOutChan := out.TChan

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

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	close(tInChan)
	b.TriggerCloseNow()
	require.NoError(t, b.WaitForClose(ctx))

	close(resChan)
}
