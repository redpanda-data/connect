package batcher_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ibatch "github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/component/input/batcher"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestBatcherStandard(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockInput := &mock.Input{
		TChan: make(chan message.Transaction),
	}

	batchConf := batchconfig.NewConfig()
	batchConf.Count = 3

	batchPol, err := policy.New(batchConf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	batcher := batcher.New(batchPol, mockInput, log.Noop())

	testMsgs := []string{}
	testResChans := []chan error{}
	for i := 0; i < 8; i++ {
		testMsgs = append(testMsgs, fmt.Sprintf("test%v", i))
		testResChans = append(testResChans, make(chan error))
	}

	resErrs := []error{}
	doneWritesChan := make(chan struct{})
	doneReadsChan := make(chan struct{})
	go func() {
		for i, m := range testMsgs {
			mockInput.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(m)}), testResChans[i])
		}
		close(doneWritesChan)
		for _, rChan := range testResChans {
			resErrs = append(resErrs, (<-rChan))
		}
		close(doneReadsChan)
	}()

	resFns := []func(context.Context, error) error{}

	var tran message.Transaction
	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	{
		tmpTran := tran
		resFns = append(resFns, tmpTran.Ack)
	}

	if exp, act := 3, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	_ = tran.Payload.Iter(func(i int, part *message.Part) error {
		if exp, act := fmt.Sprintf("test%v", i), string(part.AsBytes()); exp != act {
			t.Errorf("Unexpected message part: %v != %v", act, exp)
		}
		return nil
	})

	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	{
		tmpTran := tran
		resFns = append(resFns, tmpTran.Ack)
	}

	if exp, act := 3, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	_ = tran.Payload.Iter(func(i int, part *message.Part) error {
		if exp, act := fmt.Sprintf("test%v", i+3), string(part.AsBytes()); exp != act {
			t.Errorf("Unexpected message part: %v != %v", act, exp)
		}
		return nil
	})

	select {
	case <-batcher.TransactionChan():
		t.Error("Unexpected batch received")
	default:
	}

	select {
	case <-doneWritesChan:
	case <-time.After(time.Second):
		t.Error("timed out")
	}
	batcher.TriggerStopConsuming()

	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	{
		tmpTran := tran
		resFns = append(resFns, tmpTran.Ack)
	}

	if exp, act := 2, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	_ = tran.Payload.Iter(func(i int, part *message.Part) error {
		if exp, act := fmt.Sprintf("test%v", i+6), string(part.AsBytes()); exp != act {
			t.Errorf("Unexpected message part: %v != %v", act, exp)
		}
		return nil
	})

	for i, rFn := range resFns {
		require.NoError(t, rFn(tCtx, fmt.Errorf("testerr%v", i)))
	}

	select {
	case <-doneReadsChan:
	case <-time.After(time.Second):
		t.Error("timed out")
	}

	for i, err := range resErrs {
		exp := "testerr0"
		if i >= 3 {
			exp = "testerr1"
		}
		if i >= 6 {
			exp = "testerr2"
		}
		if act := err.Error(); exp != act {
			t.Errorf("Unexpected error returned: %v != %v", act, exp)
		}
	}

	if err := batcher.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestBatcherErrorTracking(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockInput := &mock.Input{
		TChan: make(chan message.Transaction),
	}

	batchConf := batchconfig.NewConfig()
	batchConf.Count = 3

	batchPol, err := policy.New(batchConf, mock.NewManager())
	require.NoError(t, err)

	batcher := batcher.New(batchPol, mockInput, log.Noop())

	testMsgs := []string{}
	testResChans := []chan error{}
	for i := 0; i < 3; i++ {
		testMsgs = append(testMsgs, fmt.Sprintf("test%v", i))
		testResChans = append(testResChans, make(chan error))
	}

	resErrs := []error{}
	doneReadsChan := make(chan struct{})
	go func() {
		for i, m := range testMsgs {
			mockInput.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(m)}), testResChans[i])
		}
		for _, rChan := range testResChans {
			resErrs = append(resErrs, (<-rChan))
		}
		close(doneReadsChan)
	}()

	var tran message.Transaction
	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.Equal(t, 3, tran.Payload.Len())
	_ = tran.Payload.Iter(func(i int, part *message.Part) error {
		assert.Equal(t, fmt.Sprintf("test%v", i), string(part.AsBytes()))
		return nil
	})

	batchErr := ibatch.NewError(tran.Payload, errors.New("ignore this"))
	batchErr.Failed(1, errors.New("message specific error"))
	require.NoError(t, tran.Ack(tCtx, batchErr))

	select {
	case <-doneReadsChan:
	case <-time.After(time.Second * 5):
		t.Fatal("timed out")
	}

	require.Len(t, resErrs, 3)
	assert.Nil(t, resErrs[0])
	assert.EqualError(t, resErrs[1], "message specific error")
	assert.Nil(t, resErrs[2])

	mockInput.TriggerStopConsuming()
	require.NoError(t, batcher.WaitForClose(tCtx))
}

func TestBatcherTiming(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockInput := &mock.Input{
		TChan: make(chan message.Transaction),
	}

	batchConf := batchconfig.NewConfig()
	batchConf.Count = 0
	batchConf.Period = "1ms"

	batchPol, err := policy.New(batchConf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	batcher := batcher.New(batchPol, mockInput, log.Noop())

	resChan := make(chan error)
	select {
	case mockInput.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo1")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var tran message.Transaction
	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if exp, act := 1, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	if exp, act := "foo1", string(tran.Payload.Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected message part: %v != %v", act, exp)
	}

	errSend := errors.New("this is a test error")
	require.NoError(t, tran.Ack(tCtx, errSend))
	select {
	case err := <-resChan:
		if err != errSend {
			t.Errorf("Unexpected error: %v != %v", err, errSend)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case mockInput.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo2")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if exp, act := 1, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	if exp, act := "foo2", string(tran.Payload.Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected message part: %v != %v", act, exp)
	}

	batcher.TriggerStopConsuming()

	require.NoError(t, tran.Ack(tCtx, errSend))
	select {
	case err := <-resChan:
		if err != errSend {
			t.Errorf("Unexpected error: %v != %v", err, errSend)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if err := batcher.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestBatcherFinalFlush(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	mockInput := &mock.Input{
		TChan: make(chan message.Transaction),
	}

	batchConf := batchconfig.NewConfig()
	batchConf.Count = 10

	batchPol, err := policy.New(batchConf, mock.NewManager())
	require.NoError(t, err)

	batcher := batcher.New(batchPol, mockInput, log.Noop())

	resChan := make(chan error, 1)
	select {
	case mockInput.TChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foo1")}), resChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	mockInput.TriggerStopConsuming()

	var tran message.Transaction
	select {
	case tran = <-batcher.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	if exp, act := 1, tran.Payload.Len(); exp != act {
		t.Errorf("Wrong batch size: %v != %v", act, exp)
	}
	if exp, act := "foo1", string(tran.Payload.Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected message part: %v != %v", act, exp)
	}

	batcher.TriggerStopConsuming()
	require.NoError(t, tran.Ack(tCtx, nil))

	if err := batcher.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}
