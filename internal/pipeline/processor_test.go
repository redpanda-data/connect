package pipeline_test

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

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

var errMockProc = errors.New("this is an error from mock processor")

type mockMsgProcessor struct {
	dropChan          chan bool
	hasClosedAsync    bool
	hasWaitedForClose bool
	mut               sync.Mutex
}

func (m *mockMsgProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	if drop := <-m.dropChan; drop {
		return nil, errMockProc
	}
	newMsg := message.QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})
	msgs := [1]message.Batch{newMsg}
	return msgs[:], nil
}

func (m *mockMsgProcessor) Close(ctx context.Context) error {
	m.mut.Lock()
	m.hasClosedAsync = true
	m.hasWaitedForClose = true
	m.mut.Unlock()
	return nil
}

func TestProcessorPipeline(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockMsgProcessor{dropChan: make(chan bool)}

	// Drop first message
	go func() {
		mockProc.dropChan <- true
	}()

	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}
	if err := proc.Consume(tChan); err == nil {
		t.Error("Expected error from dupe listening")
	}

	msg := message.QuickBatch([][]byte{
		[]byte(`one`),
		[]byte(`two`),
	})

	// First message should be dropped and return immediately
	select {
	case tChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}
	select {
	case _, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		} else {
			t.Error("Message was not dropped")
		}
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		}
		if res != errMockProc {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Do not drop next message
	go func() {
		mockProc.dropChan <- false
	}()

	// Send message
	select {
	case tChan <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var procT message.Transaction
	var open bool
	select {
	case procT, open = <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		if exp, act := [][]byte{[]byte("foo"), []byte("bar")}, message.GetAllBytes(procT.Payload); !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong message received: %s != %s", act, exp)
		}
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		}
		if res != nil {
			t.Error(res)
		} else {
			t.Error("Message was dropped")
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond without error
	go func() {
		require.NoError(t, procT.Ack(ctx, nil))
	}()

	// Receive response
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else if res != nil {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.TriggerCloseNow()
	if err := proc.WaitForClose(ctx); err != nil {
		t.Error(err)
	}
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}

type mockMultiMsgProcessor struct {
	N                 int
	hasClosedAsync    bool
	hasWaitedForClose bool
	mut               sync.Mutex
}

func (m *mockMultiMsgProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	var msgs []message.Batch
	for i := 0; i < m.N; i++ {
		newMsg := message.QuickBatch([][]byte{
			[]byte(fmt.Sprintf("test%v", i)),
		})
		msgs = append(msgs, newMsg)
	}
	return msgs, nil
}

func (m *mockMultiMsgProcessor) Close(ctx context.Context) error {
	m.mut.Lock()
	m.hasClosedAsync = true
	m.hasWaitedForClose = true
	m.mut.Unlock()
	return nil
}

func TestProcessorMultiMsgs(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockMultiMsgProcessor{N: 3}

	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}

	// Send message
	select {
	case tChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	expMsgs := map[string]struct{}{}
	for i := 0; i < mockProc.N; i++ {
		expMsgs[fmt.Sprintf("test%v", i)] = struct{}{}
	}

	resFns := []func(context.Context, error) error{}

	// Receive N messages
	for i := 0; i < mockProc.N; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			if !open {
				t.Error("Closed early")
			}
			act := string(procT.Payload.Get(0).AsBytes())
			if _, exists := expMsgs[act]; !exists {
				t.Errorf("Unexpected result: %v", act)
			} else {
				delete(expMsgs, act)
			}
			resFns = append(resFns, procT.Ack)
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	if len(expMsgs) != 0 {
		t.Errorf("Expected messages were not received: %v", expMsgs)
	}

	// Respond without error N times
	for i := 0; i < mockProc.N; i++ {
		require.NoError(t, resFns[i](ctx, nil))
	}

	// Receive error
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else if res != nil {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.TriggerCloseNow()
	if err := proc.WaitForClose(ctx); err != nil {
		t.Error(err)
	}
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}

func TestProcessorMultiMsgsOddSync(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockMultiMsgProcessor{N: 3}

	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)

	if err := proc.Consume(tChan); err != nil {
		t.Error(err)
	}

	expMsgs := map[string]struct{}{}
	for i := 0; i < mockProc.N; i++ {
		expMsgs[fmt.Sprintf("test%v", i)] = struct{}{}
	}

	// Send message
	select {
	case tChan <- message.NewTransaction(message.QuickBatch(nil), resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	var errResFn func(context.Context, error) error

	// Receive 1 message
	select {
	case procT, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		act := string(procT.Payload.Get(0).AsBytes())
		if _, exists := expMsgs[act]; !exists {
			t.Errorf("Unexpected result: %v", act)
		}
		delete(expMsgs, act)
		errResFn = procT.Ack
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond with 1 error
	require.NoError(t, errResFn(ctx, errors.New("foo")))

	resFns := []func(context.Context, error) error{}

	// Receive N messages
	for i := 0; i < mockProc.N-1; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			if !open {
				t.Error("Closed early")
			}
			act := string(procT.Payload.Get(0).AsBytes())
			if _, exists := expMsgs[act]; !exists {
				t.Errorf("Unexpected result: %v", act)
			} else {
				delete(expMsgs, act)
			}
			resFns = append(resFns, procT.Ack)
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	if len(expMsgs) != 0 {
		t.Errorf("Expected messages were not received: %v", expMsgs)
	}

	// Respond without error N times
	for i := 0; i < mockProc.N-1; i++ {
		require.NoError(t, resFns[i](ctx, nil))
	}

	// Receive 1 message
	select {
	case procT, open := <-proc.TransactionChan():
		if !open {
			t.Error("Closed early")
		}
		act := string(procT.Payload.Get(0).AsBytes())
		assert.Equal(t, "test0", act)
		errResFn = procT.Ack
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	// Respond with nil error
	require.NoError(t, errResFn(ctx, nil))

	// Receive overall ack
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Closed early")
		} else if res != nil {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.TriggerCloseNow()
	if err := proc.WaitForClose(ctx); err != nil {
		t.Error(err)
	}
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}
