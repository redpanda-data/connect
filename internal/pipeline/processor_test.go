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

	"github.com/benthosdev/benthos/v4/internal/batch"
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

type mockSplitProcessor struct {
	hasClosedAsync    bool
	hasWaitedForClose bool
	mut               sync.Mutex
}

func (m *mockSplitProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	var msgs []message.Batch
	for _, p := range msg {
		tmpMsg := p.ShallowCopy()
		tmpMsg.SetBytes(fmt.Appendf(nil, "%s test", p.AsBytes()))
		msgs = append(msgs, message.Batch{tmpMsg})
	}
	return msgs, nil
}

func (m *mockSplitProcessor) Close(ctx context.Context) error {
	m.mut.Lock()
	m.hasClosedAsync = true
	m.hasWaitedForClose = true
	m.mut.Unlock()
	return nil
}

func TestProcessorMultiMsgs(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockSplitProcessor{}
	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)
	require.NoError(t, proc.Consume(tChan))

	// Send message
	select {
	case tChan <- message.NewTransaction(message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	expMsgs := map[string]struct{}{
		"foo test": {},
		"bar test": {},
		"baz test": {},
	}

	resFns := []func(context.Context, error) error{}

	// Receive N messages
	for i := 0; i < 3; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			require.True(t, open)

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
	for i := 0; i < 3; i++ {
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
	require.NoError(t, proc.WaitForClose(ctx))
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}

func TestProcessorMultiMsgsBatchError(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockSplitProcessor{}
	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)

	require.NoError(t, proc.Consume(tChan))

	sortGroup, inputBatch := message.NewSortGroup(message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	})

	// Send message
	select {
	case tChan <- message.NewTransaction(inputBatch, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	expMsgs := map[string]struct{}{
		"foo test": {},
		"bar test": {},
		"baz test": {},
	}

	resFns := []func(context.Context, error) error{}

	// Receive expected messages
	for i := 0; i < 3; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			require.True(t, open)

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

	assert.Empty(t, expMsgs)
	require.Len(t, resFns, 3)

	require.NoError(t, resFns[0](ctx, nil))
	require.NoError(t, resFns[1](ctx, errors.New("oh no")))
	require.NoError(t, resFns[2](ctx, nil))

	// Receive overall ack
	select {
	case err, open := <-resChan:
		require.True(t, open)
		require.EqualError(t, err, "oh no")

		var batchErr *batch.Error
		require.ErrorAs(t, err, &batchErr)

		indexErrs := map[int]string{}
		batchErr.WalkPartsBySource(sortGroup, inputBatch, func(i int, p *message.Part, err error) bool {
			if err != nil {
				indexErrs[i] = err.Error()
			}
			return true
		})
		assert.Equal(t, map[int]string{
			1: "oh no",
		}, indexErrs)
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.TriggerCloseNow()
	require.NoError(t, proc.WaitForClose(ctx))
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}

type mockPhantomProcessor struct {
	hasClosedAsync    bool
	hasWaitedForClose bool
	mut               sync.Mutex
}

func (m *mockPhantomProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	var msgs []message.Batch
	for _, p := range msg {
		tmpMsg := p.ShallowCopy()
		tmpMsg.SetBytes(fmt.Appendf(nil, "%s test", p.AsBytes()))
		msgs = append(msgs, message.Batch{tmpMsg})
	}
	msgs = append(msgs, message.Batch{
		message.NewPart([]byte("phantom message")),
	})
	return msgs, nil
}

func (m *mockPhantomProcessor) Close(ctx context.Context) error {
	m.mut.Lock()
	m.hasClosedAsync = true
	m.hasWaitedForClose = true
	m.mut.Unlock()
	return nil
}

func TestProcessorMultiMsgsBatchUnknownError(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	mockProc := &mockPhantomProcessor{}
	proc := pipeline.NewProcessor(mockProc)

	tChan, resChan := make(chan message.Transaction), make(chan error)

	require.NoError(t, proc.Consume(tChan))

	_, inputBatch := message.NewSortGroup(message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	})

	// Send message
	select {
	case tChan <- message.NewTransaction(inputBatch, resChan):
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	expMsgs := map[string]struct{}{
		"foo test":        {},
		"bar test":        {},
		"baz test":        {},
		"phantom message": {},
	}

	resFns := []func(context.Context, error) error{}

	// Receive expected messages
	for i := 0; i < 4; i++ {
		select {
		case procT, open := <-proc.TransactionChan():
			require.True(t, open)

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

	assert.Empty(t, expMsgs)
	require.Len(t, resFns, 4)

	require.NoError(t, resFns[0](ctx, nil))
	require.NoError(t, resFns[1](ctx, nil))
	require.NoError(t, resFns[2](ctx, nil))
	require.NoError(t, resFns[3](ctx, errors.New("oh no")))

	// Receive overall ack
	select {
	case err, open := <-resChan:
		require.True(t, open)
		require.EqualError(t, err, "oh no")

		var batchErr *batch.Error
		require.False(t, errors.As(err, &batchErr))
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	proc.TriggerCloseNow()
	require.NoError(t, proc.WaitForClose(ctx))
	if !mockProc.hasClosedAsync {
		t.Error("Expected mockproc to have closed asynchronously")
	}
	if !mockProc.hasWaitedForClose {
		t.Error("Expected mockproc to have waited for close")
	}
}
