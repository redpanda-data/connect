package input_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/pipeline"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

type mockInput struct {
	closeOnce sync.Once
	ts        chan message.Transaction
}

func (m *mockInput) TransactionChan() <-chan message.Transaction {
	return m.ts
}

func (m *mockInput) Connected() bool {
	return true
}

func (m *mockInput) TriggerStopConsuming() {
	m.closeOnce.Do(func() {
		close(m.ts)
	})
}

func (m *mockInput) TriggerCloseNow() {
}

func (m *mockInput) WaitForClose(ctx context.Context) error {
	return errors.New("wasnt expecting to ever see this tbh")
}

//------------------------------------------------------------------------------

type mockPipe struct {
	tsIn <-chan message.Transaction
	ts   chan message.Transaction
}

func (m *mockPipe) Consume(ts <-chan message.Transaction) error {
	m.tsIn = ts
	return nil
}

func (m *mockPipe) TransactionChan() <-chan message.Transaction {
	return m.ts
}

func (m *mockPipe) TriggerCloseNow() {
	close(m.ts)
}

func (m *mockPipe) WaitForClose(ctx context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}
	mockPi := &mockPipe{
		ts: make(chan message.Transaction),
	}

	_, err := input.WrapWithPipeline(mockIn, func() (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})

	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err := input.WrapWithPipeline(mockIn, func() (iprocessor.Pipeline, error) {
		return mockPi, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if newInput.TransactionChan() != mockPi.ts {
		t.Error("Wrong transaction chan in new input type")
	}

	if mockIn.ts != mockPi.tsIn {
		t.Error("Wrong transactions chan in mock pipe")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	newInput.TriggerStopConsuming()
	require.NoError(t, newInput.WaitForClose(ctx))

	select {
	case _, open := <-mockIn.ts:
		if open {
			t.Error("mock input is still open after close")
		}
	case _, open := <-mockPi.ts:
		if open {
			t.Error("mock pipe is still open after close")
		}
	default:
		t.Error("neither type was closed")
	}
}

func TestWrapZeroPipelines(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}
	newInput, err := input.WrapWithPipelines(mockIn)
	if err != nil {
		t.Error(err)
	}

	if newInput != mockIn {
		t.Errorf("Wrong input obj returned: %v != %v", newInput, mockIn)
	}
}

func TestBasicWrapMultiPipelines(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}
	mockPi1 := &mockPipe{
		ts: make(chan message.Transaction),
	}
	mockPi2 := &mockPipe{
		ts: make(chan message.Transaction),
	}

	_, err := input.WrapWithPipelines(mockIn, func() (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err := input.WrapWithPipelines(mockIn, func() (iprocessor.Pipeline, error) {
		return mockPi1, nil
	}, func() (iprocessor.Pipeline, error) {
		return mockPi2, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if newInput.TransactionChan() != mockPi2.ts {
		t.Error("Wrong message chan in new input type")
	}
	if mockPi2.tsIn != mockPi1.ts {
		t.Error("Wrong message chan in mock pipe 2")
	}

	if mockIn.ts != mockPi1.tsIn {
		t.Error("Wrong messages chan in mock pipe 1")
	}
	if mockPi1.ts != mockPi2.tsIn {
		t.Error("Wrong messages chan in mock pipe 2")
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	newInput.TriggerStopConsuming()
	require.NoError(t, newInput.WaitForClose(ctx))

	select {
	case _, open := <-mockIn.ts:
		if open {
			t.Error("mock input is still open after close")
		}
	case _, open := <-mockPi1.ts:
		if open {
			t.Error("mock pipe is still open after close")
		}
	case _, open := <-mockPi2.ts:
		if open {
			t.Error("mock pipe is still open after close")
		}
	default:
		t.Error("neither type was closed")
	}
}

//------------------------------------------------------------------------------

type mockProc struct{}

func (m mockProc) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	msgs := [1]message.Batch{msg}
	return msgs[:], nil
}

func (m mockProc) Close(ctx context.Context) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

//------------------------------------------------------------------------------

func TestBasicWrapProcessors(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	mockIn := &mockInput{ts: make(chan message.Transaction)}

	pipe1 := pipeline.NewProcessor(mockProc{})
	pipe2 := pipeline.NewProcessor(mockProc{})

	newInput, err := input.WrapWithPipelines(mockIn, func() (iprocessor.Pipeline, error) {
		return pipe1, nil
	}, func() (iprocessor.Pipeline, error) {
		return pipe2, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan error)

	msg := message.QuickBatch([][]byte{[]byte("baz")})

	select {
	case mockIn.ts <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should not be discarded
	var ts message.Transaction
	var open bool
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		t.Errorf("Unexpected response: %v", res)
	case ts, open = <-newInput.TransactionChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(ts.Payload.Get(0).AsBytes()); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")

	// Send error
	go func() {
		require.NoError(t, ts.Ack(tCtx, errFailed))
	}()

	// Receive again
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		if res != errFailed {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.TriggerStopConsuming()
	require.NoError(t, newInput.WaitForClose(tCtx))
}

func TestBasicWrapDoubleProcessors(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*20)
	defer done()

	mockIn := &mockInput{ts: make(chan message.Transaction)}

	pipe1 := pipeline.NewProcessor(mockProc{}, mockProc{})

	newInput, err := input.WrapWithPipelines(mockIn, func() (iprocessor.Pipeline, error) {
		return pipe1, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan error)

	msg := message.QuickBatch([][]byte{[]byte("baz")})

	select {
	case mockIn.ts <- message.NewTransaction(msg, resChan):
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	// Message should not be discarded
	var ts message.Transaction
	var open bool
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		t.Errorf("Unexpected response: %v", res)
	case ts, open = <-newInput.TransactionChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(ts.Payload.Get(0).AsBytes()); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")

	// Send error
	go func() {
		require.NoError(t, ts.Ack(tCtx, errFailed))
	}()

	// Receive again
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		if res != errFailed {
			t.Error(res)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.TriggerStopConsuming()
	require.NoError(t, newInput.WaitForClose(tCtx))
}
