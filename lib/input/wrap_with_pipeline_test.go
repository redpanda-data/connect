package input

import (
	"errors"
	"sync"
	"testing"
	"time"

	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/response"
)

//------------------------------------------------------------------------------

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

func (m *mockInput) CloseAsync() {
	m.closeOnce.Do(func() {
		close(m.ts)
	})
}

func (m *mockInput) WaitForClose(time.Duration) error {
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

func (m *mockPipe) CloseAsync() {
	close(m.ts)
}

func (m *mockPipe) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

func TestBasicWrapPipeline(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}
	mockPi := &mockPipe{
		ts: make(chan message.Transaction),
	}

	procs := 0
	_, err := WrapWithPipeline(&procs, mockIn, func(i *int) (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})

	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err := WrapWithPipeline(&procs, mockIn, func(i *int) (iprocessor.Pipeline, error) {
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

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

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
	newInput, err := WrapWithPipelines(mockIn)
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

	_, err := WrapWithPipelines(mockIn, func(i *int) (iprocessor.Pipeline, error) {
		return nil, errors.New("nope")
	})
	if err == nil {
		t.Error("Expected error from back constructor")
	}

	newInput, err := WrapWithPipelines(mockIn, func(i *int) (iprocessor.Pipeline, error) {
		return mockPi1, nil
	}, func(i *int) (iprocessor.Pipeline, error) {
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

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

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

type mockProc struct {
}

func (m mockProc) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	msgs := [1]*message.Batch{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m mockProc) CloseAsync() {
	// Do nothing as our processor doesn't require resource cleanup.
}

// WaitForClose blocks until the processor has closed down.
func (m mockProc) WaitForClose(timeout time.Duration) error {
	// Do nothing as our processor doesn't require resource cleanup.
	return nil
}

//------------------------------------------------------------------------------

func TestBasicWrapProcessors(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}

	l := log.Noop()
	s := metrics.Noop()

	pipe1 := pipeline.NewProcessor(l, s, mockProc{})
	pipe2 := pipeline.NewProcessor(l, s, mockProc{})

	newInput, err := WrapWithPipelines(mockIn, func(i *int) (iprocessor.Pipeline, error) {
		return pipe1, nil
	}, func(i *int) (iprocessor.Pipeline, error) {
		return pipe2, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan response.Error)

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
		t.Errorf("Unexpected response: %v", res.AckError())
	case ts, open = <-newInput.TransactionChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(ts.Payload.Get(0).Get()); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")

	// Send error
	go func() {
		select {
		case ts.ResponseChan <- response.NewError(errFailed):
		case <-time.After(time.Second):
			t.Error("action timed out")
		}
	}()

	// Receive again
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		if res.AckError() != errFailed {
			t.Error(res.AckError())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestBasicWrapDoubleProcessors(t *testing.T) {
	mockIn := &mockInput{ts: make(chan message.Transaction)}

	l := log.Noop()
	s := metrics.Noop()

	pipe1 := pipeline.NewProcessor(l, s, mockProc{}, mockProc{})

	newInput, err := WrapWithPipelines(mockIn, func(i *int) (iprocessor.Pipeline, error) {
		return pipe1, nil
	})
	if err != nil {
		t.Error(err)
	}

	resChan := make(chan response.Error)

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
		t.Errorf("Unexpected response: %v", res.AckError())
	case ts, open = <-newInput.TransactionChan():
		if !open {
			t.Error("channel was closed")
		} else if exp, act := "baz", string(ts.Payload.Get(0).Get()); exp != act {
			t.Errorf("Wrong message received: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	errFailed := errors.New("derp, failed")

	// Send error
	go func() {
		select {
		case ts.ResponseChan <- response.NewError(errFailed):
		case <-time.After(time.Second):
			t.Error("action timed out")
		}
	}()

	// Receive again
	select {
	case res, open := <-resChan:
		if !open {
			t.Error("Channel was closed")
		}
		if res.AckError() != errFailed {
			t.Error(res.AckError())
		}
	case <-time.After(time.Second):
		t.Error("action timed out")
	}

	newInput.CloseAsync()
	if err = newInput.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

//------------------------------------------------------------------------------
