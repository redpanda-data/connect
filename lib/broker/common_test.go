package broker

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// MockInputType implements the input.Type interface.
type MockInputType struct {
	closed int32
	TChan  chan types.Transaction
}

// TransactionChan returns the messages channel.
func (m *MockInputType) TransactionChan() <-chan types.Transaction {
	return m.TChan
}

// Connected returns true.
func (m *MockInputType) Connected() bool {
	return true
}

// CloseAsync does nothing.
func (m *MockInputType) CloseAsync() {
	if atomic.CompareAndSwapInt32(&m.closed, 0, 1) {
		close(m.TChan)
	}
}

// WaitForClose does nothing.
func (m MockInputType) WaitForClose(t time.Duration) error {
	select {
	case _, open := <-m.TChan:
		if open {
			return errors.New("received unexpected message")
		}
	case <-time.After(t):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// MockOutputType implements the output.Type interface.
type MockOutputType struct {
	TChan <-chan types.Transaction
}

// Connected returns true.
func (m *MockOutputType) Connected() bool {
	return true
}

// Consume sets the read channel. This implementation is NOT thread safe.
func (m *MockOutputType) Consume(msgs <-chan types.Transaction) error {
	m.TChan = msgs
	return nil
}

// CloseAsync does nothing.
func (m *MockOutputType) CloseAsync() {
}

// WaitForClose does nothing.
func (m MockOutputType) WaitForClose(t time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
