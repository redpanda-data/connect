package output

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
)

// MockOutputType implements the output.Type interface.
type MockOutputType struct {
	TChan <-chan message.Transaction
}

// Consume sets the read channel. This implementation is NOT thread safe.
func (m *MockOutputType) Consume(msgs <-chan message.Transaction) error {
	m.TChan = msgs
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (m *MockOutputType) Connected() bool {
	return true
}

// CloseAsync does nothing.
func (m *MockOutputType) CloseAsync() {
}

// WaitForClose does nothing.
func (m MockOutputType) WaitForClose(t time.Duration) error {
	return nil
}
