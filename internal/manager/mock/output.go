package mock

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// OutputWriter provides a mock implementation of types.OutputWriter.
type OutputWriter func(context.Context, message.Transaction) error

// WriteTransaction attempts to write a transaction to an output.
func (o OutputWriter) WriteTransaction(ctx context.Context, t message.Transaction) error {
	return o(ctx, t)
}

// Connected always returns true.
func (o OutputWriter) Connected() bool {
	return true
}

// TriggerStopConsuming does nothing.
func (o OutputWriter) TriggerStopConsuming() {
}

// TriggerCloseNow does nothing.
func (o OutputWriter) TriggerCloseNow() {
}

// WaitForClose does nothing.
func (o OutputWriter) WaitForClose(ctx context.Context) error {
	return nil
}

// OutputChanneled implements the output.Type interface around an exported
// transaction channel.
type OutputChanneled struct {
	TChan <-chan message.Transaction
}

// Connected returns true.
func (m *OutputChanneled) Connected() bool {
	return true
}

// Consume sets the read channel. This implementation is NOT thread safe.
func (m *OutputChanneled) Consume(msgs <-chan message.Transaction) error {
	m.TChan = msgs
	return nil
}

// TriggerCloseNow does nothing.
func (m *OutputChanneled) TriggerCloseNow() {
}

// WaitForClose does nothing.
func (m OutputChanneled) WaitForClose(ctx context.Context) error {
	return nil
}
