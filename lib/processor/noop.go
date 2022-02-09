package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNoop] = TypeSpec{
		constructor: NewNoop,
		Summary:     "Noop is a processor that does nothing, the message passes through unchanged. Why? Sometimes doing nothing is the braver option.",
		config:      docs.FieldComponent().HasType(docs.FieldTypeObject),
	}
}

//------------------------------------------------------------------------------

// NoopConfig configures the no-op processor.
type NoopConfig struct{}

// NewNoopConfig creates a new default no-op processor config.
func NewNoopConfig() NoopConfig {
	return NoopConfig{}
}

// Noop is a no-op processor that does nothing.
type Noop struct{}

// NewNoop returns a Noop processor.
func NewNoop(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Noop{}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage does nothing and returns the message unchanged.
func (c *Noop) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	msgs := [1]*message.Batch{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Noop) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Noop) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
