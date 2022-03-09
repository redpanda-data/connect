package writer

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

//------------------------------------------------------------------------------

// DropConfig contains configuration fields for the drop output type.
type DropConfig struct{}

// NewDropConfig creates a new DropConfig with default values.
func NewDropConfig() DropConfig {
	return DropConfig{}
}

//------------------------------------------------------------------------------

// Drop is a benthos writer.Type implementation that writes message parts to no
// where.
type Drop struct {
	log log.Modular
}

// NewDrop creates a new file based writer.Type.
func NewDrop(
	conf DropConfig,
	log log.Modular,
	stats metrics.Type,
) *Drop {
	return &Drop{
		log: log,
	}
}

// ConnectWithContext is a noop.
func (d *Drop) ConnectWithContext(ctx context.Context) error {
	d.log.Infoln("Dropping messages.")
	return nil
}

// WriteWithContext does nothing.
func (d *Drop) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (d *Drop) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (d *Drop) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
