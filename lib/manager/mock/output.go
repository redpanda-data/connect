package mock

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

// OutputWriter provides a mock implementation of types.OutputWriter.
type OutputWriter func(context.Context, types.Transaction) error

// WriteTransaction attempts to write a transaction to an output.
func (o OutputWriter) WriteTransaction(ctx context.Context, t types.Transaction) error {
	return o(ctx, t)
}

// Connected always returns true.
func (o OutputWriter) Connected() bool {
	return true
}

// CloseAsync does nothing.
func (o OutputWriter) CloseAsync() {
}

// WaitForClose does nothing.
func (o OutputWriter) WaitForClose(time.Duration) error {
	return nil
}
