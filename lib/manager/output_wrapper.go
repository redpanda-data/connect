package manager

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type outputWrapper struct {
	output types.Output

	tranChan  chan message.Transaction
	closeOnce sync.Once
}

func wrapOutput(o types.Output) (*outputWrapper, error) {
	tranChan := make(chan message.Transaction)
	if err := o.Consume(tranChan); err != nil {
		return nil, err
	}
	return &outputWrapper{
		output:   o,
		tranChan: tranChan,
	}, nil
}

func (w *outputWrapper) WriteTransaction(ctx context.Context, t message.Transaction) error {
	select {
	case w.tranChan <- t:
	case <-ctx.Done():
		return component.ErrTimeout
	}
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (w *outputWrapper) Connected() bool {
	return w.output.Connected()
}

func (w *outputWrapper) CloseAsync() {
	w.output.CloseAsync()
}

func (w *outputWrapper) WaitForClose(timeout time.Duration) error {
	w.closeOnce.Do(func() {
		close(w.tranChan)
	})
	return w.output.WaitForClose(timeout)
}
