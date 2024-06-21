package manager

import (
	"context"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/benthosdev/benthos/v4/internal/component"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
)

var _ ioutput.Sync = &outputWrapper{}

type outputWrapper struct {
	output  ioutput.Streamed
	shutSig *shutdown.Signaller

	tranChan chan message.Transaction
	tranMut  sync.RWMutex
}

func wrapOutput(o ioutput.Streamed) (*outputWrapper, error) {
	tranChan := make(chan message.Transaction)
	if err := o.Consume(tranChan); err != nil {
		return nil, err
	}
	return &outputWrapper{
		output:   o,
		shutSig:  shutdown.NewSignaller(),
		tranChan: tranChan,
	}, nil
}

func (w *outputWrapper) WriteTransaction(ctx context.Context, t message.Transaction) error {
	w.tranMut.RLock()
	defer w.tranMut.RUnlock()
	select {
	case w.tranChan <- t:
	case <-w.shutSig.SoftStopChan():
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

func (w *outputWrapper) TriggerStopConsuming() {
	w.shutSig.TriggerSoftStop()
	w.tranMut.Lock()
	if w.tranChan != nil {
		close(w.tranChan)
		w.tranChan = nil
	}
	w.tranMut.Unlock()
}

func (w *outputWrapper) TriggerCloseNow() {
	w.output.TriggerCloseNow()
}

func (w *outputWrapper) WaitForClose(ctx context.Context) error {
	return w.output.WaitForClose(ctx)
}
