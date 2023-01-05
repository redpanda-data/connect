package tracing

import (
	"context"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

type tracedOutput struct {
	e       *events
	ctr     *uint64
	wrapped output.Streamed
	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func traceOutput(e *events, ctr *uint64, i output.Streamed) output.Streamed {
	t := &tracedOutput{
		e:       e,
		ctr:     ctr,
		wrapped: i,
		tChan:   make(chan message.Transaction),
		shutSig: shutdown.NewSignaller(),
	}
	return t
}

func (t *tracedOutput) loop(inChan <-chan message.Transaction) {
	defer close(t.tChan)
	for {
		tran, open := <-inChan
		if !open {
			return
		}
		_ = tran.Payload.Iter(func(i int, part *message.Part) error {
			_ = atomic.AddUint64(t.ctr, 1)
			meta := map[string]any{}
			_ = part.MetaIterMut(func(s string, a any) error {
				meta[s] = message.CopyJSON(a)
				return nil
			})
			t.e.Add(EventConsume, string(part.AsBytes()), meta)
			return nil
		})
		select {
		case t.tChan <- tran:
		case <-t.shutSig.CloseNowChan():
			// Stop flushing if we fully timed out
			return
		}
	}
}

func (t *tracedOutput) Consume(inChan <-chan message.Transaction) error {
	go t.loop(inChan)
	return t.wrapped.Consume(t.tChan)
}

func (t *tracedOutput) Connected() bool {
	return t.wrapped.Connected()
}

func (t *tracedOutput) TriggerCloseNow() {
	t.wrapped.TriggerCloseNow()
}

func (t *tracedOutput) WaitForClose(ctx context.Context) error {
	err := t.wrapped.WaitForClose(ctx)
	t.shutSig.CloseNow()
	return err
}
