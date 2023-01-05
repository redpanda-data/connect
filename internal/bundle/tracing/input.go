package tracing

import (
	"context"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

type tracedInput struct {
	e       *events
	ctr     *uint64
	wrapped input.Streamed
	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func traceInput(e *events, counter *uint64, i input.Streamed) input.Streamed {
	t := &tracedInput{
		e:       e,
		ctr:     counter,
		wrapped: i,
		tChan:   make(chan message.Transaction),
		shutSig: shutdown.NewSignaller(),
	}
	go t.loop()
	return t
}

func (t *tracedInput) loop() {
	defer close(t.tChan)
	readChan := t.wrapped.TransactionChan()
	for {
		tran, open := <-readChan
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
			t.e.Add(EventProduce, string(part.AsBytes()), meta)
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

func (t *tracedInput) TransactionChan() <-chan message.Transaction {
	return t.tChan
}

func (t *tracedInput) Connected() bool {
	return t.wrapped.Connected()
}

func (t *tracedInput) TriggerStopConsuming() {
	t.wrapped.TriggerStopConsuming()
}

func (t *tracedInput) TriggerCloseNow() {
	t.wrapped.TriggerCloseNow()
	t.shutSig.CloseNow()
}

func (t *tracedInput) WaitForClose(ctx context.Context) error {
	err := t.wrapped.WaitForClose(ctx)
	t.shutSig.CloseNow()
	return err
}
