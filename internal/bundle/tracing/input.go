package tracing

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/message"
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
			t.e.Add(EventProduce, string(part.Get()))
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

func (t *tracedInput) CloseAsync() {
	t.wrapped.CloseAsync()
}

func (t *tracedInput) WaitForClose(timeout time.Duration) error {
	err := t.wrapped.WaitForClose(timeout)
	t.shutSig.CloseNow()
	return err
}
