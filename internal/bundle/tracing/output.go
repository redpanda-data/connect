package tracing

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

type tracedOutput struct {
	e       *events
	ctr     *uint64
	wrapped types.Output
	tChan   chan message.Transaction
	shutSig *shutdown.Signaller
}

func traceOutput(e *events, ctr *uint64, i types.Output) types.Output {
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
			t.e.Add(EventConsume, string(part.Get()))
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

func (t *tracedOutput) CloseAsync() {
	t.wrapped.CloseAsync()
}

func (t *tracedOutput) WaitForClose(timeout time.Duration) error {
	err := t.wrapped.WaitForClose(timeout)
	t.shutSig.CloseNow()
	return err
}
