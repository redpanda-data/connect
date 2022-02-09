package tracing

import (
	"sync/atomic"
	"time"

	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/processor"
)

type tracedProcessor struct {
	e       *events
	errCtr  *uint64
	wrapped iprocessor.V1
}

func traceProcessor(e *events, errCtr *uint64, p iprocessor.V1) iprocessor.V1 {
	t := &tracedProcessor{
		e:       e,
		errCtr:  errCtr,
		wrapped: p,
	}
	return t
}

func (t *tracedProcessor) ProcessMessage(m *message.Batch) ([]*message.Batch, error) {
	prevErrs := make([]string, m.Len())
	_ = m.Iter(func(i int, part *message.Part) error {
		t.e.Add(EventConsume, string(part.Get()))
		prevErrs[i] = processor.GetFail(part)
		return nil
	})

	outMsgs, res := t.wrapped.ProcessMessage(m)
	for _, outMsg := range outMsgs {
		_ = outMsg.Iter(func(i int, part *message.Part) error {
			t.e.Add(EventProduce, string(part.Get()))
			failStr := processor.GetFail(part)
			if failStr == "" {
				return nil
			}
			if len(prevErrs) <= i || prevErrs[i] == failStr {
				return nil
			}
			_ = atomic.AddUint64(t.errCtr, 1)
			t.e.Add(EventError, failStr)
			return nil
		})
	}
	if len(outMsgs) == 0 {
		t.e.Add(EventDelete, "")
	}

	return outMsgs, res
}

func (t *tracedProcessor) CloseAsync() {
	t.wrapped.CloseAsync()
}

func (t *tracedProcessor) WaitForClose(timeout time.Duration) error {
	return t.wrapped.WaitForClose(timeout)
}
