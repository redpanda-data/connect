package tracing

import (
	"context"
	"sync/atomic"

	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
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

func (t *tracedProcessor) ProcessBatch(ctx context.Context, m message.Batch) ([]message.Batch, error) {
	prevErrs := make([]error, m.Len())
	_ = m.Iter(func(i int, part *message.Part) error {
		meta := map[string]any{}
		_ = part.MetaIterMut(func(s string, a any) error {
			meta[s] = message.CopyJSON(a)
			return nil
		})
		t.e.Add(EventConsume, string(part.AsBytes()), meta)
		prevErrs[i] = part.ErrorGet()
		return nil
	})

	outMsgs, res := t.wrapped.ProcessBatch(ctx, m)
	for _, outMsg := range outMsgs {
		_ = outMsg.Iter(func(i int, part *message.Part) error {
			meta := map[string]any{}
			_ = part.MetaIterMut(func(s string, a any) error {
				meta[s] = message.CopyJSON(a)
				return nil
			})
			t.e.Add(EventProduce, string(part.AsBytes()), meta)
			fail := part.ErrorGet()
			if fail == nil {
				return nil
			}
			// TODO: Improve mechanism for tracking the introduction of errors?
			if len(prevErrs) <= i || prevErrs[i] == fail {
				return nil
			}
			_ = atomic.AddUint64(t.errCtr, 1)
			t.e.Add(EventError, fail.Error(), nil)
			return nil
		})
	}
	if len(outMsgs) == 0 {
		t.e.Add(EventDelete, "", nil)
	}

	return outMsgs, res
}

func (t *tracedProcessor) Close(ctx context.Context) error {
	return t.wrapped.Close(ctx)
}
