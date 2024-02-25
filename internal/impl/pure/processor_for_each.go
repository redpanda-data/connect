package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("for_each", service.NewConfigSpec().
		Stable().
		Categories("Composition").
		Summary("A processor that applies a list of child processors to messages of a batch as though they were each a batch of one message.").
		Description(`
This is useful for forcing batch wide processors such as `+"[`dedupe`](/docs/components/processors/dedupe)"+` or interpolations such as the `+"`value`"+` field of the `+"`metadata`"+` processor to execute on individual message parts of a batch instead.

Please note that most processors already process per message of a batch, and this processor is not needed in those cases.`).
		Field(service.NewProcessorListField("").Default([]any{})),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			mgr := interop.UnwrapManagement(res)
			childPubProcs, err := conf.FieldProcessorList()
			if err != nil {
				return nil, err
			}

			childProcs := make([]processor.V1, len(childPubProcs))
			for i, p := range childPubProcs {
				childProcs[i] = interop.UnwrapOwnedProcessor(p)
			}

			tp, err := newForEach(childProcs, mgr)
			if err != nil {
				return nil, err
			}

			p := processor.NewAutoObservedBatchedProcessor("for_each", tp, mgr)
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

type forEachProc struct {
	children []processor.V1
}

func newForEach(children []processor.V1, mgr bundle.NewManagement) (*forEachProc, error) {
	return &forEachProc{children: children}, nil
}

func (p *forEachProc) ProcessBatch(ctx *processor.BatchProcContext, msg message.Batch) ([]message.Batch, error) {
	individualMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		individualMsgs[i] = message.Batch{p}
		return nil
	})

	resMsg := message.QuickBatch(nil)
	for _, tmpMsg := range individualMsgs {
		resultMsgs, err := processor.ExecuteAll(ctx.Context(), p.children, tmpMsg)
		if err != nil {
			return nil, err
		}
		for _, m := range resultMsgs {
			_ = m.Iter(func(i int, p *message.Part) error {
				resMsg = append(resMsg, p)
				return nil
			})
		}
	}

	if resMsg.Len() == 0 {
		return nil, nil
	}
	return []message.Batch{resMsg}, nil
}

func (p *forEachProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
