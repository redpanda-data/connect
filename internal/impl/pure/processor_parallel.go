package pure

import (
	"context"
	"strconv"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newParallel(conf.Parallel, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewV2BatchedToV1Processor("parallel", p, mgr), nil
	}, docs.ComponentSpec{
		Name: "parallel",
		Categories: []string{
			"Composition",
		},
		Summary: `
A processor that applies a list of child processors to messages of a batch as though they were each a batch of one message (similar to the ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor), but where each message is processed in parallel.`,
		Description: `
The field ` + "`cap`" + `, if greater than zero, caps the maximum number of parallel processing threads.

The functionality of this processor depends on being applied across messages that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldInt("cap", "The maximum number of messages to have processing at a given time."),
			docs.FieldProcessor("processors", "A list of child processors to apply.").Array(),
		).ChildDefaultAndTypesFromStruct(processor.NewParallelConfig()),
	})
	if err != nil {
		panic(err)
	}
}

type parallelProc struct {
	children []processor.V1
	cap      int
}

func newParallel(conf processor.ParallelConfig, mgr bundle.NewManagement) (processor.V2Batched, error) {
	var children []processor.V1
	for i, pconf := range conf.Processors {
		pMgr := mgr.IntoPath("parallel", strconv.Itoa(i))
		proc, err := pMgr.NewProcessor(pconf)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &parallelProc{
		children: children,
		cap:      conf.Cap,
	}, nil
}

func (p *parallelProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		resultMsgs[i] = message.Batch{p}
		return nil
	})

	max := p.cap
	if max == 0 || msg.Len() < max {
		max = msg.Len()
	}

	reqChan := make(chan int)
	wg := sync.WaitGroup{}
	wg.Add(max)

	for i := 0; i < max; i++ {
		go func() {
			// TODO: V4 Handle processor errors when we migrate to service APIs
			for index := range reqChan {
				resMsgs, _ := processor.ExecuteAll(ctx, p.children, resultMsgs[index])
				resultParts := []*message.Part{}
				for _, m := range resMsgs {
					_ = m.Iter(func(i int, p *message.Part) error {
						resultParts = append(resultParts, p)
						return nil
					})
				}
				resultMsgs[index] = resultParts
			}
			wg.Done()
		}()
	}
	for i := 0; i < msg.Len(); i++ {
		reqChan <- i
	}
	close(reqChan)
	wg.Wait()

	resMsg := message.QuickBatch(nil)
	for _, m := range resultMsgs {
		_ = m.Iter(func(i int, p *message.Part) error {
			resMsg = append(resMsg, p)
			return nil
		})
	}

	return []message.Batch{resMsg}, nil
}

func (p *parallelProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		if err := c.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
