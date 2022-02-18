package processor

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeParallel] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newParallel(conf.Parallel, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("parallel", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message (similar to the
` + "[`for_each`](/docs/components/processors/for_each)" + ` processor), but where each message is
processed in parallel.`,
		Description: `
The field ` + "`cap`" + `, if greater than zero, caps the maximum number of
parallel processing threads.`,
		UsesBatches: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("cap", "The maximum number of messages to have processing at a given time."),
			docs.FieldCommon("processors", "A list of child processors to apply.").Array().HasType(docs.FieldTypeProcessor),
		},
	}
}

//------------------------------------------------------------------------------

// ParallelConfig is a config struct containing fields for the Parallel
// processor.
type ParallelConfig struct {
	Cap        int      `json:"cap" yaml:"cap"`
	Processors []Config `json:"processors" yaml:"processors"`
}

// NewParallelConfig returns a default ParallelConfig.
func NewParallelConfig() ParallelConfig {
	return ParallelConfig{
		Cap:        0,
		Processors: []Config{},
	}
}

//------------------------------------------------------------------------------

type parallelProc struct {
	children []processor.V1
	cap      int
}

func newParallel(conf ParallelConfig, mgr interop.Manager) (processor.V2Batched, error) {
	var children []processor.V1
	for i, pconf := range conf.Processors {
		pMgr := mgr.IntoPath("parallel", strconv.Itoa(i))
		proc, err := New(pconf, pMgr, pMgr.Logger(), pMgr.Metrics())
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

func (p *parallelProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	resultMsgs := make([]*message.Batch, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.SetAll([]*message.Part{p})
		resultMsgs[i] = tmpMsg
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
				resMsgs, _ := ExecuteAll(p.children, resultMsgs[index])
				resultParts := []*message.Part{}
				for _, m := range resMsgs {
					_ = m.Iter(func(i int, p *message.Part) error {
						resultParts = append(resultParts, p)
						return nil
					})
				}
				resultMsgs[index].SetAll(resultParts)
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
			resMsg.Append(p)
			return nil
		})
	}

	return []*message.Batch{resMsg}, nil
}

func (p *parallelProc) Close(ctx context.Context) error {
	for _, c := range p.children {
		c.CloseAsync()
	}
	deadline, exists := ctx.Deadline()
	if !exists {
		deadline = time.Now().Add(time.Second * 5)
	}
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(deadline)); err != nil {
			return err
		}
	}
	return nil
}
