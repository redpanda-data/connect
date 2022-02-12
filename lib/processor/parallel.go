package processor

import (
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeParallel] = TypeSpec{
		constructor: NewParallel,
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

// Parallel is a processor that applies a list of child processors to each
// message of a batch individually.
type Parallel struct {
	children []processor.V1
	cap      int

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewParallel returns a Parallel processor.
func NewParallel(
	conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	var children []processor.V1
	for i, pconf := range conf.Parallel.Processors {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}
	return &Parallel{
		children: children,
		cap:      conf.Parallel.Cap,
		log:      log,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *Parallel) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	p.mCount.Incr(1)

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

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	return []*message.Batch{resMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *Parallel) CloseAsync() {
	for _, c := range p.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (p *Parallel) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range p.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
