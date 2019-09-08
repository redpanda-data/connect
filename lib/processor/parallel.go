// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeParallel] = TypeSpec{
		constructor: NewParallel,
		description: `
A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message (similar to the
` + "[`for_each`](#for_each)" + ` processor), but where each message is
processed in parallel.

The field ` + "`cap`" + `, if greater than zero, caps the maximum number of
parallel processing threads.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.Parallel.Processors))
			for i, pConf := range conf.Parallel.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"cap":        conf.Parallel.Cap,
				"processors": procConfs,
			}, nil
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
	children []types.Processor
	cap      int

	log log.Modular

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewParallel returns a Parallel processor.
func NewParallel(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	var children []types.Processor
	for i, pconf := range conf.Parallel.Processors {
		prefix := fmt.Sprintf("%v", i)
		proc, err := New(pconf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
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
func (p *Parallel) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	resultMsgs := make([]types.Message, msg.Len())
	msg.Iter(func(i int, p types.Part) error {
		tmpMsg := message.New(nil)
		tmpMsg.SetAll([]types.Part{p})
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

	var unAcks int32
	for i := 0; i < max; i++ {
		go func() {
			for index := range reqChan {
				resMsgs, res := ExecuteAll(p.children, resultMsgs[index])
				if res != nil && res.SkipAck() {
					atomic.AddInt32(&unAcks, 1)
				}
				resultParts := []types.Part{}
				for _, m := range resMsgs {
					m.Iter(func(i int, p types.Part) error {
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

	resMsg := message.New(nil)
	for _, m := range resultMsgs {
		m.Iter(func(i int, p types.Part) error {
			resMsg.Append(p)
			return nil
		})
	}
	if resMsg.Len() == 0 && unAcks == int32(msg.Len()) {
		return nil, response.NewUnack()
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(resMsg.Len()))

	return []types.Message{resMsg}, nil
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
