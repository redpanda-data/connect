// Copyright (c) 2017 Ashley Jeffs
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

package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	running int32

	log   log.Modular
	stats metrics.Type

	msgProcessors []types.Processor

	messagesOut chan types.Transaction
	responsesIn chan types.Response

	messagesIn <-chan types.Transaction

	mSndSucc metrics.StatCounter
	mSndErr  metrics.StatCounter

	closeChan chan struct{}
	closed    chan struct{}
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(
	log log.Modular,
	stats metrics.Type,
	msgProcessors ...types.Processor,
) *Processor {
	return &Processor{
		running:       1,
		msgProcessors: msgProcessors,
		log:           log.NewModule(".pipeline.processor"),
		stats:         stats,
		mSndSucc:      stats.GetCounter("pipeline.processor.send.success"),
		mSndErr:       stats.GetCounter("pipeline.processor.send.error"),
		messagesOut:   make(chan types.Transaction),
		responsesIn:   make(chan types.Response),
		closeChan:     make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Processor) loop() {
	defer func() {
		close(p.messagesOut)
		close(p.closed)
	}()

	var (
		mProcCount   = p.stats.GetCounter("pipeline.processor.count")
		mProcDropped = p.stats.GetCounter("pipeline.processor.dropped")
	)

	var open bool
	for atomic.LoadInt32(&p.running) == 1 {
		var tran types.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.closeChan:
			return
		}
		mProcCount.Incr(1)

		resultMsgs, resultRes := processor.ExecuteAll(p.msgProcessors, tran.Payload)
		if len(resultMsgs) == 0 {
			mProcDropped.Incr(1)
			select {
			case tran.ResponseChan <- resultRes:
			case <-p.closeChan:
				return
			}
			continue
		}

		if len(resultMsgs) > 1 {
			p.dispatchMessages(resultMsgs, tran.ResponseChan)
		} else {
			select {
			case p.messagesOut <- types.NewTransaction(resultMsgs[0], tran.ResponseChan):
			case <-p.closeChan:
				return
			}
		}
	}
}

// dispatchMessages attempts to send a multiple messages results of processors
// over the shared messages channel. This send is retried until success.
func (p *Processor) dispatchMessages(msgs []types.Message, ogResChan chan<- types.Response) {
	throt := throttle.New(throttle.OptCloseChan(p.closeChan))

	var skipAcks int64
	sendMsg := func(m types.Message) {
		resChan := make(chan types.Response)
		transac := types.NewTransaction(m, resChan)

		for {
			select {
			case p.messagesOut <- transac:
			case <-p.closeChan:
				return
			}

			var res types.Response
			var open bool
			select {
			case res, open = <-resChan:
				if !open {
					return
				}
			case <-p.closeChan:
				return
			}

			if skipAck := res.SkipAck(); res.Error() == nil || skipAck {
				if skipAck {
					atomic.AddInt64(&skipAcks, 1)
				}
				p.mSndSucc.Incr(1)
				return
			}
			p.mSndErr.Incr(1)
			if !throt.Retry() {
				return
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	for _, msg := range msgs {
		go func(m types.Message) {
			defer wg.Done()
			sendMsg(m)
		}(msg)
	}

	wg.Wait()
	throt.Reset()

	var res types.Response
	if skipAcks == int64(len(msgs)) {
		res = response.NewUnack()
	} else {
		res = response.NewAck()
	}

	select {
	case ogResChan <- res:
	case <-p.closeChan:
		return
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Processor) Consume(msgs <-chan types.Transaction) error {
	if p.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Processor) TransactionChan() <-chan types.Transaction {
	return p.messagesOut
}

// CloseAsync shuts down the pipeline and stops processing messages.
func (p *Processor) CloseAsync() {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closeChan)
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (p *Processor) WaitForClose(timeout time.Duration) error {
	select {
	case <-p.closed:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
