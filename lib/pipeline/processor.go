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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
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
		stats:         stats,
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
		// Signal all children to close.
		for _, c := range p.msgProcessors {
			c.CloseAsync()
		}

		close(p.messagesOut)
		close(p.closed)
	}()

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

		resultMsgs, resultRes := processor.ExecuteAll(p.msgProcessors, tran.Payload)
		if len(resultMsgs) == 0 {
			if resultRes == nil {
				resultRes = response.NewUnack()
				p.log.Warnln("Nil response returned with zero messages from processors")
			}
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
				return
			}
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

		// Signal all children to close.
		for _, c := range p.msgProcessors {
			c.CloseAsync()
		}
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (p *Processor) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	select {
	case <-p.closed:
	case <-time.After(time.Until(stopBy)):
		return types.ErrTimeout
	}

	// Wait for all processors to close.
	for _, c := range p.msgProcessors {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
