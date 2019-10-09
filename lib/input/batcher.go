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

package input

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Batcher wraps an input with a batch policy.
type Batcher struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	child   Type
	batcher *batch.Policy

	messagesOut chan types.Transaction

	ctx           context.Context
	closeFn       func()
	fullyCloseCtx context.Context
	fullyCloseFn  func()

	closedChan chan struct{}
}

// NewBatcher creates a new Batcher around an input.
func NewBatcher(
	batcher *batch.Policy,
	child Type,
	log log.Modular,
	stats metrics.Type,
) Type {
	ctx, cancelFn := context.WithCancel(context.Background())
	fullyCloseCtx, fullyCancelFn := context.WithCancel(context.Background())

	b := Batcher{
		stats:       stats,
		log:         log,
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan types.Transaction),

		ctx:           ctx,
		closeFn:       cancelFn,
		fullyCloseCtx: fullyCloseCtx,
		fullyCloseFn:  fullyCancelFn,
		closedChan:    make(chan struct{}),
	}
	go b.loop()
	return &b
}

//------------------------------------------------------------------------------

func (m *Batcher) loop() {
	defer func() {
		m.child.CloseAsync()
		err := m.child.WaitForClose(time.Second)
		for err == types.ErrTimeout {
			err = m.child.WaitForClose(time.Second)
		}
		close(m.messagesOut)
		close(m.closedChan)
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext >= 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	pendingResChans := []chan<- types.Response{}
	pendingAcks := sync.WaitGroup{}

	flushBatchFn := func() {
		sendMsg := m.batcher.Flush()
		if sendMsg == nil {
			return
		}

		resChan := make(chan types.Response)
		select {
		case m.messagesOut <- types.NewTransaction(sendMsg, resChan):
		case <-m.fullyCloseCtx.Done():
			return
		}

		pendingAcks.Add(1)
		go func(rChan <-chan types.Response, aggregatedResChans []chan<- types.Response) {
			defer pendingAcks.Done()

			select {
			case <-m.fullyCloseCtx.Done():
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, c := range aggregatedResChans {
					select {
					case <-m.fullyCloseCtx.Done():
						return
					case c <- res:
					}
				}
			}
		}(resChan, pendingResChans)
		pendingResChans = nil
	}

	defer func() {
		// Final flush of remaining documents.
		m.log.Debugln("Flushing remaining messages of batch.")
		flushBatchFn()

		// Wait for all pending acks to resolve.
		m.log.Debugln("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		m.log.Debugln("Pending acks resolved.")
	}()

	for {
		if nextTimedBatchChan == nil {
			if tNext := m.batcher.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}

		var flushBatch bool
		select {
		case tran, open := <-m.child.TransactionChan():
			if !open {
				// If we're waiting for a timed batch then we will respect it.
				if nextTimedBatchChan != nil {
					select {
					case <-nextTimedBatchChan:
					case <-m.ctx.Done():
					}
				}
				return
			}
			tran.Payload.Iter(func(i int, p types.Part) error {
				if m.batcher.Add(p) {
					flushBatch = true
				}
				return nil
			})
			pendingResChans = append(pendingResChans, tran.ResponseChan)
		case <-nextTimedBatchChan:
			flushBatch = true
			nextTimedBatchChan = nil
		case <-m.ctx.Done():
			return
		}

		if flushBatch {
			flushBatchFn()
		}
	}
}

// Connected returns true if the underlying input is connected.
func (m *Batcher) Connected() bool {
	return m.child.Connected()
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *Batcher) TransactionChan() <-chan types.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the Batcher and stops processing messages.
func (m *Batcher) CloseAsync() {
	m.closeFn()
}

// WaitForClose blocks until the Batcher output has closed down.
func (m *Batcher) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		m.fullyCloseFn()
	}()
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
