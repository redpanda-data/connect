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

package output

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Batcher wraps an output with a batching policy.
type Batcher struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	child   Type
	batcher *batch.Policy

	messagesIn  <-chan types.Transaction
	messagesOut chan types.Transaction

	running int32

	closeChan      chan struct{}
	fullyCloseChan chan struct{}
	fullyCloseOnce sync.Once

	closedChan chan struct{}
}

// NewBatcher creates a new Producer/Consumer around a buffer.
func NewBatcher(
	batcher *batch.Policy,
	child Type,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := Batcher{
		stats:          stats,
		log:            log,
		child:          child,
		batcher:        batcher,
		messagesOut:    make(chan types.Transaction),
		running:        1,
		closeChan:      make(chan struct{}),
		fullyCloseChan: make(chan struct{}),
		closedChan:     make(chan struct{}),
	}
	return &m
}

//------------------------------------------------------------------------------

func (m *Batcher) loop() {
	defer func() {
		close(m.messagesOut)
		m.child.CloseAsync()
		err := m.child.WaitForClose(time.Second)
		for err != nil {
			err = m.child.WaitForClose(time.Second)
		}
		close(m.closedChan)
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext >= 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	var pendingResChans []chan<- types.Response
	for atomic.LoadInt32(&m.running) == 1 {
		if nextTimedBatchChan == nil {
			if tNext := m.batcher.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}

		var flushBatch bool
		select {
		case tran, open := <-m.messagesIn:
			if !open {
				// Final flush of remaining documents.
				atomic.StoreInt32(&m.running, 0)
				flushBatch = true
				// If we're waiting for a timed batch then we will respect it.
				if nextTimedBatchChan != nil {
					select {
					case <-nextTimedBatchChan:
					case <-m.closeChan:
						return
					}
				}
			} else {
				tran.Payload.Iter(func(i int, p types.Part) error {
					if m.batcher.Add(p) {
						flushBatch = true
					}
					return nil
				})
				pendingResChans = append(pendingResChans, tran.ResponseChan)
			}
		case <-nextTimedBatchChan:
			flushBatch = true
			nextTimedBatchChan = nil
		case <-m.closeChan:
			atomic.StoreInt32(&m.running, 0)
			flushBatch = true
		}

		if !flushBatch {
			continue
		}

		sendMsg := m.batcher.Flush()
		if sendMsg == nil {
			continue
		}

		resChan := make(chan types.Response)
		select {
		case m.messagesOut <- types.NewTransaction(sendMsg, resChan):
		case <-m.fullyCloseChan:
			return
		}

		go func(rChan chan types.Response, upstreamResChans []chan<- types.Response) {
			select {
			case <-m.fullyCloseChan:
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, c := range upstreamResChans {
					select {
					case <-m.fullyCloseChan:
						return
					case c <- res:
					}
				}
			}
		}(resChan, pendingResChans)
		pendingResChans = nil
	}
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (m *Batcher) Connected() bool {
	return m.child.Connected()
}

// Consume assigns a messages channel for the output to read.
func (m *Batcher) Consume(msgs <-chan types.Transaction) error {
	if m.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	if err := m.child.Consume(m.messagesOut); err != nil {
		return err
	}
	m.messagesIn = msgs
	go m.loop()
	return nil
}

// CloseAsync shuts down the Batcher and stops processing messages.
func (m *Batcher) CloseAsync() {
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// WaitForClose blocks until the Batcher output has closed down.
func (m *Batcher) WaitForClose(timeout time.Duration) error {
	if atomic.LoadInt32(&m.running) == 0 {
		go m.fullyCloseOnce.Do(func() {
			<-time.After(timeout - time.Second)
			close(m.fullyCloseChan)
		})
	}
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
