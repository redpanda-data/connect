package buffer

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ParallelBatcher wraps a buffer with a Producer/Consumer interface.
type ParallelBatcher struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	child   Type
	batcher *batch.Policy

	messagesOut chan types.Transaction

	running    int32
	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewParallelBatcher creates a new Producer/Consumer around a buffer.
func NewParallelBatcher(
	batcher *batch.Policy,
	child Type,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := ParallelBatcher{
		stats:       stats,
		log:         log,
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan types.Transaction),
		running:     1,
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}
	return &m
}

//------------------------------------------------------------------------------

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *ParallelBatcher) outputLoop() {
	defer func() {
		m.child.CloseAsync()
		err := m.child.WaitForClose(time.Second)
		for err != nil {
			err = m.child.WaitForClose(time.Second)
		}
		m.batcher.CloseAsync()
		err = m.batcher.WaitForClose(time.Second)
		for err != nil {
			err = m.batcher.WaitForClose(time.Second)
		}
		close(m.messagesOut)
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
		case tran, open := <-m.child.TransactionChan():
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
			return
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
		case <-m.closeChan:
			return
		}

		go func(rChan chan types.Response, upstreamResChans []chan<- types.Response) {
			select {
			case <-m.closeChan:
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, c := range upstreamResChans {
					select {
					case <-m.closeChan:
						return
					case c <- res:
					}
				}
			}
		}(resChan, pendingResChans)
		pendingResChans = nil
	}
}

// Consume assigns a messages channel for the output to read.
func (m *ParallelBatcher) Consume(msgs <-chan types.Transaction) error {
	if err := m.child.Consume(msgs); err != nil {
		return err
	}
	go m.outputLoop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *ParallelBatcher) TransactionChan() <-chan types.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the ParallelBatcher and stops processing messages.
func (m *ParallelBatcher) CloseAsync() {
	m.child.CloseAsync()
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// StopConsuming instructs the buffer to stop consuming messages and close once
// the buffer is empty.
func (m *ParallelBatcher) StopConsuming() {
	m.child.StopConsuming()
}

// WaitForClose blocks until the ParallelBatcher output has closed down.
func (m *ParallelBatcher) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
