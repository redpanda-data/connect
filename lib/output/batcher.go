package output

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/transaction"
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

	ctx           context.Context
	closeFn       func()
	fullyCloseCtx context.Context
	fullyCloseFn  func()

	closedChan chan struct{}
}

func newBatcherFromConf(
	conf batch.PolicyConfig,
	child Type,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if !conf.IsNoop() {
		policy, err := batch.NewPolicy(conf, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		child = NewBatcher(policy, child, log, stats)
	}
	return child, nil
}

// NewBatcher creates a new Producer/Consumer around a buffer.
func NewBatcher(
	batcher *batch.Policy,
	child Type,
	log log.Modular,
	stats metrics.Type,
) Type {
	ctx, cancelFn := context.WithCancel(context.Background())
	fullyCloseCtx, fullyCancelFn := context.WithCancel(context.Background())

	m := Batcher{
		stats:         stats,
		log:           log,
		child:         child,
		batcher:       batcher,
		messagesOut:   make(chan types.Transaction),
		running:       1,
		ctx:           ctx,
		closeFn:       cancelFn,
		fullyCloseCtx: fullyCloseCtx,
		fullyCloseFn:  fullyCancelFn,
		closedChan:    make(chan struct{}),
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
		m.batcher.CloseAsync()
		err = m.batcher.WaitForClose(time.Second)
		for err != nil {
			m.log.Warnf("Waiting for batch policy to close, blocked by: %v\n", err)
			err = m.batcher.WaitForClose(time.Second)
		}
		close(m.closedChan)
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext >= 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	var pendingTrans []*transaction.Tracked
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
					case <-m.ctx.Done():
					}
				}
			} else {
				trackedTran := transaction.NewTracked(tran.Payload, tran.ResponseChan)
				trackedTran.Message().Iter(func(i int, p types.Part) error {
					if m.batcher.Add(p) {
						flushBatch = true
					}
					return nil
				})
				pendingTrans = append(pendingTrans, trackedTran)
			}
		case <-nextTimedBatchChan:
			flushBatch = true
			nextTimedBatchChan = nil
		case <-m.ctx.Done():
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
		case <-m.fullyCloseCtx.Done():
			return
		}

		go func(rChan chan types.Response, upstreamTrans []*transaction.Tracked) {
			select {
			case <-m.fullyCloseCtx.Done():
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, t := range upstreamTrans {
					t.Ack(m.fullyCloseCtx, res.Error())
				}
			}
		}(resChan, pendingTrans)
		pendingTrans = nil
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
	atomic.StoreInt32(&m.running, 0)
	m.closeFn()
}

// WaitForClose blocks until the Batcher output has closed down.
func (m *Batcher) WaitForClose(timeout time.Duration) error {
	if atomic.LoadInt32(&m.running) == 0 {
		go func() {
			<-time.After(timeout - time.Second)
			m.fullyCloseFn()
		}()
	}
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
