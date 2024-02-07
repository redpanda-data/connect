package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

// Impl wraps an input with a batch policy.
type Impl struct {
	log log.Modular

	child   input.Streamed
	batcher *policy.Batcher

	messagesOut chan message.Transaction

	shutSig *shutdown.Signaller
}

// New creates a new Batcher around an input.
func New(batcher *policy.Batcher, child input.Streamed, log log.Modular) input.Streamed {
	b := Impl{
		log:         log,
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan message.Transaction),
		shutSig:     shutdown.NewSignaller(),
	}
	go b.loop()
	return &b
}

//------------------------------------------------------------------------------

func (m *Impl) loop() {
	closeNowCtx, cnDone := m.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		m.child.TriggerCloseNow()
		_ = m.child.WaitForClose(context.Background())

		_ = m.batcher.Close(context.Background())

		close(m.messagesOut)
		m.shutSig.ShutdownComplete()
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext > 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	pendingTrans := []*transaction.Tracked{}
	pendingAcks := sync.WaitGroup{}

	flushBatchFn := func() {
		sendMsg := m.batcher.Flush(closeNowCtx)
		if sendMsg == nil {
			return
		}

		resChan := make(chan error)
		select {
		case m.messagesOut <- message.NewTransaction(sendMsg, resChan):
		case <-m.shutSig.CloseNowChan():
			return
		}

		pendingAcks.Add(1)
		go func(rChan <-chan error, aggregatedTransactions []*transaction.Tracked) {
			defer pendingAcks.Done()

			select {
			case <-m.shutSig.CloseNowChan():
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, c := range aggregatedTransactions {
					if err := c.Ack(closeNowCtx, res); err != nil {
						return
					}
				}
			}
		}(resChan, pendingTrans)
		pendingTrans = nil
	}

	defer func() {
		// Final flush of remaining documents.
		m.log.Debug("Flushing remaining messages of batch.")
		flushBatchFn()

		// Wait for all pending acks to resolve.
		m.log.Debug("Waiting for pending acks to resolve before shutting down.")
		pendingAcks.Wait()
		m.log.Debug("Pending acks resolved.")
	}()

	for {
		if nextTimedBatchChan == nil {
			if tNext := m.batcher.UntilNext(); tNext > 0 {
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
					case <-m.shutSig.CloseAtLeisureChan():
					}
				}
				flushBatchFn()
				return
			}

			trackedTran := transaction.NewTracked(tran.Payload, tran.Ack)
			_ = trackedTran.Message().Iter(func(i int, p *message.Part) error {
				if m.batcher.Add(p) {
					flushBatch = true
				}
				return nil
			})
			pendingTrans = append(pendingTrans, trackedTran)
		case <-nextTimedBatchChan:
			flushBatch = true
			nextTimedBatchChan = nil
		case <-m.shutSig.CloseNowChan():
			return
		}

		if flushBatch {
			flushBatchFn()
		}
	}
}

// Connected returns true if the underlying input is connected.
func (m *Impl) Connected() bool {
	return m.child.Connected()
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *Impl) TransactionChan() <-chan message.Transaction {
	return m.messagesOut
}

// TriggerStopConsuming instructs the input to start shutting down resources
// once all pending messages are delivered and acknowledged. This call does
// not block.
func (m *Impl) TriggerStopConsuming() {
	m.shutSig.CloseAtLeisure()
	m.child.TriggerStopConsuming()
}

// TriggerCloseNow triggers the shut down of this component but should not block
// the calling goroutine.
func (m *Impl) TriggerCloseNow() {
	m.shutSig.CloseNow()
}

// WaitForClose is a blocking call to wait until the component has finished
// shutting down and cleaning up resources.
func (m *Impl) WaitForClose(ctx context.Context) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
