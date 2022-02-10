package output

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/transaction"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Batcher wraps an output with a batching policy.
type Batcher struct {
	stats metrics.Type
	log   log.Modular

	child   output.Streamed
	batcher *batch.Policy

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewBatcherFromConfig creates a new output preceded by a batching mechanism
// that enforces a given batching policy configuration.
func NewBatcherFromConfig(
	conf batch.PolicyConfig,
	child output.Streamed, mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (output.Streamed, error) {
	if !conf.IsNoop() {
		bMgr, bLog, bStats := interop.LabelChild("batching", mgr, log, stats)
		policy, err := batch.NewPolicy(conf, bMgr, bLog, bStats)
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		child = NewBatcher(policy, child, log, stats)
	}
	return child, nil
}

// NewBatcher creates a new output preceded by a batching mechanism that
// enforces a given batching policy.
func NewBatcher(
	batcher *batch.Policy,
	child output.Streamed, log log.Modular,
	stats metrics.Type,
) output.Streamed {
	m := Batcher{
		stats:       stats,
		log:         log,
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan message.Transaction),
		shutSig:     shutdown.NewSignaller(),
	}
	return &m
}

//------------------------------------------------------------------------------

func (m *Batcher) loop() {
	defer func() {
		close(m.messagesOut)
		m.child.CloseAsync()
		_ = m.child.WaitForClose(shutdown.MaximumShutdownWait())

		m.batcher.CloseAsync()
		_ = m.batcher.WaitForClose(shutdown.MaximumShutdownWait())

		m.shutSig.ShutdownComplete()
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext >= 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	var pendingTrans []*transaction.Tracked
	for !m.shutSig.ShouldCloseAtLeisure() {
		if nextTimedBatchChan == nil {
			if tNext := m.batcher.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}

		var flushBatch bool
		select {
		case tran, open := <-m.messagesIn:
			if !open {
				if flushBatch = m.batcher.Count() > 0; !flushBatch {
					return
				}

				// If we're waiting for a timed batch then we will respect it.
				if nextTimedBatchChan != nil {
					select {
					case <-nextTimedBatchChan:
					case <-m.shutSig.CloseAtLeisureChan():
					}
				}
			} else {
				trackedTran := transaction.NewTracked(tran.Payload, tran.ResponseChan)
				_ = trackedTran.Message().Iter(func(i int, p *message.Part) error {
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
		case <-m.shutSig.CloseAtLeisureChan():
			flushBatch = true
		}

		if !flushBatch {
			continue
		}

		sendMsg := m.batcher.Flush()
		if sendMsg == nil {
			continue
		}

		resChan := make(chan response.Error)
		select {
		case m.messagesOut <- message.NewTransaction(sendMsg, resChan):
		case <-m.shutSig.CloseAtLeisureChan():
			return
		}

		go func(rChan chan response.Error, upstreamTrans []*transaction.Tracked) {
			select {
			case <-m.shutSig.CloseAtLeisureChan():
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				closeAtLeisureCtx, done := m.shutSig.CloseAtLeisureCtx(context.Background())
				for _, t := range upstreamTrans {
					if err := t.Ack(closeAtLeisureCtx, res.AckError()); err != nil {
						done()
						return
					}
				}
				done()
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

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (m *Batcher) MaxInFlight() (int, bool) {
	return output.GetMaxInFlight(m.child)
}

// Consume assigns a messages channel for the output to read.
func (m *Batcher) Consume(msgs <-chan message.Transaction) error {
	if m.messagesIn != nil {
		return component.ErrAlreadyStarted
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
	m.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the Batcher output has closed down.
func (m *Batcher) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
