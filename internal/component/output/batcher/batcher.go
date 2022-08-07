package batcher

import (
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

// Impl wraps an output with a batching policy.
type Impl struct {
	stats metrics.Type
	log   log.Modular

	child   output.Streamed
	batcher *policy.Batcher

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewFromConfig creates a new output preceded by a batching mechanism that
// enforces a given batching policy configuration.
func NewFromConfig(conf batchconfig.Config, child output.Streamed, mgr bundle.NewManagement) (output.Streamed, error) {
	if !conf.IsNoop() {
		policy, err := policy.New(conf, mgr.IntoPath("batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		child = New(policy, child, mgr)
	}
	return child, nil
}

// New creates a new output preceded by a batching mechanism that enforces a
// given batching policy.
func New(batcher *policy.Batcher, child output.Streamed, mgr bundle.NewManagement) output.Streamed {
	m := Impl{
		stats:       mgr.Metrics(),
		log:         mgr.Logger(),
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan message.Transaction),
		shutSig:     shutdown.NewSignaller(),
	}
	return &m
}

//------------------------------------------------------------------------------

func (m *Impl) loop() {
	closeNowCtx, cnDone := m.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		close(m.messagesOut)

		m.child.TriggerCloseNow()
		_ = m.child.WaitForClose(context.Background())

		_ = m.batcher.Close(context.Background())

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
				trackedTran := transaction.NewTracked(tran.Payload, tran.Ack)
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

		sendMsg := m.batcher.Flush(closeNowCtx)
		if sendMsg == nil {
			continue
		}

		resChan := make(chan error)
		select {
		case m.messagesOut <- message.NewTransaction(sendMsg, resChan):
		case <-m.shutSig.CloseAtLeisureChan():
			return
		}

		go func(rChan chan error, upstreamTrans []*transaction.Tracked) {
			select {
			case <-m.shutSig.CloseAtLeisureChan():
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				closeLeisureCtx, done := m.shutSig.CloseAtLeisureCtx(context.Background())
				for _, t := range upstreamTrans {
					if err := t.Ack(closeLeisureCtx, res); err != nil {
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
func (m *Impl) Connected() bool {
	return m.child.Connected()
}

// Consume assigns a messages channel for the output to read.
func (m *Impl) Consume(msgs <-chan message.Transaction) error {
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

// TriggerCloseNow shuts down the Batcher and stops processing messages.
func (m *Impl) TriggerCloseNow() {
	m.shutSig.CloseNow()
}

// WaitForClose blocks until the Batcher output has closed down.
func (m *Impl) WaitForClose(ctx context.Context) error {
	select {
	case <-m.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
