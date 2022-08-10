package pipeline

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

// Pool is a pool of pipelines. Each pipeline reads from a shared transaction
// channel. Inputs remain coupled to their outputs as they propagate the
// response channel in the transaction.
type Pool struct {
	workers []processor.Pipeline

	log log.Modular

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewPool creates a new processing pool.
func NewPool(threads int, log log.Modular, msgProcessors ...processor.V1) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	p := &Pool{
		workers:     make([]processor.Pipeline, threads),
		log:         log,
		messagesOut: make(chan message.Transaction),
		shutSig:     shutdown.NewSignaller(),
	}

	for i := range p.workers {
		p.workers[i] = NewProcessor(msgProcessors...)
	}

	return p, nil
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Pool) loop() {
	// Note this is currently kept open as we only have our children as a
	// shutdown mechanism. This puts trust in individual processor pipelines, if
	// that's not realistic we can consider adding a close now to the
	// TriggerCloseNow method.
	closeNowCtx, cnDone := p.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		for _, c := range p.workers {
			if err := c.WaitForClose(closeNowCtx); err != nil {
				break
			}
		}

		close(p.messagesOut)
		p.shutSig.ShutdownComplete()
	}()

	internalMessages := make(chan message.Transaction)
	remainingWorkers := int64(len(p.workers))

	var closeInternalOnce sync.Once

	for _, worker := range p.workers {
		if err := worker.Consume(p.messagesIn); err != nil {
			p.log.Errorf("Failed to start pipeline worker: %v\n", err)
			atomic.AddInt64(&remainingWorkers, -1)
			continue
		}
		go func(w processor.Pipeline) {
			defer func() {
				if v := atomic.AddInt64(&remainingWorkers, -1); v <= 0 {
					closeInternalOnce.Do(func() {
						close(internalMessages)
					})
				}
			}()
			for {
				var t message.Transaction
				var open bool
				select {
				case t, open = <-w.TransactionChan():
					if !open {
						return
					}
				case <-p.shutSig.CloseNowChan():
					return
				}
				select {
				case internalMessages <- t:
				case <-p.shutSig.CloseNowChan():
					return
				}
			}
		}(worker)
	}

	for atomic.LoadInt64(&remainingWorkers) > 0 {
		select {
		case t, open := <-internalMessages:
			if !open {
				return
			}
			select {
			case p.messagesOut <- t:
			case <-p.shutSig.CloseNowChan():
				return
			}
		case <-p.shutSig.CloseNowChan():
			return
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Pool) Consume(msgs <-chan message.Transaction) error {
	if p.messagesIn != nil {
		return component.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Pool) TransactionChan() <-chan message.Transaction {
	return p.messagesOut
}

// TriggerCloseNow signals that the component should close immediately,
// messages in flight will be dropped.
func (p *Pool) TriggerCloseNow() {
	for _, w := range p.workers {
		w.TriggerCloseNow()
	}
}

// WaitForClose blocks until the component has closed down or the context is
// cancelled. Closing occurs either when the input transaction channel is
// closed and messages are flushed (and acked), or when CloseNowAsync is
// called.
func (p *Pool) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
