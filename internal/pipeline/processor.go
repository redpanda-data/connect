package pipeline

import (
	"context"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	msgProcessors []processor.V1

	messagesOut chan message.Transaction
	responsesIn chan error

	messagesIn <-chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(msgProcessors ...processor.V1) *Processor {
	return &Processor{
		msgProcessors: msgProcessors,
		messagesOut:   make(chan message.Transaction),
		responsesIn:   make(chan error),
		shutSig:       shutdown.NewSignaller(),
	}
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Processor) loop() {
	closeNowCtx, cnDone := p.shutSig.CloseNowCtx(context.Background())
	defer cnDone()

	defer func() {
		// Signal all children to close.
		for _, c := range p.msgProcessors {
			if err := c.Close(closeNowCtx); err != nil {
				break
			}
		}

		close(p.messagesOut)
		p.shutSig.ShutdownComplete()
	}()

	var open bool
	for !p.shutSig.ShouldCloseAtLeisure() {
		var tran message.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.shutSig.CloseNowChan():
			return
		}

		resultMsgs, resultRes := processor.ExecuteAll(closeNowCtx, p.msgProcessors, tran.Payload)
		if len(resultMsgs) == 0 {
			if err := tran.Ack(closeNowCtx, resultRes); err != nil && closeNowCtx.Err() != nil {
				return
			}
			continue
		}

		if len(resultMsgs) > 1 {
			p.dispatchMessages(closeNowCtx, resultMsgs, tran.Ack)
		} else {
			select {
			case p.messagesOut <- message.NewTransactionFunc(resultMsgs[0], tran.Ack):
			case <-p.shutSig.CloseNowChan():
				return
			}
		}
	}
}

// dispatchMessages attempts to send a multiple messages results of processors
// over the shared messages channel. This send is retried until success.
func (p *Processor) dispatchMessages(ctx context.Context, msgs []message.Batch, ackFn func(context.Context, error) error) {
	throt := throttle.New(throttle.OptCloseChan(p.shutSig.CloseAtLeisureChan()))

	pending := msgs
	for len(pending) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(pending))

		var newPending []message.Batch
		var newPendingMut sync.Mutex

		for _, b := range pending {
			b := b
			transac := message.NewTransactionFunc(b.ShallowCopy(), func(ctx context.Context, err error) error {
				if err != nil {
					newPendingMut.Lock()
					newPending = append(newPending, b)
					newPendingMut.Unlock()
				}
				wg.Done()
				return nil
			})

			select {
			case p.messagesOut <- transac:
			case <-ctx.Done():
				return
			}
		}
		wg.Wait()

		if pending = newPending; len(pending) > 0 && !throt.Retry() {
			return
		}
	}

	throt.Reset()
	_ = ackFn(ctx, nil)
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Processor) Consume(msgs <-chan message.Transaction) error {
	if p.messagesIn != nil {
		return component.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Processor) TransactionChan() <-chan message.Transaction {
	return p.messagesOut
}

// TriggerCloseNow signals that the processor pipeline should close immediately.
func (p *Processor) TriggerCloseNow() {
	p.shutSig.CloseNow()
}

// WaitForClose blocks until the component has closed down or the context is
// cancelled. Closing occurs either when the input transaction channel is closed
// and messages are flushed (and acked), or when CloseNowAsync is called.
func (p *Processor) WaitForClose(ctx context.Context) error {
	select {
	case <-p.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
