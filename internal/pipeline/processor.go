package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

//------------------------------------------------------------------------------

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	msgProcessors []iprocessor.V1

	messagesOut chan message.Transaction
	responsesIn chan error

	messagesIn <-chan message.Transaction

	shutSig *shutdown.Signaller
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(msgProcessors ...iprocessor.V1) *Processor {
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
	defer func() {
		// Signal all children to close.
		for _, c := range p.msgProcessors {
			c.CloseAsync()
		}

		close(p.messagesOut)
		p.shutSig.ShutdownComplete()
	}()

	closeCtx, done := p.shutSig.CloseAtLeisureCtx(context.Background())
	defer done()

	var open bool
	for !p.shutSig.ShouldCloseAtLeisure() {
		var tran message.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.shutSig.CloseAtLeisureChan():
			return
		}

		resultMsgs, resultRes := processor.ExecuteAll(p.msgProcessors, tran.Payload)
		if len(resultMsgs) == 0 {
			if err := tran.Ack(closeCtx, resultRes); err != nil && closeCtx.Err() != nil {
				return
			}
			continue
		}

		if len(resultMsgs) > 1 {
			p.dispatchMessages(closeCtx, resultMsgs, tran.Ack)
		} else {
			select {
			case p.messagesOut <- message.NewTransactionFunc(resultMsgs[0], tran.Ack):
			case <-p.shutSig.CloseAtLeisureChan():
				return
			}
		}
	}
}

// dispatchMessages attempts to send a multiple messages results of processors
// over the shared messages channel. This send is retried until success.
func (p *Processor) dispatchMessages(ctx context.Context, msgs []*message.Batch, ackFn func(context.Context, error) error) {
	throt := throttle.New(throttle.OptCloseChan(p.shutSig.CloseAtLeisureChan()))

	pending := msgs
	for len(pending) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(pending))

		var newPending []*message.Batch
		var newPendingMut sync.Mutex

		for _, b := range pending {
			b := b
			transac := message.NewTransactionFunc(b, func(ctx context.Context, err error) error {
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

	// TODO: V4 Exit before ack if closing
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

// CloseAsync shuts down the pipeline and stops processing messages.
func (p *Processor) CloseAsync() {
	p.shutSig.CloseAtLeisure()

	// Signal all children to close.
	for _, c := range p.msgProcessors {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (p *Processor) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	select {
	case <-p.shutSig.HasClosedChan():
	case <-time.After(time.Until(stopBy)):
		return component.ErrTimeout
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
