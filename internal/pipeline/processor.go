package pipeline

import (
	"context"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
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

		sorter, sortBatch := message.NewSortGroup(tran.Payload)

		resultBatches, err := processor.ExecuteAll(closeNowCtx, p.msgProcessors, sortBatch)
		if len(resultBatches) == 0 || err != nil {
			if _ = tran.Ack(closeNowCtx, err); closeNowCtx.Err() != nil {
				return
			}
			continue
		}

		if len(resultBatches) == 1 {
			select {
			case p.messagesOut <- message.NewTransactionFunc(resultBatches[0], tran.Ack):
			case <-p.shutSig.CloseNowChan():
				return
			}
			continue
		}

		var (
			errMut     sync.Mutex
			batchErr   *batch.Error
			generalErr error
			batchWG    sync.WaitGroup
		)

		for _, b := range resultBatches {
			var wgOnce sync.Once
			batchWG.Add(1)
			tmpBatch := b.ShallowCopy()

			select {
			case p.messagesOut <- message.NewTransactionFunc(tmpBatch, func(ctx context.Context, err error) error {
				if err != nil {
					errMut.Lock()
					defer errMut.Unlock()

					if batchErr == nil {
						batchErr = batch.NewError(sortBatch, err)
					}
					for _, m := range tmpBatch {
						if bIndex := sorter.GetIndex(m); bIndex >= 0 {
							batchErr.Failed(bIndex, err)
						} else {
							// We are unable to link this message with an origin
							// and therefore we must provide a general
							// batch-wide error instead.
							generalErr = err
						}
					}
				}

				wgOnce.Do(func() {
					batchWG.Done()
				})
				return nil
			}):
			case <-p.shutSig.CloseNowChan():
				return
			}
		}

		batchWG.Wait()

		if generalErr != nil {
			_ = tran.Ack(closeNowCtx, generalErr)
		} else if batchErr != nil {
			_ = tran.Ack(closeNowCtx, batchErr)
		} else {
			_ = tran.Ack(closeNowCtx, nil)
		}
	}
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
