package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Processor is a pipeline that supports both Consumer and Producer interfaces.
// The processor will read from a source, perform some processing, and then
// either propagate a new message or drop it.
type Processor struct {
	running int32
	stats   metrics.Type

	msgProcessors []types.Processor

	messagesOut chan types.Transaction
	responsesIn chan types.Response

	messagesIn <-chan types.Transaction

	closeChan chan struct{}
	closed    chan struct{}
}

// NewProcessor returns a new message processing pipeline.
func NewProcessor(
	log log.Modular,
	stats metrics.Type,
	msgProcessors ...types.Processor,
) *Processor {
	return &Processor{
		running:       1,
		msgProcessors: msgProcessors,
		stats:         stats,
		messagesOut:   make(chan types.Transaction),
		responsesIn:   make(chan types.Response),
		closeChan:     make(chan struct{}),
		closed:        make(chan struct{}),
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
		close(p.closed)
	}()

	var open bool
	for atomic.LoadInt32(&p.running) == 1 {
		var tran types.Transaction
		select {
		case tran, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.closeChan:
			return
		}

		resultMsgs, resultRes := processor.ExecuteAll(p.msgProcessors, tran.Payload)
		if len(resultMsgs) == 0 {
			if resultRes == nil {
				resultRes = response.NewAck()
			}
			select {
			case tran.ResponseChan <- resultRes:
			case <-p.closeChan:
				return
			}
			continue
		}

		if len(resultMsgs) > 1 {
			p.dispatchMessages(resultMsgs, tran.ResponseChan)
		} else {
			select {
			case p.messagesOut <- types.NewTransaction(resultMsgs[0], tran.ResponseChan):
			case <-p.closeChan:
				return
			}
		}
	}
}

// dispatchMessages attempts to send a multiple messages results of processors
// over the shared messages channel. This send is retried until success.
func (p *Processor) dispatchMessages(msgs []types.Message, ogResChan chan<- types.Response) {
	throt := throttle.New(throttle.OptCloseChan(p.closeChan))

	sendMsg := func(m types.Message) {
		resChan := make(chan types.Response)
		transac := types.NewTransaction(m, resChan)

		for {
			select {
			case p.messagesOut <- transac:
			case <-p.closeChan:
				return
			}

			var open bool
			var res types.Response
			select {
			case res, open = <-resChan:
				if !open {
					return
				}
			case <-p.closeChan:
				return
			}

			if res.Error() == nil {
				return
			}
			if !throt.Retry() {
				return
			}
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	for _, msg := range msgs {
		go func(m types.Message) {
			sendMsg(m)
			wg.Done()
		}(msg)
	}

	wg.Wait()

	// TODO: V4 Exit before ack if closing
	throt.Reset()

	select {
	case ogResChan <- response.NewAck():
	case <-p.closeChan:
		return
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the pipeline to read.
func (p *Processor) Consume(msgs <-chan types.Transaction) error {
	if p.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Processor) TransactionChan() <-chan types.Transaction {
	return p.messagesOut
}

// CloseAsync shuts down the pipeline and stops processing messages.
func (p *Processor) CloseAsync() {
	if atomic.CompareAndSwapInt32(&p.running, 1, 0) {
		close(p.closeChan)

		// Signal all children to close.
		for _, c := range p.msgProcessors {
			c.CloseAsync()
		}
	}
}

// WaitForClose blocks until the StackBuffer output has closed down.
func (p *Processor) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	select {
	case <-p.closed:
	case <-time.After(time.Until(stopBy)):
		return types.ErrTimeout
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
