package pipeline

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
)

// Pool is a pool of pipelines. Each pipeline reads from a shared transaction
// channel. Inputs remain coupled to their outputs as they propagate the
// response channel in the transaction.
type Pool struct {
	running uint32

	workers []iprocessor.Pipeline

	log log.Modular

	messagesIn  <-chan message.Transaction
	messagesOut chan message.Transaction

	closeChan chan struct{}
	closed    chan struct{}
}

func newPoolV2(threads int, log log.Modular, msgProcessors ...iprocessor.V1) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	p := &Pool{
		running:     1,
		workers:     make([]iprocessor.Pipeline, threads),
		log:         log,
		messagesOut: make(chan message.Transaction),
		closeChan:   make(chan struct{}),
		closed:      make(chan struct{}),
	}

	for i := range p.workers {
		p.workers[i] = NewProcessor(msgProcessors...)
	}

	return p, nil
}

//------------------------------------------------------------------------------

// loop is the processing loop of this pipeline.
func (p *Pool) loop() {
	defer func() {
		atomic.StoreUint32(&p.running, 0)

		// Signal all workers to close.
		for _, worker := range p.workers {
			worker.CloseAsync()
		}

		// Wait for all workers to be closed before closing our response and
		// messages channels as the workers may still have access to them.
		for _, worker := range p.workers {
			_ = worker.WaitForClose(shutdown.MaximumShutdownWait())
		}

		close(p.messagesOut)
		close(p.closed)
	}()

	internalMessages := make(chan message.Transaction)
	remainingWorkers := int64(len(p.workers))

	for _, worker := range p.workers {
		if err := worker.Consume(p.messagesIn); err != nil {
			p.log.Errorf("Failed to start pipeline worker: %v\n", err)
			atomic.AddInt64(&remainingWorkers, -1)
			continue
		}
		go func(w iprocessor.Pipeline) {
			defer func() {
				if atomic.AddInt64(&remainingWorkers, -1) == 0 {
					close(internalMessages)
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
				case <-p.closeChan:
					return
				}
				select {
				case internalMessages <- t:
				case <-p.closeChan:
					return
				}
			}
		}(worker)
	}

	for atomic.LoadUint32(&p.running) == 1 && atomic.LoadInt64(&remainingWorkers) > 0 {
		select {
		case t, open := <-internalMessages:
			if !open {
				return
			}
			select {
			case p.messagesOut <- t:
			case <-p.closeChan:
				return
			}
		case <-p.closeChan:
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

// CloseAsync shuts down the pipeline and stops processing messages.
func (p *Pool) CloseAsync() {
	if atomic.CompareAndSwapUint32(&p.running, 1, 0) {
		close(p.closeChan)
	}
}

// WaitForClose - Blocks until the StackBuffer output has closed down.
func (p *Pool) WaitForClose(timeout time.Duration) error {
	select {
	case <-p.closed:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
