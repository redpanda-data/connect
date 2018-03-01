package pipeline

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// Pool is a pool of pipelines. It reads from a single source and writes to a
// single source. The input is decoupled which means failed delivery
// notification cannot be propagated back up to the original input.
//
// If delivery acknowledgements to the input is required this pool should not be
// used. Instead, you should configure multiple inputs each with their own
// pipeline e.g. configure 8 kafka_balanced inputs each with a single processor
// rather than a single kafka_balanced with a pool of 8 workers.
type Pool struct {
	running uint32

	workers          []Type
	remainingWorkers int32

	constructor ConstructorFunc

	log   log.Modular
	stats metrics.Type

	workChan chan types.Message

	messagesOut chan types.Transaction
	messagesIn  <-chan types.Transaction

	closeChan chan struct{}
	closed    chan struct{}
}

// NewPool returns a new pipeline pool that utilized multiple processor threads.
func NewPool(
	constructor ConstructorFunc,
	workers int,
	log log.Modular,
	stats metrics.Type,
) (*Pool, error) {
	p := &Pool{
		running:          1,
		workers:          make([]Type, workers),
		remainingWorkers: 0,
		constructor:      constructor,
		log:              log,
		stats:            stats,
		workChan:         make(chan types.Message),
		messagesOut:      make(chan types.Transaction),
		closeChan:        make(chan struct{}),
		closed:           make(chan struct{}),
	}

	for i := range p.workers {
		var err error
		if p.workers[i], err = p.constructor(); err != nil {
			return nil, err
		}
	}

	return p, nil
}

//------------------------------------------------------------------------------

// workerLoop is the processing loop of a pool worker.
func (p *Pool) workerLoop(worker Type, wg *sync.WaitGroup) {
	sendChan := make(chan types.Transaction)
	resChan := make(chan types.Response)

	defer func() {
		close(sendChan)
		atomic.AddInt32(&p.remainingWorkers, -1)
		wg.Done()
	}()

	if err := worker.StartReceiving(sendChan); err != nil {
		p.log.Errorf("Failed to start pool worker: %v\n", err)
		return
	}

	var msgOut types.Message
	for {
		var open bool

		if len(msgOut.Parts) == 0 {
			var msgIn types.Message

			// Read new work from pool.
			if msgIn, open = <-p.workChan; !open {
				return
			}
			p.stats.Incr("pipeline.pool.worker.message.received", 1)

			// Send work to processing pipeline.
			sendChan <- types.NewTransaction(msgIn, resChan)
			p.stats.Incr("pipeline.pool.worker.message.sent", 1)

			var tOut types.Transaction

			// Receive result from processing pipeline or response.
			select {
			case tOut, open = <-worker.TransactionChan():
				if !open {
					return
				}
				p.stats.Incr("pipeline.pool.worker.result.received", 1)

				// Send decoupled response to processing pipeline
				tOut.ResponseChan <- types.NewSimpleResponse(nil)
				if _, open = <-resChan; !open {
					return
				}

				msgOut = tOut.Payload
			case _, open = <-resChan:
				if !open {
					return
				}
				// Message was dropped, move onto next.
			}
		}

		if len(msgOut.Parts) > 0 {
			// Send result to shared output channel.
			p.messagesOut <- types.NewTransaction(msgOut, resChan)
			p.stats.Incr("pipeline.pool.worker.result.sent", 1)

			// Receive output response from shared response channel.
			res := <-resChan
			if err := res.Error(); err != nil {
				p.log.Errorf("Failed to send message: %v\n", err)
			} else {
				msgOut = types.Message{}
				p.stats.Incr("pipeline.pool.worker.response.received", 1)
			}
			p.stats.Incr("pipeline.worker.response.sent", 1)
		}
	}
}

// loop is the processing loop of this pipeline.
func (p *Pool) loop() {
	workerGroup := sync.WaitGroup{}

	defer func() {
		atomic.StoreUint32(&p.running, 0)

		// Signal all workers to close.
		close(p.workChan)
		for _, worker := range p.workers {
			worker.CloseAsync()
		}

		// Wait for all workers to be closed before closing our response and
		// messages channels as the workers may still have access to them.
		for _, worker := range p.workers {
			err := worker.WaitForClose(time.Second)
			for err != nil {
				err = worker.WaitForClose(time.Second)
			}
		}

		workerGroup.Wait()

		close(p.messagesOut)
		close(p.closed)
	}()

	workerGroup.Add(len(p.workers))
	for _, worker := range p.workers {
		atomic.AddInt32(&p.remainingWorkers, 1)
		go p.workerLoop(worker, &workerGroup)
	}

	var open bool
	for atomic.LoadUint32(&p.running) == 1 && atomic.LoadInt32(&p.remainingWorkers) > 0 {
		var t types.Transaction
		select {
		case t, open = <-p.messagesIn:
			if !open {
				return
			}
		case <-p.closeChan:
			return
		}
		p.stats.Incr("pipeline.pool.message.received", 1)

		select {
		case p.workChan <- t.Payload:
		case <-p.closeChan:
			return
		}

		select {
		case t.ResponseChan <- types.NewSimpleResponse(nil):
		case <-p.closeChan:
			return
		}
	}
}

//------------------------------------------------------------------------------

// StartReceiving assigns a messages channel for the pipeline to read.
func (p *Pool) StartReceiving(msgs <-chan types.Transaction) error {
	if p.messagesIn != nil {
		return types.ErrAlreadyStarted
	}
	p.messagesIn = msgs
	go p.loop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// pipeline.
func (p *Pool) TransactionChan() <-chan types.Transaction {
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
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
