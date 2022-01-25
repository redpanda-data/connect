package pipeline

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Pool is a pool of pipelines. Each pipeline reads from a shared transaction
// channel. Inputs remain coupled to their outputs as they propagate the
// response channel in the transaction.
type Pool struct {
	running uint32

	workers []types.Pipeline

	log   log.Modular
	stats metrics.Type

	messagesIn  <-chan types.Transaction
	messagesOut chan types.Transaction

	closeChan chan struct{}
	closed    chan struct{}
}

// TODO: V4 Remove this
func newTestPool(
	constructor types.PipelineConstructorFunc,
	threads int,
	log log.Modular,
	stats metrics.Type,
) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	p := &Pool{
		running:     1,
		workers:     make([]types.Pipeline, threads),
		log:         log,
		stats:       stats,
		messagesOut: make(chan types.Transaction),
		closeChan:   make(chan struct{}),
		closed:      make(chan struct{}),
	}

	for i := range p.workers {
		procs := 0
		var err error
		if p.workers[i], err = constructor(&procs); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func newPoolV2(
	threads int,
	log log.Modular,
	stats metrics.Type,
	msgProcessors ...types.Processor,
) (*Pool, error) {
	if threads <= 0 {
		threads = runtime.NumCPU()
	}

	p := &Pool{
		running:     1,
		workers:     make([]types.Pipeline, threads),
		log:         log,
		stats:       stats,
		messagesOut: make(chan types.Transaction),
		closeChan:   make(chan struct{}),
		closed:      make(chan struct{}),
	}

	for i := range p.workers {
		p.workers[i] = NewProcessor(log, stats, msgProcessors...)
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

	internalMessages := make(chan types.Transaction)
	remainingWorkers := int64(len(p.workers))

	for _, worker := range p.workers {
		if err := worker.Consume(p.messagesIn); err != nil {
			p.log.Errorf("Failed to start pipeline worker: %v\n", err)
			atomic.AddInt64(&remainingWorkers, -1)
			continue
		}
		go func(w types.Pipeline) {
			defer func() {
				if atomic.AddInt64(&remainingWorkers, -1) == 0 {
					close(internalMessages)
				}
			}()
			for {
				var t types.Transaction
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
func (p *Pool) Consume(msgs <-chan types.Transaction) error {
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
