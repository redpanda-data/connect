package broker

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

//------------------------------------------------------------------------------

// FanOutSequential is a broker that implements types.Consumer and broadcasts
// each message out to an array of outputs, but does so sequentially, only
// proceeding onto an output when the preceding output has successfully
// reported message receipt.
type FanOutSequential struct {
	logger log.Modular
	stats  metrics.Type

	transactions <-chan types.Transaction

	outputTsChans []chan types.Transaction
	outputs       []types.Output

	ctx        context.Context
	close      func()
	closedChan chan struct{}
}

// NewFanOutSequential creates a new FanOutSequential type by providing outputs.
func NewFanOutSequential(
	outputs []types.Output, logger log.Modular, stats metrics.Type,
) (*FanOutSequential, error) {
	ctx, done := context.WithCancel(context.Background())
	o := &FanOutSequential{
		stats:        stats,
		logger:       logger,
		transactions: nil,
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		ctx:          ctx,
		close:        done,
	}

	o.outputTsChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTsChans {
		o.outputTsChans[i] = make(chan types.Transaction)
		if err := o.outputs[i].Consume(o.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *FanOutSequential) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return types.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *FanOutSequential) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *FanOutSequential) loop() {
	wg := sync.WaitGroup{}

	defer func() {
		wg.Wait()
		for _, c := range o.outputTsChans {
			close(c)
		}
		close(o.closedChan)
	}()

	var (
		mMsgsRcvd  = o.stats.GetCounter("messages.received")
		mOutputErr = o.stats.GetCounter("error")
		mMsgsSnt   = o.stats.GetCounter("messages.sent")
	)

	// This gets locked if an outbound message is running in an error loop for
	// an output. It prevents consuming new messages until the error is
	// resolved.
	var errMut sync.RWMutex
	for {
		var ts types.Transaction
		var open bool

		errMut.Lock()
		errMut.Unlock()
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.ctx.Done():
			return
		}
		mMsgsRcvd.Incr(1)

		wg.Add(1)
		bp, received := context.WithCancel(context.Background())
		go func() {
			defer wg.Done()

			for i := range o.outputTsChans {
				msgCopy := ts.Payload.Copy()

				throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
				resChan := make(chan types.Response)
				failed := false

				// Try until success or shutdown.
			sendLoop:
				for {
					select {
					case o.outputTsChans[i] <- types.NewTransaction(msgCopy, resChan):
						received()
					case <-o.ctx.Done():
						return
					}
					select {
					case res := <-resChan:
						if res.Error() != nil {
							// Once an output returns an error we block
							// incoming messages until the problem is
							// resolved.
							//
							// We claim a read lock so that other outbound
							// messages are also able to retry.
							if !failed {
								errMut.RLock()
								defer errMut.RUnlock()
								failed = true
							}
							o.logger.Errorf("Failed to dispatch fan out message to output '%v': %v\n", i, res.Error())
							mOutputErr.Incr(1)
							if !throt.Retry() {
								return
							}
						} else {
							mMsgsSnt.Incr(1)
							break sendLoop
						}
					case <-o.ctx.Done():
						return
					}
				}
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-o.ctx.Done():
				return
			}
		}()

		// Block until one output has accepted our message or the service is
		// closing. This ensures we preserve back pressure when outputs are
		// saturated.
		select {
		case <-bp.Done():
		case <-o.ctx.Done():
			return
		}
	}
}

// CloseAsync shuts down the FanOutSequential broker and stops processing requests.
func (o *FanOutSequential) CloseAsync() {
	o.close()
}

// WaitForClose blocks until the FanOutSequential broker has closed down.
func (o *FanOutSequential) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
