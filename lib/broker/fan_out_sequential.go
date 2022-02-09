package broker

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/output"
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

	maxInFlight  int
	transactions <-chan types.Transaction

	outputTSChans []chan types.Transaction
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
		maxInFlight:  1,
		stats:        stats,
		logger:       logger,
		transactions: nil,
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		ctx:          ctx,
		close:        done,
	}

	o.outputTSChans = make([]chan types.Transaction, len(o.outputs))
	for i := range o.outputTSChans {
		o.outputTSChans[i] = make(chan types.Transaction)
		if err := o.outputs[i].Consume(o.outputTSChans[i]); err != nil {
			return nil, err
		}
		if mif, ok := output.GetMaxInFlight(o.outputs[i]); ok && mif > o.maxInFlight {
			o.maxInFlight = mif
		}
	}
	return o, nil
}

// WithMaxInFlight sets the maximum number of in-flight messages this broker
// supports. This must be set before calling Consume.
func (o *FanOutSequential) WithMaxInFlight(i int) *FanOutSequential {
	if i < 1 {
		i = 1
	}
	o.maxInFlight = i
	return o
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *FanOutSequential) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return component.ErrAlreadyStarted
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

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (o *FanOutSequential) MaxInFlight() (int, bool) {
	return o.maxInFlight, true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *FanOutSequential) loop() {
	var (
		wg         = sync.WaitGroup{}
		mMsgsRcvd  = o.stats.GetCounter("messages.received")
		mOutputErr = o.stats.GetCounter("error")
		mMsgsSnt   = o.stats.GetCounter("messages.sent")
	)

	defer func() {
		wg.Wait()
		for _, c := range o.outputTSChans {
			close(c)
		}
		closeAllOutputs(o.outputs)
		close(o.closedChan)
	}()

	sendLoop := func() {
		defer wg.Done()
		for {
			var ts types.Transaction
			var open bool

			select {
			case ts, open = <-o.transactions:
				if !open {
					return
				}
			case <-o.ctx.Done():
				return
			}
			mMsgsRcvd.Incr(1)

			for i := range o.outputTSChans {
				msgCopy := ts.Payload.Copy()

				throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
				resChan := make(chan response.Error)

				// Try until success or shutdown.
			sendLoop:
				for {
					select {
					case o.outputTSChans[i] <- types.NewTransaction(msgCopy, resChan):
					case <-o.ctx.Done():
						return
					}
					select {
					case res := <-resChan:
						if res.AckError() != nil {
							o.logger.Errorf("Failed to dispatch fan out message to output '%v': %v\n", i, res.AckError())
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
			case ts.ResponseChan <- response.NewError(nil):
			case <-o.ctx.Done():
				return
			}
		}
	}

	// Max in flight
	for i := 0; i < o.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
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
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
