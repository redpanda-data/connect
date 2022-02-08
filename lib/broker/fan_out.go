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
	"golang.org/x/sync/errgroup"
)

//------------------------------------------------------------------------------

// FanOut is a broker that implements types.Consumer and broadcasts each message
// out to an array of outputs.
type FanOut struct {
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

// NewFanOut creates a new FanOut type by providing outputs.
func NewFanOut(
	outputs []types.Output, logger log.Modular, stats metrics.Type,
) (*FanOut, error) {
	ctx, done := context.WithCancel(context.Background())
	o := &FanOut{
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
func (o *FanOut) WithMaxInFlight(i int) *FanOut {
	if i < 1 {
		i = 1
	}
	o.maxInFlight = i
	return o
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (o *FanOut) Consume(transactions <-chan types.Transaction) error {
	if o.transactions != nil {
		return component.ErrAlreadyStarted
	}
	o.transactions = transactions

	go o.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (o *FanOut) Connected() bool {
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
func (o *FanOut) MaxInFlight() (int, bool) {
	return o.maxInFlight, true
}

//------------------------------------------------------------------------------

func closeAllOutputs(outputs []types.Output) {
	for _, o := range outputs {
		o.CloseAsync()
	}
	for _, o := range outputs {
		for {
			if err := o.WaitForClose(time.Second); err == nil {
				break
			}
		}
	}
}

// loop is an internal loop that brokers incoming messages to many outputs.
func (o *FanOut) loop() {
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

			var owg errgroup.Group
			for target := range o.outputTSChans {
				msgCopy, i := ts.Payload.Copy(), target
				owg.Go(func() error {
					throt := throttle.New(throttle.OptCloseChan(o.ctx.Done()))
					resChan := make(chan types.Response)

					// Try until success or shutdown.
					for {
						select {
						case o.outputTSChans[i] <- types.NewTransaction(msgCopy, resChan):
						case <-o.ctx.Done():
							return component.ErrTypeClosed
						}
						select {
						case res := <-resChan:
							if res.AckError() != nil {
								o.logger.Errorf("Failed to dispatch fan out message to output '%v': %v\n", i, res.AckError())
								mOutputErr.Incr(1)
								if !throt.Retry() {
									return component.ErrTypeClosed
								}
							} else {
								mMsgsSnt.Incr(1)
								return nil
							}
						case <-o.ctx.Done():
							return component.ErrTypeClosed
						}
					}
				})
			}

			if owg.Wait() == nil {
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-o.ctx.Done():
					return
				}
			}
		}
	}

	// Max in flight
	for i := 0; i < o.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// CloseAsync shuts down the FanOut broker and stops processing requests.
func (o *FanOut) CloseAsync() {
	o.close()
}

// WaitForClose blocks until the FanOut broker has closed down.
func (o *FanOut) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
