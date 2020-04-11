package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Try is a broker that implements types.Consumer and attempts to send each
// message to a single output, but on failure will attempt the next output in
// the list.
type Try struct {
	stats         metrics.Type
	outputsPrefix string

	transactions <-chan types.Transaction

	outputTsChans []chan types.Transaction
	outputs       []types.Output

	ctx        context.Context
	close      func()
	closedChan chan struct{}
}

// NewTry creates a new Try type by providing consumers.
func NewTry(outputs []types.Output, stats metrics.Type) (*Try, error) {
	ctx, done := context.WithCancel(context.Background())
	t := &Try{
		stats:         stats,
		outputsPrefix: "broker.outputs",
		transactions:  nil,
		outputs:       outputs,
		closedChan:    make(chan struct{}),
		ctx:           ctx,
		close:         done,
	}
	if len(outputs) == 0 {
		return nil, errors.New("missing outputs")
	}
	t.outputTsChans = make([]chan types.Transaction, len(t.outputs))
	for i := range t.outputTsChans {
		t.outputTsChans[i] = make(chan types.Transaction)
		if err := t.outputs[i].Consume(t.outputTsChans[i]); err != nil {
			return nil, err
		}
	}
	return t, nil
}

//------------------------------------------------------------------------------

// WithOutputMetricsPrefix changes the prefix used for counter metrics showing
// errors of an output.
func (t *Try) WithOutputMetricsPrefix(prefix string) {
	t.outputsPrefix = prefix
}

// Consume assigns a new messages channel for the broker to read.
func (t *Try) Consume(ts <-chan types.Transaction) error {
	if t.transactions != nil {
		return types.ErrAlreadyStarted
	}
	t.transactions = ts

	go t.loop()
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (t *Try) Connected() bool {
	for _, out := range t.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (t *Try) loop() {
	var (
		wg        = sync.WaitGroup{}
		mMsgsRcvd = t.stats.GetCounter("count")
		mErrs     = []metrics.StatCounter{}
	)

	defer func() {
		wg.Wait()
		for _, c := range t.outputTsChans {
			close(c)
		}
		close(t.closedChan)
	}()

	for i := range t.outputs {
		mErrs = append(mErrs, t.stats.GetCounter(fmt.Sprintf("%v.%v.failed", t.outputsPrefix, i)))
	}

	sendLoop := func() {
		defer wg.Done()
		for {
			var open bool
			var tran types.Transaction

			select {
			case tran, open = <-t.transactions:
				if !open {
					return
				}
			case <-t.ctx.Done():
				return
			}
			mMsgsRcvd.Incr(1)

			rChan := make(chan types.Response)
			select {
			case t.outputTsChans[0] <- types.NewTransaction(tran.Payload, rChan):
			case <-t.ctx.Done():
				return
			}

			var res types.Response
			var lOpen bool

		triesLoop:
			for i := 1; i <= len(t.outputTsChans); i++ {
				select {
				case res, lOpen = <-rChan:
					if !lOpen {
						return
					}
					if res.Error() != nil {
						mErrs[i-1].Incr(1)
					} else {
						break triesLoop
					}
				case <-t.ctx.Done():
					return
				}

				if i < len(t.outputTsChans) {
					select {
					case t.outputTsChans[i] <- types.NewTransaction(tran.Payload, rChan):
					case <-t.ctx.Done():
						return
					}
				}
			}
			select {
			case tran.ResponseChan <- res:
			case <-t.ctx.Done():
				return
			}
		}
	}

	// Max in flight
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// CloseAsync shuts down the Try broker and stops processing requests.
func (t *Try) CloseAsync() {
	t.close()
}

// WaitForClose blocks until the Try broker has closed down.
func (t *Try) WaitForClose(timeout time.Duration) error {
	select {
	case <-t.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
