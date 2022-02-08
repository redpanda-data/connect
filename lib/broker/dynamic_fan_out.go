package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"golang.org/x/sync/errgroup"
)

//------------------------------------------------------------------------------

// DynamicOutput is an interface of output types that must be closable.
type DynamicOutput interface {
	types.Output
}

// wrappedOutput is a struct that wraps a DynamicOutput with an identifying
// name.
type wrappedOutput struct {
	Ctx     context.Context
	Name    string
	Output  DynamicOutput
	ResChan chan<- error
}

// outputWithTSChan is a struct containing both an output and the transaction
// chan it reads from.
type outputWithTSChan struct {
	tsChan chan types.Transaction
	output DynamicOutput
	ctx    context.Context
	done   func()
}

//------------------------------------------------------------------------------

// DynamicFanOut is a broker that implements types.Consumer and broadcasts each
// message out to a dynamic map of outputs.
type DynamicFanOut struct {
	maxInFlight int

	log   log.Modular
	stats metrics.Type

	onAdd    func(label string)
	onRemove func(label string)

	transactions <-chan types.Transaction

	outputsMut    sync.RWMutex
	newOutputChan chan wrappedOutput
	outputs       map[string]outputWithTSChan

	ctx        context.Context
	close      func()
	closedChan chan struct{}
}

// NewDynamicFanOut creates a new DynamicFanOut type by providing outputs.
func NewDynamicFanOut(
	outputs map[string]DynamicOutput,
	logger log.Modular,
	stats metrics.Type,
	options ...func(*DynamicFanOut),
) (*DynamicFanOut, error) {
	ctx, done := context.WithCancel(context.Background())
	d := &DynamicFanOut{
		maxInFlight:   1,
		stats:         stats,
		log:           logger,
		onAdd:         func(l string) {},
		onRemove:      func(l string) {},
		transactions:  nil,
		newOutputChan: make(chan wrappedOutput),
		outputs:       make(map[string]outputWithTSChan, len(outputs)),
		closedChan:    make(chan struct{}),
		ctx:           ctx,
		close:         done,
	}
	for _, opt := range options {
		opt(d)
	}

	for k, v := range outputs {
		if err := d.addOutput(k, v); err != nil {
			return nil, fmt.Errorf("failed to initialise dynamic output '%v': %v", k, err)
		}
		d.onAdd(k)
	}
	return d, nil
}

// WithMaxInFlight sets the maximum number of in-flight messages this broker
// supports. This must be set before calling Consume.
func (d *DynamicFanOut) WithMaxInFlight(i int) *DynamicFanOut {
	if i < 1 {
		i = 1
	}
	d.maxInFlight = i
	return d
}

// SetOutput attempts to add a new output to the dynamic output broker. If an
// output already exists with the same identifier it will be closed and removed.
// If either action takes longer than the timeout period an error will be
// returned.
//
// A nil output argument is safe and will simply remove the previous output
// under the indentifier, if there was one.
//
// TODO: V4 use context here instead.
func (d *DynamicFanOut) SetOutput(ident string, output DynamicOutput, timeout time.Duration) error {
	ctx, done := context.WithTimeout(d.ctx, timeout)
	defer done()
	resChan := make(chan error, 1)
	select {
	case d.newOutputChan <- wrappedOutput{
		Name:    ident,
		Output:  output,
		ResChan: resChan,
		Ctx:     ctx,
	}:
	case <-ctx.Done():
		return component.ErrTimeout
	}
	select {
	case err := <-resChan:
		return err
	case <-ctx.Done():
	}
	return component.ErrTimeout
}

//------------------------------------------------------------------------------

// OptDynamicFanOutSetOnAdd sets the function that is called whenever a dynamic
// output is added.
func OptDynamicFanOutSetOnAdd(onAddFunc func(label string)) func(*DynamicFanOut) {
	return func(d *DynamicFanOut) {
		d.onAdd = onAddFunc
	}
}

// OptDynamicFanOutSetOnRemove sets the function that is called whenever a
// dynamic output is removed.
func OptDynamicFanOutSetOnRemove(onRemoveFunc func(label string)) func(*DynamicFanOut) {
	return func(d *DynamicFanOut) {
		d.onRemove = onRemoveFunc
	}
}

//------------------------------------------------------------------------------

// Consume assigns a new transactions channel for the broker to read.
func (d *DynamicFanOut) Consume(transactions <-chan types.Transaction) error {
	if d.transactions != nil {
		return component.ErrAlreadyStarted
	}
	d.transactions = transactions

	go d.loop()
	return nil
}

//------------------------------------------------------------------------------

func (d *DynamicFanOut) addOutput(ident string, output DynamicOutput) error {
	if _, exists := d.outputs[ident]; exists {
		return fmt.Errorf("output key '%v' already exists", ident)
	}

	ow := outputWithTSChan{
		tsChan: make(chan types.Transaction),
		output: output,
	}

	if err := output.Consume(ow.tsChan); err != nil {
		output.CloseAsync()
		return err
	}
	ow.ctx, ow.done = context.WithCancel(context.Background())

	d.outputs[ident] = ow
	return nil
}

func (d *DynamicFanOut) removeOutput(ctx context.Context, ident string) error {
	ow, exists := d.outputs[ident]
	if !exists {
		return nil
	}

	timeout := time.Second * 5
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	ow.output.CloseAsync()
	err := ow.output.WaitForClose(timeout)

	ow.done()
	close(ow.tsChan)
	delete(d.outputs, ident)

	return err
}

//------------------------------------------------------------------------------

// loop is an internal loop that brokers incoming messages to many outputs.
func (d *DynamicFanOut) loop() {
	var (
		wg          = sync.WaitGroup{}
		mCount      = d.stats.GetCounter("count")
		mRemoveErr  = d.stats.GetCounter("output.remove.error")
		mRemoveSucc = d.stats.GetCounter("output.remove.success")
		mAddErr     = d.stats.GetCounter("output.add.error")
		mAddSucc    = d.stats.GetCounter("output.add.success")
		mMsgsRcd    = d.stats.GetCounter("messages.received")
		mOutputErr  = d.stats.GetCounter("output.error")
		mMsgsSnt    = d.stats.GetCounter("messages.sent")
	)

	defer func() {
		wg.Wait()
		for _, ow := range d.outputs {
			ow.output.CloseAsync()
			close(ow.tsChan)
		}
		for _, ow := range d.outputs {
			if err := ow.output.WaitForClose(time.Second); err != nil {
				for err != nil {
					err = ow.output.WaitForClose(time.Second)
				}
			}
		}
		d.outputs = map[string]outputWithTSChan{}
		close(d.closedChan)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case wrappedOutput, open := <-d.newOutputChan:
				if !open {
					return
				}
				func() {
					d.outputsMut.Lock()
					defer d.outputsMut.Unlock()

					// First, always remove the previous output if it exists.
					if _, exists := d.outputs[wrappedOutput.Name]; exists {
						if err := d.removeOutput(wrappedOutput.Ctx, wrappedOutput.Name); err != nil {
							mRemoveErr.Incr(1)
							d.log.Errorf("Failed to stop old copy of dynamic output '%v' in time: %v, the output will continue to shut down in the background.\n", wrappedOutput.Name, err)
						} else {
							mRemoveSucc.Incr(1)
						}
						d.onRemove(wrappedOutput.Name)
					}

					// Next, attempt to create a new output (if specified).
					if wrappedOutput.Output == nil {
						wrappedOutput.ResChan <- nil
					} else {
						err := d.addOutput(wrappedOutput.Name, wrappedOutput.Output)
						if err != nil {
							mAddErr.Incr(1)
							d.log.Errorf("Failed to start new dynamic output '%v': %v\n", wrappedOutput.Name, err)
						} else {
							mAddSucc.Incr(1)
							d.onAdd(wrappedOutput.Name)
						}
						wrappedOutput.ResChan <- err
					}
				}()
			case <-d.ctx.Done():
				return
			}
		}
	}()

	sendLoop := func() {
		defer wg.Done()

		for {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-d.transactions:
				if !open {
					d.close()
					return
				}
				mCount.Incr(1)
			case <-d.ctx.Done():
				return
			}
			mMsgsRcd.Incr(1)

			d.outputsMut.RLock()
			for len(d.outputs) == 0 {
				// Assuming this isn't a common enough occurrence that it
				// won't be busy enough to require a sync.Cond, looping with
				// a sleep is fine for now.
				d.outputsMut.RUnlock()
				select {
				case <-time.After(time.Millisecond * 10):
				case <-d.ctx.Done():
					return
				}
				d.outputsMut.RLock()
			}

			var owg errgroup.Group
			for name := range d.outputs {
				msgCopy, name := ts.Payload.Copy(), name
				owg.Go(func() error {
					throt := throttle.New(throttle.OptCloseChan(d.ctx.Done()))
					resChan := make(chan types.Response)

					// Try until success, shutdown, or the output was removed.
					for {
						d.outputsMut.RLock()
						output, exists := d.outputs[name]
						if !exists {
							d.outputsMut.RUnlock()
							return nil
						}

						select {
						case output.tsChan <- types.NewTransaction(msgCopy, resChan):
						case <-d.ctx.Done():
							d.outputsMut.RUnlock()
							return component.ErrTypeClosed
						}

						// Allow outputs to be mutated at this stage in case the
						// response is slow.
						d.outputsMut.RUnlock()

						select {
						case res := <-resChan:
							if res.AckError() != nil {
								d.log.Errorf("Failed to dispatch dynamic fan out message to '%v': %v\n", name, res.AckError())
								mOutputErr.Incr(1)
								if cont := throt.Retry(); !cont {
									return component.ErrTypeClosed
								}
							} else {
								mMsgsSnt.Incr(1)
								return nil
							}
						case <-output.ctx.Done():
							return nil
						case <-d.ctx.Done():
							return component.ErrTypeClosed
						}
					}
				})
			}
			d.outputsMut.RUnlock()

			if owg.Wait() == nil {
				select {
				case ts.ResponseChan <- response.NewAck():
				case <-d.ctx.Done():
					return
				}
			}
		}
	}

	// Max in flight
	for i := 0; i < d.maxInFlight; i++ {
		wg.Add(1)
		go sendLoop()
	}
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (d *DynamicFanOut) Connected() bool {
	d.outputsMut.RLock()
	defer d.outputsMut.RUnlock()
	for _, out := range d.outputs {
		if !out.output.Connected() {
			return false
		}
	}
	return true
}

// MaxInFlight returns the maximum number of in flight messages permitted by the
// output. This value can be used to determine a sensible value for parent
// outputs, but should not be relied upon as part of dispatcher logic.
func (d *DynamicFanOut) MaxInFlight() (int, bool) {
	return d.maxInFlight, true
}

// CloseAsync shuts down the DynamicFanOut broker and stops processing requests.
func (d *DynamicFanOut) CloseAsync() {
	d.close()
}

// WaitForClose blocks until the DynamicFanOut broker has closed down.
func (d *DynamicFanOut) WaitForClose(timeout time.Duration) error {
	select {
	case <-d.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
