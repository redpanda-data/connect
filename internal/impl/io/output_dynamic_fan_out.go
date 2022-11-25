package io

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

// wrappedOutput is a struct that wraps a DynamicOutput with an identifying
// name.
type wrappedOutput struct {
	Ctx     context.Context
	Name    string
	Output  output.Streamed
	ResChan chan<- error
}

// outputWithTSChan is a struct containing both an output and the transaction
// chan it reads from.
type outputWithTSChan struct {
	tsChan chan message.Transaction
	output output.Streamed
	ctx    context.Context
	done   func()
}

type dynamicFanOutOutputBroker struct {
	log log.Modular

	onAdd    func(label string)
	onRemove func(label string)

	transactions <-chan message.Transaction

	outputsMut    sync.RWMutex
	newOutputChan chan wrappedOutput
	outputs       map[string]outputWithTSChan

	shutSig *shutdown.Signaller
}

func newDynamicFanOutOutputBroker(
	outputs map[string]output.Streamed,
	logger log.Modular,
	onAdd func(label string),
	onRemove func(label string),
) (*dynamicFanOutOutputBroker, error) {
	d := &dynamicFanOutOutputBroker{
		log:           logger,
		transactions:  nil,
		newOutputChan: make(chan wrappedOutput),
		outputs:       make(map[string]outputWithTSChan, len(outputs)),
		shutSig:       shutdown.NewSignaller(),
		onAdd:         onAdd,
		onRemove:      onRemove,
	}
	if d.onAdd == nil {
		d.onAdd = func(l string) {}
	}
	if d.onRemove == nil {
		d.onRemove = func(l string) {}
	}

	for k, v := range outputs {
		if err := d.addOutput(k, v); err != nil {
			return nil, fmt.Errorf("failed to initialise dynamic output '%v': %v", k, err)
		}
		d.onAdd(k)
	}
	return d, nil
}

// SetOutput attempts to add a new output to the dynamic output broker. If an
// output already exists with the same identifier it will be closed and removed.
// If either action takes longer than the timeout period an error will be
// returned.
//
// A nil output argument is safe and will simply remove the previous output
// under the indentifier, if there was one.
func (d *dynamicFanOutOutputBroker) SetOutput(ctx context.Context, ident string, output output.Streamed) error {
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

func (d *dynamicFanOutOutputBroker) Consume(transactions <-chan message.Transaction) error {
	if d.transactions != nil {
		return component.ErrAlreadyStarted
	}
	d.transactions = transactions

	go d.loop()
	return nil
}

func (d *dynamicFanOutOutputBroker) addOutput(ident string, output output.Streamed) error {
	if _, exists := d.outputs[ident]; exists {
		return fmt.Errorf("output key '%v' already exists", ident)
	}

	ow := outputWithTSChan{
		tsChan: make(chan message.Transaction),
		output: output,
	}

	if err := output.Consume(ow.tsChan); err != nil {
		output.TriggerCloseNow()
		return err
	}
	ow.ctx, ow.done = context.WithCancel(context.Background())

	d.outputs[ident] = ow
	return nil
}

func (d *dynamicFanOutOutputBroker) removeOutput(ctx context.Context, ident string) error {
	ow, exists := d.outputs[ident]
	if !exists {
		return nil
	}

	ow.output.TriggerCloseNow()
	err := ow.output.WaitForClose(ctx)

	ow.done()
	close(ow.tsChan)
	delete(d.outputs, ident)

	return err
}

func (d *dynamicFanOutOutputBroker) loop() {
	apiWG := sync.WaitGroup{}

	ackInterruptChan := make(chan struct{})
	var ackPending int64

	defer func() {
		// Wait for pending acks to be resolved, or forceful termination
	ackWaitLoop:
		for atomic.LoadInt64(&ackPending) > 0 {
			select {
			case <-ackInterruptChan:
			case <-time.After(time.Millisecond * 100):
				// Just incase an interrupt doesn't arrive.
			case <-d.shutSig.CloseAtLeisureChan():
				break ackWaitLoop
			}
		}

		for _, ow := range d.outputs {
			ow.output.TriggerCloseNow()
			close(ow.tsChan)
		}
		for _, ow := range d.outputs {
			ow.output.TriggerCloseNow()
		}
		for _, ow := range d.outputs {
			_ = ow.output.WaitForClose(context.Background())
		}

		d.shutSig.CloseNow()
		apiWG.Wait()

		d.outputs = map[string]outputWithTSChan{}
		d.shutSig.ShutdownComplete()
	}()

	apiWG.Add(1)
	go func() {
		defer apiWG.Done()
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
							d.log.Errorf("Failed to stop old copy of dynamic output '%v' in time: %v, the output will continue to shut down in the background.\n", wrappedOutput.Name, err)
						}
						d.onRemove(wrappedOutput.Name)
					}

					// Next, attempt to create a new output (if specified).
					if wrappedOutput.Output == nil {
						wrappedOutput.ResChan <- nil
					} else {
						err := d.addOutput(wrappedOutput.Name, wrappedOutput.Output)
						if err != nil {
							d.log.Errorf("Failed to start new dynamic output '%v': %v\n", wrappedOutput.Name, err)
						} else {
							d.onAdd(wrappedOutput.Name)
						}
						wrappedOutput.ResChan <- err
					}
				}()
			case <-d.shutSig.CloseAtLeisureChan():
				return
			}
		}
	}()

	for {
		var ts message.Transaction
		var open bool
		select {
		case ts, open = <-d.transactions:
			if !open {
				return
			}
		case <-d.shutSig.CloseAtLeisureChan():
			return
		}

		d.outputsMut.RLock()
		for len(d.outputs) == 0 {
			// Assuming this isn't a common enough occurrence that it
			// won't be busy enough to require a sync.Cond, looping with
			// a sleep is fine for now.
			d.outputsMut.RUnlock()
			select {
			case <-time.After(time.Millisecond * 10):
			case <-d.shutSig.CloseAtLeisureChan():
				return
			}
			d.outputsMut.RLock()
		}

		_ = atomic.AddInt64(&ackPending, 1)
		pendingResponses := int64(len(d.outputs))

		for _, output := range d.outputs {
			select {
			case output.tsChan <- message.NewTransactionFunc(ts.Payload.ShallowCopy(), func(ctx context.Context, err error) error {
				if atomic.AddInt64(&pendingResponses, -1) == 0 || err != nil {
					atomic.StoreInt64(&pendingResponses, 0)
					ackErr := ts.Ack(ctx, err)
					_ = atomic.AddInt64(&ackPending, -1)
					select {
					case ackInterruptChan <- struct{}{}:
					default:
					}
					return ackErr
				}
				return nil
			}):
			case <-d.shutSig.CloseAtLeisureChan():
				break // This signal will be caught again in the next loop
			}
		}
		d.outputsMut.RUnlock()
	}
}

func (d *dynamicFanOutOutputBroker) Connected() bool {
	d.outputsMut.RLock()
	defer d.outputsMut.RUnlock()
	for _, out := range d.outputs {
		if !out.output.Connected() {
			return false
		}
	}
	return true
}

func (d *dynamicFanOutOutputBroker) TriggerCloseNow() {
	d.shutSig.CloseNow()
}

func (d *dynamicFanOutOutputBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-d.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
