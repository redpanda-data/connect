package io

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

// wrappedInput is a struct that wraps a input.Streamed with an identifying name.
type wrappedInput struct {
	ctx     context.Context
	Name    string
	Input   input.Streamed
	ResChan chan<- error
}

type dynamicFanInInput struct {
	log log.Modular

	transactionChan chan message.Transaction

	onAdd    func(ctx context.Context, label string)
	onRemove func(ctx context.Context, label string)

	newInputChan     chan wrappedInput
	inputs           map[string]input.Streamed
	inputClosedChans map[string]chan struct{}

	shutSig *shutdown.Signaller
}

func newDynamicFanInInput(
	inputs map[string]input.Streamed,
	logger log.Modular,
	onAdd func(ctx context.Context, l string),
	onRemove func(ctx context.Context, l string),
) (*dynamicFanInInput, error) {
	d := &dynamicFanInInput{
		log: logger,

		transactionChan: make(chan message.Transaction),

		onAdd:    func(ctx context.Context, l string) {},
		onRemove: func(ctx context.Context, l string) {},

		newInputChan:     make(chan wrappedInput),
		inputs:           make(map[string]input.Streamed),
		inputClosedChans: make(map[string]chan struct{}),

		shutSig: shutdown.NewSignaller(),
	}
	if onAdd != nil {
		d.onAdd = onAdd
	}
	if onRemove != nil {
		d.onRemove = onRemove
	}
	for key, input := range inputs {
		if err := d.addInput(key, input); err != nil {
			d.log.Errorf("Failed to start new dynamic input '%v': %v\n", key, err)
		}
	}
	go d.managerLoop()
	return d, nil
}

// SetInput attempts to add a new input to the dynamic input broker. If an input
// already exists with the same identifier it will be closed and removed. If
// either action takes longer than the timeout period an error will be returned.
//
// A nil input is safe and will simply remove the previous input under the
// indentifier, if there was one.
func (d *dynamicFanInInput) SetInput(ctx context.Context, ident string, input input.Streamed) error {
	if d.shutSig.ShouldCloseAtLeisure() {
		return component.ErrTypeClosed
	}
	resChan := make(chan error)
	select {
	case d.newInputChan <- wrappedInput{
		ctx:     ctx,
		Name:    ident,
		Input:   input,
		ResChan: resChan,
	}:
	case <-d.shutSig.CloseAtLeisureChan():
		return component.ErrTypeClosed
	}
	return <-resChan
}

func (d *dynamicFanInInput) TransactionChan() <-chan message.Transaction {
	return d.transactionChan
}

func (d *dynamicFanInInput) Connected() bool {
	// Always return true as this is fuzzy right now.
	return true
}

func (d *dynamicFanInInput) addInput(ident string, in input.Streamed) error {
	closedChan := make(chan struct{})
	// Launch goroutine that async writes input into single channel
	go func(in input.Streamed, cChan chan struct{}) {
		defer func() {
			d.onRemove(context.Background(), ident)
			close(cChan)
		}()
		d.onAdd(context.Background(), ident)
		for {
			in, open := <-in.TransactionChan()
			if !open {
				// Race condition: This will be called when shutting down.
				return
			}
			d.transactionChan <- in
		}
	}(in, closedChan)

	// Add new input to our map
	d.inputs[ident] = in
	d.inputClosedChans[ident] = closedChan

	return nil
}

func (d *dynamicFanInInput) removeInput(ctx context.Context, ident string) error {
	input, exists := d.inputs[ident]
	if !exists {
		// Nothing to do
		return nil
	}

	input.TriggerStopConsuming()
	select {
	case <-d.inputClosedChans[ident]:
	case <-ctx.Done():
		// Do NOT remove inputs from our map unless we are sure they are
		// closed.
		return ctx.Err()
	}

	delete(d.inputs, ident)
	delete(d.inputClosedChans, ident)

	return nil
}

// managerLoop is an internal loop that monitors new and dead input types.
func (d *dynamicFanInInput) managerLoop() {
	defer func() {
		for _, i := range d.inputs {
			i.TriggerStopConsuming()
		}

		closeNowCtx, done := d.shutSig.CloseNowCtx(context.Background())
		for key := range d.inputs {
			_ = d.removeInput(closeNowCtx, key)
		}

		for _, i := range d.inputs {
			i.TriggerCloseNow()
		}

		done()
		close(d.transactionChan)
		d.shutSig.ShutdownComplete()
	}()

	for {
		select {
		case wrappedInput, open := <-d.newInputChan:
			if !open {
				return
			}
			var err error
			if _, exists := d.inputs[wrappedInput.Name]; exists {
				if err = d.removeInput(wrappedInput.ctx, wrappedInput.Name); err != nil {
					d.log.Errorf("Failed to stop old copy of dynamic input '%v': %v\n", wrappedInput.Name, err)
				}
			}
			if err == nil && wrappedInput.Input != nil {
				// If the input is nil then we only wanted to remove the input.
				if err = d.addInput(wrappedInput.Name, wrappedInput.Input); err != nil {
					d.log.Errorf("Failed to start new dynamic input '%v': %v\n", wrappedInput.Name, err)
				}
			}
			select {
			case wrappedInput.ResChan <- err:
			case <-d.shutSig.CloseAtLeisureChan():
				close(wrappedInput.ResChan)
				return
			}
		case <-d.shutSig.CloseAtLeisureChan():
			return
		}
	}
}

func (d *dynamicFanInInput) TriggerStopConsuming() {
	d.shutSig.CloseAtLeisure()
}

func (d *dynamicFanInInput) TriggerCloseNow() {
	d.shutSig.CloseNow()
}

func (d *dynamicFanInInput) WaitForClose(ctx context.Context) error {
	select {
	case <-d.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
