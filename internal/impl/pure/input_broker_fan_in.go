package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type fanInInputBroker struct {
	transactions chan message.Transaction

	closables       []input.Streamed
	inputClosedChan chan int
	inputMap        map[int]struct{}

	closedChan chan struct{}
}

func newFanInInputBroker(inputs []input.Streamed) (*fanInInputBroker, error) {
	i := &fanInInputBroker{
		transactions: make(chan message.Transaction),

		inputClosedChan: make(chan int),
		inputMap:        make(map[int]struct{}),

		closables:  []input.Streamed{},
		closedChan: make(chan struct{}),
	}

	for n, input := range inputs {
		i.closables = append(i.closables, input)

		// Keep track of # open inputs
		i.inputMap[n] = struct{}{}

		// Launch goroutine that async writes input into single channel
		go func(index int) {
			defer func() {
				// If the input closes we need to signal to the broker
				i.inputClosedChan <- index
			}()
			for {
				in, open := <-inputs[index].TransactionChan()
				if !open {
					return
				}
				i.transactions <- in
			}
		}(n)
	}

	go i.loop()
	return i, nil
}

func (i *fanInInputBroker) TransactionChan() <-chan message.Transaction {
	return i.transactions
}

func (i *fanInInputBroker) Connected() bool {
	for _, in := range i.closables {
		if !in.Connected() {
			return false
		}
	}
	return true
}

func (i *fanInInputBroker) loop() {
	defer func() {
		close(i.inputClosedChan)
		close(i.transactions)
		close(i.closedChan)
	}()

	for len(i.inputMap) > 0 {
		index := <-i.inputClosedChan
		delete(i.inputMap, index)
	}
}

func (i *fanInInputBroker) TriggerStopConsuming() {
	for _, closable := range i.closables {
		closable.TriggerStopConsuming()
	}
}

func (i *fanInInputBroker) TriggerCloseNow() {
	for _, closable := range i.closables {
		closable.TriggerCloseNow()
	}
}

func (i *fanInInputBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-i.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
