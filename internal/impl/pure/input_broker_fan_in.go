package pure

import (
	"context"
	"errors"
	"sync"

	"github.com/Jeffail/shutdown"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type fanInInputBroker struct {
	transactions chan message.Transaction

	closables       []input.Streamed
	inputClosedChan chan int
	remainingMap    map[int]struct{}
	remainingMapMut sync.Mutex

	shutSig *shutdown.Signaller
}

func newFanInInputBroker(inputs []input.Streamed) (*fanInInputBroker, error) {
	if len(inputs) == 0 {
		return nil, errors.New("fan in broker requires at least one input")
	}

	i := &fanInInputBroker{
		transactions: make(chan message.Transaction),

		inputClosedChan: make(chan int),
		remainingMap:    make(map[int]struct{}),

		closables: []input.Streamed{},
		shutSig:   shutdown.NewSignaller(),
	}

	for n, input := range inputs {
		i.closables = append(i.closables, input)

		// Keep track of # open inputs
		i.remainingMap[n] = struct{}{}

		// Launch goroutine that async writes input into single channel
		go func(index int) {
			defer func() {
				// If the input closes we need to signal to the broker
				i.inputClosedChan <- index
			}()
			for {
				var in message.Transaction
				var open bool
				select {
				case in, open = <-inputs[index].TransactionChan():
					if !open {
						return
					}
				case <-i.shutSig.HardStopChan():
					return
				}
				select {
				case i.transactions <- in:
				case <-i.shutSig.HardStopChan():
					return
				}
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
	i.remainingMapMut.Lock()
	defer i.remainingMapMut.Unlock()

	if len(i.remainingMap) == 0 {
		return false
	}

	for index := range i.remainingMap {
		if !i.closables[index].Connected() {
			return false
		}
	}
	return true
}

func (i *fanInInputBroker) loop() {
	defer func() {
		close(i.inputClosedChan)
		close(i.transactions)
		i.shutSig.TriggerHasStopped()
	}()

	for {
		index := <-i.inputClosedChan

		i.remainingMapMut.Lock()
		delete(i.remainingMap, index)
		remaining := len(i.remainingMap)
		i.remainingMapMut.Unlock()

		if remaining == 0 {
			return
		}
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
	i.shutSig.TriggerHardStop()
}

func (i *fanInInputBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-i.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
