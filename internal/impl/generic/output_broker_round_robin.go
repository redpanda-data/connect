package generic

import (
	"sync/atomic"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type roundRobinOutputBroker struct {
	running int32

	transactions <-chan message.Transaction

	outputTSChans []chan message.Transaction
	outputs       []output.Streamed

	closedChan chan struct{}
	closeChan  chan struct{}
}

func newRoundRobinOutputBroker(outputs []output.Streamed) (*roundRobinOutputBroker, error) {
	o := &roundRobinOutputBroker{
		running:      1,
		transactions: nil,
		outputs:      outputs,
		closedChan:   make(chan struct{}),
		closeChan:    make(chan struct{}),
	}
	o.outputTSChans = make([]chan message.Transaction, len(o.outputs))
	for i := range o.outputTSChans {
		o.outputTSChans[i] = make(chan message.Transaction)
		if err := o.outputs[i].Consume(o.outputTSChans[i]); err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (o *roundRobinOutputBroker) Consume(ts <-chan message.Transaction) error {
	if o.transactions != nil {
		return component.ErrAlreadyStarted
	}
	o.transactions = ts

	go o.loop()
	return nil
}

func (o *roundRobinOutputBroker) Connected() bool {
	for _, out := range o.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

func (o *roundRobinOutputBroker) loop() {
	defer func() {
		for _, c := range o.outputTSChans {
			close(c)
		}
		closeAllOutputs(o.outputs)
		close(o.closedChan)
	}()

	i := 0
	var open bool
	for atomic.LoadInt32(&o.running) == 1 {
		var ts message.Transaction
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.closeChan:
			return
		}
		select {
		case o.outputTSChans[i] <- ts:
		case <-o.closeChan:
			return
		}

		i++
		if i >= len(o.outputTSChans) {
			i = 0
		}
	}
}

func (o *roundRobinOutputBroker) CloseAsync() {
	if atomic.CompareAndSwapInt32(&o.running, 1, 0) {
		close(o.closeChan)
	}
}

func (o *roundRobinOutputBroker) WaitForClose(timeout time.Duration) error {
	select {
	case <-o.closedChan:
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
