package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

type roundRobinOutputBroker struct {
	transactions <-chan message.Transaction

	outputTSChans []chan message.Transaction
	outputs       []output.Streamed

	shutSig *shutdown.Signaller
}

func newRoundRobinOutputBroker(outputs []output.Streamed) (*roundRobinOutputBroker, error) {
	o := &roundRobinOutputBroker{
		transactions: nil,
		outputs:      outputs,
		shutSig:      shutdown.NewSignaller(),
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
		_ = closeAllOutputs(context.Background(), o.outputs)
		o.shutSig.ShutdownComplete()
	}()

	i := 0
	var open bool
	for {
		var ts message.Transaction
		select {
		case ts, open = <-o.transactions:
			if !open {
				return
			}
		case <-o.shutSig.CloseNowChan():
			return
		}
		select {
		case o.outputTSChans[i] <- ts:
		case <-o.shutSig.CloseNowChan():
			return
		}

		i++
		if i >= len(o.outputTSChans) {
			i = 0
		}
	}
}

func (o *roundRobinOutputBroker) TriggerCloseNow() {
	o.shutSig.CloseNow()
}

func (o *roundRobinOutputBroker) WaitForClose(ctx context.Context) error {
	select {
	case <-o.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
