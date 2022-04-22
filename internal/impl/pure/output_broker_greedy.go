package pure

import (
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type greedyOutputBroker struct {
	outputs []output.Streamed
}

func newGreedyOutputBroker(outputs []output.Streamed) (*greedyOutputBroker, error) {
	return &greedyOutputBroker{
		outputs: outputs,
	}, nil
}

func (g *greedyOutputBroker) Consume(ts <-chan message.Transaction) error {
	for _, out := range g.outputs {
		if err := out.Consume(ts); err != nil {
			return err
		}
	}
	return nil
}

func (g *greedyOutputBroker) Connected() bool {
	for _, out := range g.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

func (g *greedyOutputBroker) CloseAsync() {
	for _, out := range g.outputs {
		out.CloseAsync()
	}
}

func (g *greedyOutputBroker) WaitForClose(timeout time.Duration) error {
	tStarted := time.Now()
	remaining := timeout
	for _, out := range g.outputs {
		if err := out.WaitForClose(remaining); err != nil {
			return err
		}
		remaining -= time.Since(tStarted)
		if remaining <= 0 {
			return component.ErrTimeout
		}
	}
	return nil
}
