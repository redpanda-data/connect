package broker

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Greedy is a broker that implements types.Consumer and sends each message
// out to a single consumer chosen from an array in round-robin fashion.
// Consumers that apply backpressure will block all consumers.
type Greedy struct {
	outputs []types.Output
}

// NewGreedy creates a new Greedy type by providing consumers.
func NewGreedy(outputs []types.Output) (*Greedy, error) {
	return &Greedy{
		outputs: outputs,
	}, nil
}

//------------------------------------------------------------------------------

// Consume assigns a new messages channel for the broker to read.
func (g *Greedy) Consume(ts <-chan types.Transaction) error {
	for _, out := range g.outputs {
		if err := out.Consume(ts); err != nil {
			return err
		}
	}
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (g *Greedy) Connected() bool {
	for _, out := range g.outputs {
		if !out.Connected() {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------

// CloseAsync shuts down the Greedy broker and stops processing requests.
func (g *Greedy) CloseAsync() {
	for _, out := range g.outputs {
		out.CloseAsync()
	}
}

// WaitForClose blocks until the Greedy broker has closed down.
func (g *Greedy) WaitForClose(timeout time.Duration) error {
	tStarted := time.Now()
	remaining := timeout
	for _, out := range g.outputs {
		if err := out.WaitForClose(remaining); err != nil {
			return err
		}
		remaining = remaining - time.Since(tStarted)
		if remaining <= 0 {
			return types.ErrTimeout
		}
	}
	return nil
}

//------------------------------------------------------------------------------
