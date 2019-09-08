// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
