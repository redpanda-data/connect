// Copyright (c) 2017 Ashley Jeffs
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

package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNoop] = TypeSpec{
		constructor: NewNoop,
		description: `
Noop is a no-op processor that does nothing, the message passes through
unchanged.`,
	}
}

//------------------------------------------------------------------------------

// Noop is a no-op processor that does nothing.
type Noop struct {
}

// NewNoop returns a Noop processor.
func NewNoop(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Noop{}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage does nothing and returns the message unchanged.
func (c *Noop) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Noop) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Noop) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
