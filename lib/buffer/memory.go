// Copyright (c) 2014 Ashley Jeffs
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

package buffer

import (
	"github.com/Jeffail/benthos/lib/buffer/parallel"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["memory"] = TypeSpec{
		constructor: NewMemory,
		description: `
The memory buffer type simply allocates a set amount of RAM for buffering
messages. This can be useful when reading from sources that produce large bursts
of data. Messages inside the buffer are lost if the service is stopped.`,
	}
}

//------------------------------------------------------------------------------

// NewMemory - Create a buffer held in memory.
func NewMemory(config Config, log log.Modular, stats metrics.Type) (Type, error) {
	return NewParallelWrapper(config, parallel.NewMemory(config.Memory.Limit), log, stats), nil
}

//------------------------------------------------------------------------------
