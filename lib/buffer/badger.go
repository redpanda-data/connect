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

package buffer

import (
	"github.com/Jeffail/benthos/lib/buffer/parallel"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["badger"] = TypeSpec{
		constructor: NewBadger,
		description: `
The badger buffer type uses a [badger](https://github.com/dgraph-io/badger) db
in order to persist messages to disk as a key/value store. The benefit of this
method is that unlike the mmap_file approach this buffer can be emptied by
parallel consumers.

Note that throughput can be significantly improved by disabling 'sync_writes',
but this comes at the cost of delivery guarantees under crashes.

This buffer has stronger delivery guarantees and higher throughput across
brokered outputs (except for the fan_out pattern) at the cost of lower single
output throughput.`,
	}
}

//------------------------------------------------------------------------------

// NewBadger creates a buffer around a badger k/v db.
func NewBadger(config Config, log log.Modular, stats metrics.Type) (Type, error) {
	b, err := parallel.NewBadger(config.Badger)
	if err != nil {
		return nil, err
	}
	return NewParallelWrapper(config, b, log, stats), nil
}

//------------------------------------------------------------------------------
