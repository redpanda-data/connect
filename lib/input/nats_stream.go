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

package input

import (
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNATSStream] = TypeSpec{
		constructor: NewNATSStream,
		description: `
Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics.

Tracking and persisting offsets through a durable name is also optional and
works with or without a queue. If a durable name is not provided then subjects
are consumed from the most recently published message.

When a consumer closes its connection it unsubscribes, when all consumers of a
durable queue do this the offsets are deleted. In order to avoid this you can
stop the consumers from unsubscribing by setting the field
` + "`unsubscribe_on_close` to `false`" + `.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

Use the ` + "`batching`" + ` fields to configure an optional
[batching policy](../batching.md#batch-policy).

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_stream_subject
- nats_stream_sequence
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.NATSStream, conf.NATSStream.Batching)
		},
	}
}

//------------------------------------------------------------------------------

// NewNATSStream creates a new NATSStream input type.
func NewNATSStream(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var c reader.Async
	var err error
	if c, err = reader.NewNATSStream(conf.NATSStream, log, stats); err != nil {
		return nil, err
	}
	c = reader.NewAsyncBundleUnacks(c)
	if c, err = reader.NewAsyncBatcher(conf.NATSStream.Batching, c, mgr, log, stats); err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeNATSStream, true, c, log, stats)
}

//------------------------------------------------------------------------------
