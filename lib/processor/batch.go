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

package processor

import (
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["batch"] = TypeSpec{
		constructor: NewBatch,
		description: `
Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until the total size of the batch in bytes matches or exceeds the
configured byte size. Once the limit is reached the parts are combined into a
single batch of messages and sent through the pipeline. Once the combined batch
has reached a destination the acknowledgment is sent out for all messages inside
the batch, preserving at-least-once delivery guarantees.

When a batch is sent to an output the behaviour will differ depending on the
protocol. If the output type supports multipart messages then the batch is sent
as a single message with multiple parts. If the output only supports single part
messages then the parts will be sent as a batch of single part messages. If the
output supports neither multipart or batches of messages then Benthos falls back
to sending them individually.

If a Benthos stream contains multiple brokered inputs or outputs then the batch
operator should *always* be applied directly after an input in order to avoid
unexpected behaviour and message ordering.`,
	}
}

//------------------------------------------------------------------------------

// BatchConfig contains configuration for the Batch processor.
type BatchConfig struct {
	ByteSize int `json:"byte_size" yaml:"byte_size"`
}

// NewBatchConfig returns a BatchConfig with default values.
func NewBatchConfig() BatchConfig {
	return BatchConfig{
		ByteSize: 10000,
	}
}

//------------------------------------------------------------------------------

// Batch is a processor that combines messages into a batch until a size limit
// is reached, at which point the batch is sent out.
type Batch struct {
	log       log.Modular
	stats     metrics.Type
	n         int
	sizeTally int
	parts     [][]byte

	mCount   metrics.StatCounter
	mSent    metrics.StatCounter
	mDropped metrics.StatCounter
}

// NewBatch returns a Batch processor.
func NewBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Batch{
		log:   log.NewModule(".processor.batch"),
		stats: stats,
		n:     conf.Batch.ByteSize,

		mCount:   stats.GetCounter("processor.batch.count"),
		mSent:    stats.GetCounter("processor.batch.sent"),
		mDropped: stats.GetCounter("processor.batch.dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a single message and buffers it, drops it, returning a
// NoAck response, until eventually it reaches a size limit, at which point it
// batches those messages into one multiple part message which is sent on.
func (c *Batch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	// Add new parts to the buffer.
	for _, part := range msg.GetAll() {
		c.sizeTally += len(part)
		c.parts = append(c.parts, part)
	}

	// If we have reached our target count of parts in the buffer.
	if c.sizeTally >= c.n {
		newMsg := types.NewMessage(c.parts)
		c.parts = nil
		c.sizeTally = 0

		c.mSent.Incr(1)
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	c.mDropped.Incr(1)
	return nil, types.NewUnacknowledgedResponse()
}

//------------------------------------------------------------------------------
