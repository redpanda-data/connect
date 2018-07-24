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
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["batch"] = TypeSpec{
		constructor: NewBatch,
		description: `
Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until either:

- The total size of the batch in bytes matches or exceeds ` + "`byte_size`" + `.
- A message added to the batch causes the condition to resolve ` + "`true`" + `.
- The ` + "`period_ms`" + ` field is non-zero and the time since the last batch
  exceeds its value.

Once one of these events trigger the parts are combined into a single batch of
messages and sent through the pipeline. After reaching a destination the
acknowledgment is sent out for all messages inside the batch at the same time,
preserving at-least-once delivery guarantees.

The ` + "`period_ms`" + ` field is optional, and when greater than zero defines
a period in milliseconds whereby a batch is sent even if the ` + "`byte_size`" + `
has not yet been reached. Batch parameters are only triggered when a message is
added, meaning a pending batch can last beyond this period if no messages are
added since the period was reached.

When a batch is sent to an output the behaviour will differ depending on the
protocol. If the output type supports multipart messages then the batch is sent
as a single message with multiple parts. If the output only supports single part
messages then the parts will be sent as a batch of single part messages. If the
output supports neither multipart or batches of messages then Benthos falls back
to sending them individually.

If a Benthos stream contains multiple brokered inputs or outputs then the batch
operator should *always* be applied directly after an input in order to avoid
unexpected behaviour and message ordering.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			condSanit, err := condition.SanitiseConfig(conf.Batch.Condition)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"byte_size": conf.Batch.ByteSize,
				"condition": condSanit,
				"period_ms": conf.Batch.PeriodMS,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// BatchConfig contains configuration for the Batch processor.
type BatchConfig struct {
	ByteSize  int              `json:"byte_size" yaml:"byte_size"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	PeriodMS  int              `json:"period_ms" yaml:"period_ms"`
}

// NewBatchConfig returns a BatchConfig with default values.
func NewBatchConfig() BatchConfig {
	cond := condition.NewConfig()
	cond.Type = "static"
	cond.Static = false
	return BatchConfig{
		ByteSize:  10000,
		Condition: cond,
		PeriodMS:  0,
	}
}

//------------------------------------------------------------------------------

// Batch is a processor that combines messages into a batch until a size limit
// is reached, at which point the batch is sent out.
type Batch struct {
	log   log.Modular
	stats metrics.Type

	n         int
	period    time.Duration
	cond      condition.Type
	sizeTally int
	parts     [][]byte

	lastBatch time.Time

	mCount       metrics.StatCounter
	mSent        metrics.StatCounter
	mSentParts   metrics.StatCounter
	mSizeBatch   metrics.StatCounter
	mPeriodBatch metrics.StatCounter
	mCondBatch   metrics.StatCounter
	mDropped     metrics.StatCounter
}

// NewBatch returns a Batch processor.
func NewBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	logger := log.NewModule(".processor.batch")
	cond, err := condition.New(conf.Batch.Condition, mgr, logger, metrics.Namespaced(stats, "processor.batch"))
	if err != nil {
		return nil, err
	}
	return &Batch{
		log:    logger,
		stats:  stats,
		n:      conf.Batch.ByteSize,
		period: time.Duration(conf.Batch.PeriodMS) * time.Millisecond,
		cond:   cond,

		lastBatch: time.Now(),

		mCount:       stats.GetCounter("processor.batch.count"),
		mSizeBatch:   stats.GetCounter("processor.batch.on_size"),
		mPeriodBatch: stats.GetCounter("processor.batch.on_period"),
		mCondBatch:   stats.GetCounter("processor.batch.on_condition"),
		mSent:        stats.GetCounter("processor.batch.sent"),
		mSentParts:   stats.GetCounter("processor.batch.parts.sent"),
		mDropped:     stats.GetCounter("processor.batch.dropped"),
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

	batch := false
	if !batch && c.sizeTally >= c.n {
		batch = true
		c.mSizeBatch.Incr(1)
		c.log.Traceln("Batching based on byte_size")
	}
	if !batch && c.period > 0 && time.Since(c.lastBatch) > c.period {
		batch = true
		c.mPeriodBatch.Incr(1)
		c.log.Traceln("Batching based on period_ms")
	}
	if !batch && c.cond.Check(msg) {
		batch = true
		c.mCondBatch.Incr(1)
		c.log.Traceln("Batching based on condition")
	}

	// If we have reached our target count of parts in the buffer.
	if batch {
		newMsg := types.NewMessage(c.parts)
		c.parts = nil
		c.sizeTally = 0
		c.lastBatch = time.Now()

		c.mSentParts.Incr(int64(newMsg.Len()))
		c.mSent.Incr(1)
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	c.log.Traceln("Added message to pending batch")
	c.mDropped.Incr(1)
	return nil, types.NewUnacknowledgedResponse()
}

//------------------------------------------------------------------------------
