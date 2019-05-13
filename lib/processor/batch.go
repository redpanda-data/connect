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
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/condition"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBatch] = TypeSpec{
		constructor: NewBatch,
		description: `
Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until either:

- The ` + "`byte_size`" + ` field is non-zero and the total size of the batch in
  bytes matches or exceeds it.
- The ` + "`count`" + ` field is non-zero and the total number of messages in
  the batch matches or exceeds it.
- A message added to the batch causes the condition to resolve ` + "`true`" + `.
- The ` + "`period`" + ` field is non-empty and the time since the last batch
  exceeds its value.

Once one of these events trigger the parts are combined into a single batch of
messages and sent through the pipeline. After reaching a destination the
acknowledgment is sent out for all messages inside the batch at the same time,
preserving at-least-once delivery guarantees.

The ` + "`period`" + ` field - when non-empty - defines a period of time whereby
a batch is sent even if the ` + "`byte_size`" + ` has not yet been reached.
Batch parameters are only triggered when a message is added, meaning a pending
batch can last beyond this period if no messages are added since the period was
reached.

When a batch is sent to an output the behaviour will differ depending on the
protocol. If the output type supports multipart messages then the batch is sent
as a single message with multiple parts. If the output only supports single part
messages then the parts will be sent as a batch of single part messages. If the
output supports neither multipart or batches of messages then Benthos falls back
to sending them individually.

### WARNING

The batch processor should *always* be positioned within the ` + "`input`" + `
section - ideally before any other processor - in order to avoid unexpected
acknowledgment behaviour and message ordering.

For more information about batching in Benthos please check out
[this document](../batching.md).`,

		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			condSanit, err := condition.SanitiseConfig(conf.Batch.Condition)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"byte_size": conf.Batch.ByteSize,
				"count":     conf.Batch.Count,
				"condition": condSanit,
				"period":    conf.Batch.Period,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// BatchConfig contains configuration fields for the Batch processor.
type BatchConfig struct {
	ByteSize  int              `json:"byte_size" yaml:"byte_size"`
	Count     int              `json:"count" yaml:"count"`
	Condition condition.Config `json:"condition" yaml:"condition"`
	Period    string           `json:"period" yaml:"period"`
}

// NewBatchConfig returns a BatchConfig with default values.
func NewBatchConfig() BatchConfig {
	cond := condition.NewConfig()
	cond.Type = "static"
	cond.Static = false
	return BatchConfig{
		ByteSize:  0,
		Count:     0,
		Condition: cond,
		Period:    "",
	}
}

//------------------------------------------------------------------------------

// Batch is a processor that combines messages into a batch until a size limit
// or other condition is reached, at which point the batch is sent out. When a
// message is combined without yet producing a batch a NoAck response is
// returned, which is interpretted as source types as an instruction to send
// another message through but hold off on acknowledging this one.
//
// Eventually, when the batch reaches its target size, the batch is sent through
// the pipeline as a single message and an acknowledgement for that message
// determines whether the whole batch of messages are acknowledged.
type Batch struct {
	log   log.Modular
	stats metrics.Type

	byteSize  int
	count     int
	period    time.Duration
	cond      condition.Type
	sizeTally int
	parts     []types.Part

	lastBatch time.Time
	mut       sync.Mutex

	mCount       metrics.StatCounter
	mSent        metrics.StatCounter
	mBatchSent   metrics.StatCounter
	mSizeBatch   metrics.StatCounter
	mCountBatch  metrics.StatCounter
	mPeriodBatch metrics.StatCounter
	mCondBatch   metrics.StatCounter
	mDropped     metrics.StatCounter
}

// NewBatch returns a Batch processor.
func NewBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.Batch.Condition, mgr, log.NewModule(".condition"), metrics.Namespaced(stats, "condition"))
	if err != nil {
		return nil, err
	}
	if conf.Batch.ByteSize <= 0 &&
		conf.Batch.Count <= 0 &&
		len(conf.Batch.Period) <= 0 {
		log.Warnln("Batch processor configured without a count, byte_size or" +
			" period cap. It's possible that this batch will never resolve.")
	}
	var period time.Duration
	if len(conf.Batch.Period) > 0 {
		if period, err = time.ParseDuration(conf.Batch.Period); err != nil {
			return nil, fmt.Errorf("failed to parse duration string: %v", err)
		}
	}
	return &Batch{
		log:      log,
		stats:    stats,
		byteSize: conf.Batch.ByteSize,
		count:    conf.Batch.Count,
		period:   period,
		cond:     cond,

		lastBatch: time.Now(),

		mCount:       stats.GetCounter("count"),
		mSizeBatch:   stats.GetCounter("on_size"),
		mCountBatch:  stats.GetCounter("on_count"),
		mPeriodBatch: stats.GetCounter("on_period"),
		mCondBatch:   stats.GetCounter("on_condition"),
		mSent:        stats.GetCounter("sent"),
		mBatchSent:   stats.GetCounter("batch.sent"),
		mDropped:     stats.GetCounter("dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Batch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	c.mut.Lock()
	defer c.mut.Unlock()

	// Add new parts to the buffer.
	msg.Iter(func(i int, b types.Part) error {
		c.sizeTally += len(b.Get())
		c.parts = append(c.parts, b.Copy())
		return nil
	})

	batch := false
	if !batch && c.count > 0 && len(c.parts) >= c.count {
		batch = true
		c.mCountBatch.Incr(1)
		c.log.Traceln("Batching based on count")
	}
	if !batch && c.byteSize > 0 && c.sizeTally >= c.byteSize {
		batch = true
		c.mSizeBatch.Incr(1)
		c.log.Traceln("Batching based on byte_size")
	}
	if !batch && c.period > 0 && time.Since(c.lastBatch) > c.period {
		batch = true
		c.mPeriodBatch.Incr(1)
		c.log.Traceln("Batching based on period")
	}
	if !batch && c.cond.Check(msg) {
		batch = true
		c.mCondBatch.Incr(1)
		c.log.Traceln("Batching based on condition")
	}

	// If we have reached our target count of parts in the buffer.
	if batch {
		newMsg := message.New(nil)
		newMsg.Append(c.parts...)

		c.parts = nil
		c.sizeTally = 0
		c.lastBatch = time.Now()

		c.mSent.Incr(int64(newMsg.Len()))
		c.mBatchSent.Incr(1)
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	c.log.Traceln("Added message to pending batch")
	c.mDropped.Incr(1)
	return nil, response.NewUnack()
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Batch) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Batch) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
