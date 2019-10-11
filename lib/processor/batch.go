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
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBatch] = TypeSpec{
		constructor: NewBatch,
		description: `
DEPRECATED: This processor is no longer supported and has been replaced with
improved batching mechanisms. For more information about batching in Benthos
please check out [this document](../batching.md).

This processor is scheduled to be removed in Benthos V4`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return batch.SanitisePolicyConfig(batch.PolicyConfig(conf.Batch))
		},
	}
}

//------------------------------------------------------------------------------

// BatchConfig contains configuration fields for the Batch processor.
type BatchConfig batch.PolicyConfig

// NewBatchConfig returns a BatchConfig with default values.
func NewBatchConfig() BatchConfig {
	return BatchConfig(batch.NewPolicyConfig())
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
//
// TODO: V4 Remove me.
type Batch struct {
	log   log.Modular
	stats metrics.Type

	policy *batch.Policy
	mut    sync.Mutex

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
	mDropped   metrics.StatCounter
}

// NewBatch returns a Batch processor.
func NewBatch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	log.Warnln("The batch processor is deprecated and is scheduled for removal in Benthos V4. For more information about batching in Benthos check out https://docs.benthos.dev/batching")
	policy, err := batch.NewPolicy(batch.PolicyConfig(conf.Batch), mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return &Batch{
		log:    log,
		stats:  stats,
		policy: policy,

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
		mDropped:   stats.GetCounter("dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Batch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)
	c.mut.Lock()
	defer c.mut.Unlock()

	var batch bool

	// Add new parts to the buffer.
	msg.Iter(func(i int, b types.Part) error {
		if c.policy.Add(b.Copy()) {
			batch = true
		}
		return nil
	})

	// If we have reached our target count of parts in the buffer.
	if batch {
		if newMsg := c.policy.Flush(); newMsg != nil {
			c.mSent.Incr(int64(newMsg.Len()))
			c.mBatchSent.Incr(1)
			return []types.Message{newMsg}, nil
		}
	}

	c.log.Traceln("Added message to pending batch")
	c.mDropped.Incr(1)
	return nil, response.NewUnack()
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Batch) CloseAsync() {
	c.mut.Lock()
	pending := c.policy.Count()
	c.mut.Unlock()
	if pending > 0 {
		c.log.Warnf("Batch processor exiting with %v unflushed message parts. The source messages will be reconsumed the next time Benthos starts.\n", pending)
	}
}

// WaitForClose blocks until the processor has closed down.
func (c *Batch) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
