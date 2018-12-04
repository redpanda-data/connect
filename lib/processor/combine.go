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
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCombine] = TypeSpec{
		constructor: NewCombine,
		description: `
DEPRECATED: Use the ` + "[`batch`](#batch)" + ` processor with the
` + "`count`" + ` field instead.`,
	}
}

//------------------------------------------------------------------------------

// CombineConfig contains configuration fields for the Combine processor.
type CombineConfig struct {
	Parts int `json:"parts" yaml:"parts"`
}

// NewCombineConfig returns a CombineConfig with default values.
func NewCombineConfig() CombineConfig {
	return CombineConfig{
		Parts: 2,
	}
}

//------------------------------------------------------------------------------

// Combine is a processor that combines messages into a batch until a target
// number of message parts is reached, at which point the batch is sent out.
// When a message is combined without yet producing a batch a NoAck response is
// returned, which is interpretted as source types as an instruction to send
// another message through but hold off on acknowledging this one.
//
// Eventually, when the batch reaches its target size, the batch is sent through
// the pipeline as a single message and an acknowledgement for that message
// determines whether the whole batch of messages are acknowledged.
type Combine struct {
	log   log.Modular
	stats metrics.Type
	n     int
	parts []types.Part

	mCount     metrics.StatCounter
	mWarnParts metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
	mDropped   metrics.StatCounter
}

// NewCombine returns a Combine processor.
func NewCombine(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	log.Warnf("WARNING: The combine processor is deprecated, please use `batch` with the `count` field instead.")
	return &Combine{
		log:   log,
		stats: stats,
		n:     conf.Combine.Parts,

		mCount:     stats.GetCounter("count"),
		mWarnParts: stats.GetCounter("warning.too_many_parts"),
		mSent:      stats.GetCounter("sent"),
		mSentParts: stats.GetCounter("parts.sent"),
		mDropped:   stats.GetCounter("dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Combine) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	if msg.Len() > c.n {
		c.mWarnParts.Incr(1)
		c.mSent.Incr(1)
		c.mSentParts.Incr(int64(msg.Len()))
		msgs := [1]types.Message{msg}
		return msgs[:], nil
	}

	// Add new parts to the buffer.
	msg.Iter(func(i int, b types.Part) error {
		c.parts = append(c.parts, b.Copy())
		return nil
	})

	// If we have reached our target count of parts in the buffer.
	if len(c.parts) >= c.n {
		newMsg := message.New(nil)
		newMsg.Append(c.parts...)

		c.parts = nil

		c.mSent.Incr(1)
		c.mSentParts.Incr(int64(newMsg.Len()))
		c.log.Traceln("Batching based on parts")
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	c.log.Traceln("Added message to pending batch")
	c.mDropped.Incr(1)
	return nil, response.NewUnack()
}

//------------------------------------------------------------------------------
