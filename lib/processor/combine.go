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
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["combine"] = TypeSpec{
		constructor: NewCombine,
		description: `
If a message queue contains multiple part messages as individual parts it can
be useful to 'squash' them back into a single message. We can then push it
through a protocol that natively supports multiple part messages.

For example, if we started with N messages each containing M parts, pushed those
messages into Kafka by splitting the parts. We could now consume our N*M
messages from Kafka and squash them back into M part messages with the combine
processor, and then subsequently push them into something like ZMQ.

If a message received has more parts than the 'combine' amount it will be sent
unchanged with its original parts. This occurs even if there are cached parts
waiting to be combined, which will change the ordering of message parts through
the platform.

When a message part is received that increases the total cached number of parts
beyond the threshold it will have _all_ of its parts appended to the resuling
message. E.g. if you set the threshold at 4 and send a message of 2 parts
followed by a message of 3 parts then you will receive one output message of 5
parts.`,
	}
}

//------------------------------------------------------------------------------

// CombineConfig contains configuration for the Combine processor.
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

// Combine is a processor that takes messages with a single part in a
// benthos multiple part blob format and decodes them into multiple part
// messages.
type Combine struct {
	log   log.Modular
	stats metrics.Type
	n     int
	parts [][]byte

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
	return &Combine{
		log:   log.NewModule(".processor.combine"),
		stats: stats,
		n:     conf.Combine.Parts,

		mCount:     stats.GetCounter("processor.combine.count"),
		mWarnParts: stats.GetCounter("processor.combine.warning.too_many_parts"),
		mSent:      stats.GetCounter("processor.combine.sent"),
		mSentParts: stats.GetCounter("processor.combine.parts.sent"),
		mDropped:   stats.GetCounter("processor.combine.dropped"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a single message and buffers it, drops it, returning a
// NoAck response, until eventually it has N buffered messages, at which point
// it combines those messages into one multiple part message which is sent on.
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
	for _, part := range msg.GetAll() {
		c.parts = append(c.parts, part)
	}

	// If we have reached our target count of parts in the buffer.
	if len(c.parts) >= c.n {
		newMsg := types.NewMessage(c.parts)
		c.parts = nil

		c.mSent.Incr(1)
		c.mSentParts.Incr(int64(newMsg.Len()))
		c.log.Traceln("Batching based on parts")
		msgs := [1]types.Message{newMsg}
		return msgs[:], nil
	}

	c.log.Traceln("Added message to pending batch")
	c.mDropped.Incr(1)
	return nil, types.NewUnacknowledgedResponse()
}

//------------------------------------------------------------------------------
