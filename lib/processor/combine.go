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
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["combine"] = typeSpec{
		constructor: NewCombine,
		description: `
If a message queue contains multiple part messages as individual parts it can
be useful to 'squash' them back into a single message. We can then push it
through a protocol that natively supports multiple part messages.

For example, if we started with N messages each containing M parts, pushed those
messages into Kafka by splitting the parts. We could now consume our N*M
messages from Kafka and squash them back into M part messages with the combine
processor, and then subsequently push them into something like ZMQ.`,
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
	n     int
	parts [][]byte
}

// NewCombine returns a Combine processor.
func NewCombine(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return &Combine{
		log: log.NewModule(".processor.combine"),
		n:   conf.Combine.Parts,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a single message and buffers it, drops it, returning a
// NoAck response, until eventually it has N buffered messages, at which point
// it combines those messages into one multiple part message which is sent on.
func (c *Combine) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	c.parts = append(c.parts, msg.Parts...)

	if len(c.parts) >= c.n {
		msg.Parts = c.parts[:c.n]
		oldParts := c.parts
		c.parts = [][]byte{}
		c.parts = append(c.parts, oldParts[c.n:]...)

		return msg, nil, true
	}

	return nil, types.NewUnacknowledgedResponse(), false
}

//------------------------------------------------------------------------------
