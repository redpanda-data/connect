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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeInsertPart] = TypeSpec{
		constructor: NewInsertPart,
		description: `
Insert a new message into a batch at an index. If the specified index is greater
than the length of the existing batch it will be appended to the end.

The index can be negative, and if so the message will be inserted from the end
counting backwards starting from -1. E.g. if index = -1 then the new message
will become the last of the batch, if index = -2 then the new message will be
inserted before the last message, and so on. If the negative index is greater
than the length of the existing batch it will be inserted at the beginning.

The new message will have metadata copied from the first pre-existing message of
the batch.

This processor will interpolate functions within the 'content' field, you can
find a list of functions [here](../config_interpolation.md#functions).`,
	}
}

//------------------------------------------------------------------------------

// InsertPartConfig contains configuration fields for the InsertPart processor.
type InsertPartConfig struct {
	Index   int    `json:"index" yaml:"index"`
	Content string `json:"content" yaml:"content"`
}

// NewInsertPartConfig returns a InsertPartConfig with default values.
func NewInsertPartConfig() InsertPartConfig {
	return InsertPartConfig{
		Index:   -1,
		Content: "",
	}
}

//------------------------------------------------------------------------------

// InsertPart is a processor that inserts a new message part at a specific
// index.
type InsertPart struct {
	interpolate bool
	part        []byte

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewInsertPart returns a InsertPart processor.
func NewInsertPart(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	part := []byte(conf.InsertPart.Content)
	interpolate := text.ContainsFunctionVariables(part)
	return &InsertPart{
		part:        part,
		interpolate: interpolate,
		conf:        conf,
		log:         log,
		stats:       stats,

		mCount:     stats.GetCounter("count"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (p *InsertPart) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

	var newPartBytes []byte
	if p.interpolate {
		newPartBytes = text.ReplaceFunctionVariables(msg, p.part)
	} else {
		newPartBytes = p.part
	}

	index := p.conf.InsertPart.Index
	msgLen := msg.Len()
	if index < 0 {
		index = msgLen + index + 1
		if index < 0 {
			index = 0
		}
	} else if index > msgLen {
		index = msgLen
	}

	newMsg := message.New(nil)
	newPart := msg.Get(0).Copy()
	newPart.Set(newPartBytes)
	msg.Iter(func(i int, p types.Part) error {
		if i == index {
			newMsg.Append(newPart)
		}
		newMsg.Append(p.Copy())
		return nil
	})
	if index == msg.Len() {
		newMsg.Append(newPart)
	}

	p.mBatchSent.Incr(1)
	p.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (p *InsertPart) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (p *InsertPart) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
