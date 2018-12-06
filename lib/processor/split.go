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
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSplit] = TypeSpec{
		constructor: NewSplit,
		description: `
Breaks message batches (synonymous with multiple part messages) into smaller
batches, targeting a specific batch size of discrete message parts (default size
is 1 message.)

For each batch, if there is a remainder of parts after splitting a batch, the
remainder is also sent as a single batch. For example, if your target size was
10, and the processor received a batch of 95 message parts, the result would be
9 batches of 10 messages followed by a batch of 5 messages.

The split processor should *always* be positioned at the end of a list of
processors.`,
	}
}

//------------------------------------------------------------------------------

// SplitConfig is a configuration struct containing fields for the Split
// processor, which breaks message batches down into batches of a smaller size.
type SplitConfig struct {
	Size int `json:"size" yaml:"size"`
}

// NewSplitConfig returns a SplitConfig with default values.
func NewSplitConfig() SplitConfig {
	return SplitConfig{
		Size: 1,
	}
}

//------------------------------------------------------------------------------

// Split is a processor that splits messages into a message per part.
type Split struct {
	log   log.Modular
	stats metrics.Type

	size int

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSplit returns a Split processor.
func NewSplit(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Split{
		log:   log,
		stats: stats,

		size: conf.Split.Size,

		mCount:     stats.GetCounter("count"),
		mDropped:   stats.GetCounter("dropped"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Split) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)

	if msg.Len() == 0 {
		s.mDropped.Incr(1)
		return nil, response.NewAck()
	}

	msgs := []types.Message{}

	for i := 0; i < msg.Len(); i += s.size {
		batchSize := s.size
		if msg.Len() < (i + batchSize) {
			batchSize = msg.Len() - i
		}
		parts := make([]types.Part, batchSize)
		for j := range parts {
			parts[j] = msg.Get(i + j).Copy()
		}
		newMsg := message.New(nil)
		newMsg.SetAll(parts)
		msgs = append(msgs, newMsg)
	}

	s.mBatchSent.Incr(int64(len(msgs)))
	s.mSent.Incr(int64(msg.Len()))
	return msgs, nil
}

//------------------------------------------------------------------------------
