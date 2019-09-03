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
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSplit] = TypeSpec{
		constructor: NewSplit,
		description: `
Breaks message batches (synonymous with multiple part messages) into smaller
batches. The size of the resulting batches are determined either by a discrete
size or, if the field ` + "`byte_size`" + ` is non-zero, then by total size in
bytes (which ever limit is reached first).

If there is a remainder of messages after splitting a batch the remainder is
also sent as a single batch. For example, if your target size was 10, and the
processor received a batch of 95 message parts, the result would be 9 batches of
10 messages followed by a batch of 5 messages.`,
	}
}

//------------------------------------------------------------------------------

// SplitConfig is a configuration struct containing fields for the Split
// processor, which breaks message batches down into batches of a smaller size.
type SplitConfig struct {
	Size     int `json:"size" yaml:"size"`
	ByteSize int `json:"byte_size" yaml:"byte_size"`
}

// NewSplitConfig returns a SplitConfig with default values.
func NewSplitConfig() SplitConfig {
	return SplitConfig{
		Size:     1,
		ByteSize: 0,
	}
}

//------------------------------------------------------------------------------

// Split is a processor that splits messages into a message per part.
type Split struct {
	log   log.Modular
	stats metrics.Type

	size     int
	byteSize int

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

		size:     conf.Split.Size,
		byteSize: conf.Split.ByteSize,

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

	nextMsg := message.New(nil)
	byteSize := 0

	msg.Iter(func(i int, p types.Part) error {
		if (s.size > 0 && nextMsg.Len() >= s.size) ||
			(s.byteSize > 0 && (byteSize+len(p.Get())) > s.byteSize) {
			if nextMsg.Len() > 0 {
				msgs = append(msgs, nextMsg)
				nextMsg = message.New(nil)
				byteSize = 0
			} else {
				s.log.Warnf("A single message exceeds the target batch byte size of '%v', actual size: '%v'", s.byteSize, len(p.Get()))
			}
		}
		nextMsg.Append(p)
		byteSize += len(p.Get())
		return nil
	})

	if nextMsg.Len() > 0 {
		msgs = append(msgs, nextMsg)
	}

	s.mBatchSent.Incr(int64(len(msgs)))
	s.mSent.Incr(int64(msg.Len()))
	return msgs, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Split) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *Split) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
