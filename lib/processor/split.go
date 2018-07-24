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
	Constructors["split"] = TypeSpec{
		constructor: NewSplit,
		description: `
Extracts the individual parts of a multipart message and turns them each into a
unique message. It is NOT necessary to use the split processor when your output
only supports single part messages, since those message parts will automatically
be sent as individual messages.

Please note that when you split a message you will lose the coupling between the
acknowledgement from the output destination to the origin message at the input
source. If all but one part of a split message is successfully propagated to the
destination the source will still see an error and may attempt to resend the
entire message again.

The split operator is useful for breaking down messages containing a large
number of parts into smaller batches by using the split processor followed by
the combine processor. For example:

1 Message of 1000 parts -> Split -> Combine 10 -> 100 Messages of 10 parts.`,
	}
}

//------------------------------------------------------------------------------

// Split is a processor that splits messages into a message per part.
type Split struct {
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewSplit returns a Split processor.
func NewSplit(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Split{
		log:   log.NewModule(".processor.split"),
		stats: stats,

		mCount:     stats.GetCounter("processor.split.count"),
		mDropped:   stats.GetCounter("processor.split.dropped"),
		mSent:      stats.GetCounter("processor.split.sent"),
		mSentParts: stats.GetCounter("processor.split.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a single message and returns a slice of messages,
// containing a message per part.
func (s *Split) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)

	if msg.Len() == 0 {
		s.mDropped.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	msgs := make([]types.Message, msg.Len())
	for i, part := range msg.GetAll() {
		msgs[i] = types.NewMessage([][]byte{part})
	}

	s.mSent.Incr(int64(len(msgs)))
	s.mSentParts.Incr(int64(len(msgs)))
	return msgs, nil
}

//------------------------------------------------------------------------------
