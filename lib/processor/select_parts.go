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
	Constructors["select_parts"] = TypeSpec{
		constructor: NewSelectParts,
		description: `
Cherry pick a set of parts from messages by their index. Indexes larger than the
number of parts are simply ignored.

The selected parts are added to the new message in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].

If none of the selected parts exist in the input message (resulting in an empty
output message) the message is dropped entirely.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// SelectPartsConfig contains any configuration for the SelectParts processor.
type SelectPartsConfig struct {
	Parts []int `json:"parts" yaml:"parts"`
}

// NewSelectPartsConfig returns a SelectPartsConfig with default values.
func NewSelectPartsConfig() SelectPartsConfig {
	return SelectPartsConfig{
		Parts: []int{0},
	}
}

//------------------------------------------------------------------------------

// SelectParts is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type SelectParts struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSelected  metrics.StatCounter
	mDropped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewSelectParts returns a SelectParts processor.
func NewSelectParts(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &SelectParts{
		conf:  conf,
		log:   log.NewModule(".processor.select_parts"),
		stats: stats,

		mCount:     stats.GetCounter("processor.select_parts.count"),
		mSkipped:   stats.GetCounter("processor.select_parts.skipped"),
		mSelected:  stats.GetCounter("processor.select_parts.selected"),
		mDropped:   stats.GetCounter("processor.select_parts.dropped"),
		mSent:      stats.GetCounter("processor.select_parts.sent"),
		mSentParts: stats.GetCounter("processor.select_parts.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage extracts a set of parts from each message.
func (m *SelectParts) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.mCount.Incr(1)

	newMsg := types.NewMessage(nil)
	lParts := msg.Len()
	for _, index := range m.conf.SelectParts.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			m.mSkipped.Incr(1)
		} else {
			m.mSelected.Incr(1)
			newMsg.Append(msg.Get(index))
		}
	}

	if newMsg.Len() == 0 {
		m.mDropped.Incr(1)
		return nil, types.NewSimpleResponse(nil)
	}

	m.mSent.Incr(1)
	m.mSentParts.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
