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
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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
}

// NewSelectParts returns a SelectParts processor.
func NewSelectParts(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return &SelectParts{
		conf:  conf,
		log:   log.NewModule(".processor.select_parts"),
		stats: stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage extracts a set of parts from each message.
func (m *SelectParts) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.stats.Incr("processor.select_parts.count", 1)

	newMsg := types.NewMessage()
	lParts := len(msg.Parts)
	for _, index := range m.conf.SelectParts.Parts {
		if index < 0 {
			// Negative indexes count backwards from the end.
			index = lParts + index
		}

		// Check boundary of part index.
		if index < 0 || index >= lParts {
			m.stats.Incr("processor.select_parts.skipped", 1)
		} else {
			m.stats.Incr("processor.select_parts.selected", 1)
			newMsg.Parts = append(newMsg.Parts, msg.Parts[index])
		}
	}

	if len(newMsg.Parts) == 0 {
		m.stats.Incr("processor.select_parts.dropped", 1)
		return nil, types.NewSimpleResponse(nil)
	}

	m.stats.Incr("processor.select_parts.sent", 1)
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
