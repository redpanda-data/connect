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
	constructors["select_parts"] = typeSpec{
		constructor: NewSelectParts,
		description: `
Cherry pick a set of parts from messages by their index. Indexes larger than the
number of parts are simply ignored.

The selected parts are added to the new message in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].`,
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

// ProcessMessage checks each message against a set of bounds.
func (m *SelectParts) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	m.stats.Incr("processor.select_parts.count", 1)

	newMsg := types.NewMessage()
	lParts := len(msg.Parts)
	for _, index := range m.conf.SelectParts.Parts {
		if index < lParts {
			m.stats.Incr("processor.select_parts.selected", 1)
			newMsg.Parts = append(newMsg.Parts, msg.Parts[index])
		} else {
			m.stats.Incr("processor.select_parts.skipped", 1)
		}
	}

	return &newMsg, nil, true
}

//------------------------------------------------------------------------------
