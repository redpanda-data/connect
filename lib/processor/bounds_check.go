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
	constructors["bounds_check"] = typeSpec{
		constructor: NewBoundsCheck,
		description: `
Checks whether each message fits within certain boundaries, and drops messages
that do not (log warning message and a metric).`,
	}
}

//------------------------------------------------------------------------------

// BoundsCheckConfig contains any bounds configuration for the BoundsCheck
// processor.
type BoundsCheckConfig struct {
	MaxParts    int `json:"max_parts" yaml:"max_parts"`
	MinParts    int `json:"min_parts" yaml:"min_parts"`
	MaxPartSize int `json:"max_part_size" yaml:"max_part_size"`
}

// NewBoundsCheckConfig returns a BoundsCheckConfig with default values.
func NewBoundsCheckConfig() BoundsCheckConfig {
	return BoundsCheckConfig{
		MaxParts:    100,
		MinParts:    1,
		MaxPartSize: 4 * 1024 * 1024 * 1024, // 4GB
	}
}

//------------------------------------------------------------------------------

// BoundsCheck is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type BoundsCheck struct {
	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewBoundsCheck returns a BoundsCheck processor.
func NewBoundsCheck(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return &BoundsCheck{
		conf:  conf,
		log:   log.NewModule(".processor.bounds_check"),
		stats: stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (m *BoundsCheck) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	m.stats.Incr("processor.bounds_check.count", 1)
	lParts := len(msg.Parts)
	if lParts < m.conf.BoundsCheck.MinParts {
		m.log.Warnf(
			"Rejecting message due to message parts below minimum (%v): %v\n",
			m.conf.BoundsCheck.MinParts, lParts,
		)
		m.stats.Incr("processor.bounds_check.dropped", 1)
		m.stats.Incr("processor.bounds_check.dropped_empty", 1)
		return nil, types.NewSimpleResponse(nil), false
	} else if lParts > m.conf.BoundsCheck.MaxParts {
		m.log.Warnf(
			"Rejecting message due to message parts exceeding limit (%v): %v\n",
			m.conf.BoundsCheck.MaxParts, lParts,
		)
		m.stats.Incr("processor.bounds_check.dropped", 1)
		m.stats.Incr("processor.bounds_check.dropped_num_parts", 1)
		return nil, types.NewSimpleResponse(nil), false
	}
	for _, part := range msg.Parts {
		if size := len(part); size > m.conf.BoundsCheck.MaxPartSize {
			m.log.Warnf(
				"Rejecting message due to message part size exceeding limit (%v): %v\n",
				m.conf.BoundsCheck.MaxPartSize, size,
			)
			m.stats.Incr("processor.bounds_check.dropped", 1)
			m.stats.Incr("processor.bounds_check.dropped_part_size", 1)
			return nil, types.NewSimpleResponse(nil), false
		}
	}
	return msg, nil, true
}

//------------------------------------------------------------------------------
