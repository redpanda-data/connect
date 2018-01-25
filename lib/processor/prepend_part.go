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
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	constructors["prepend_part"] = typeSpec{
		constructor: NewPrependPart,
		description: `
Insert a new message part at the beginning of the message. This will become the
first part of the resultant message.

This processor will interpolate functions within the 'content' field.`,
	}
}

//------------------------------------------------------------------------------

// PrependPartConfig contains any configuration for the PrependPart processor.
type PrependPartConfig struct {
	Content string `json:"content" yaml:"content"`
}

// NewPrependPartConfig returns a PrependPartConfig with default values.
func NewPrependPartConfig() PrependPartConfig {
	return PrependPartConfig{
		Content: "",
	}
}

//------------------------------------------------------------------------------

// PrependPart is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type PrependPart struct {
	interpolate bool
	part        []byte

	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewPrependPart returns a PrependPart processor.
func NewPrependPart(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	part := []byte(conf.PrependPart.Content)
	interpolate := text.ContainsSpecialVariables(part)
	return &PrependPart{
		part:        part,
		interpolate: interpolate,
		conf:        conf,
		log:         log.NewModule(".processor.prepend_part"),
		stats:       stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *PrependPart) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	p.stats.Incr("processor.prepend_part.count", 1)

	var newPart []byte
	if p.interpolate {
		newPart = text.ReplaceSpecialVariables(p.part)
	} else {
		newPart = p.part
	}

	newParts := [][]byte{newPart}
	msg.Parts = append(newParts, msg.Parts...)

	return msg, nil, true
}

//------------------------------------------------------------------------------
