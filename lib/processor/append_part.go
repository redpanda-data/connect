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
	constructors["append_part"] = typeSpec{
		constructor: NewAppendPart,
		description: `
Insert a new message part at the end of the message. This will become the last
part of the resultant message.

This processor will interpolate functions within the 'content' field.`,
	}
}

//------------------------------------------------------------------------------

// AppendPartConfig contains any configuration for the AppendPart processor.
type AppendPartConfig struct {
	Content string `json:"content" yaml:"content"`
}

// NewAppendPartConfig returns a AppendPartConfig with default values.
func NewAppendPartConfig() AppendPartConfig {
	return AppendPartConfig{
		Content: "",
	}
}

//------------------------------------------------------------------------------

// AppendPart is a processor that adds a new message part to the end of a
// message.
type AppendPart struct {
	interpolate bool
	part        []byte

	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewAppendPart returns a AppendPart processor.
func NewAppendPart(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	part := []byte(conf.AppendPart.Content)
	interpolate := text.ContainsSpecialVariables(part)
	return &AppendPart{
		interpolate: interpolate,
		part:        part,
		conf:        conf,
		log:         log.NewModule(".processor.append_part"),
		stats:       stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *AppendPart) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	p.stats.Incr("processor.append_part.count", 1)

	var newPart []byte
	if p.interpolate {
		newPart = text.ReplaceSpecialVariables(p.part)
	} else {
		newPart = p.part
	}
	msg.Parts = append(msg.Parts, newPart)

	return msg, nil, true
}

//------------------------------------------------------------------------------
