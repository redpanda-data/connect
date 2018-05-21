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
	"encoding/json"
	"errors"
	"strings"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["select_json"] = TypeSpec{
		constructor: NewSelectJSON,
		description: `
Parses a message part as a JSON blob and attempts to obtain a field within the
structure identified by a dot path. If found successfully the value will become
the new contents of the target message part according to its type, meaning a
string field will be unquoted, but an object/array will remain valid JSON.

For example, with the following config:

` + "``` yaml" + `
select_json:
  parts: [0]
  path: foo.bar
` + "```" + `

If the initial contents of part 0 were:

` + "``` json" + `
{"foo":{"bar":"1", "baz":"2"}}
` + "```" + `

Then the resulting contents of part 0 would be: ` + "`1`" + `. However, if the
initial contents of part 0 were:

` + "``` json" + `
{"foo":{"bar":{"baz":"1"}}}
` + "```" + `

The resulting contents of part 0 would be: ` + "`" + `{"baz":"1"}` + "`" + `

Sometimes messages are received in an enveloped form, where the real payload is
a field inside a larger JSON structure. The 'select_json' processor can extract
the payload into the message contents as a valid JSON structure in this case
even if the payload is an escaped string.

If the list of target parts is empty the processor will be applied to all
message parts. Part indexes can be negative, and if so the part will be selected
from the end counting backwards starting from -1. E.g. if part = -1 then the
selected part will be the last part of the message, if part = -2 then the part
before the last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// Errors for the SelectJSON type.
var (
	ErrEmptyTargetPath = errors.New("target path is empty")
)

//------------------------------------------------------------------------------

// SelectJSONConfig contains any configuration for the SelectJSON processor.
type SelectJSONConfig struct {
	Parts []int  `json:"parts" yaml:"parts"`
	Path  string `json:"path" yaml:"path"`
}

// NewSelectJSONConfig returns a SelectJSONConfig with default values.
func NewSelectJSONConfig() SelectJSONConfig {
	return SelectJSONConfig{
		Parts: []int{},
		Path:  "",
	}
}

//------------------------------------------------------------------------------

// SelectJSON is a processor that extracts a JSON field from a message part and
// replaces the contents with the field value.
type SelectJSON struct {
	target []string
	parts  []int

	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewSelectJSON returns a SelectJSON processor.
func NewSelectJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if len(conf.SelectJSON.Path) == 0 || conf.SelectJSON.Path == "." {
		return nil, ErrEmptyTargetPath
	}
	j := &SelectJSON{
		target: strings.Split(conf.SelectJSON.Path, "."),
		parts:  conf.SelectJSON.Parts,
		conf:   conf,
		log:    log.NewModule(".processor.select_json"),
		stats:  stats,
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *SelectJSON) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.stats.Incr("processor.select_json.count", 1)

	newMsg := msg.ShallowCopy()

	targetParts := p.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		jsonPart, err := msg.GetJSON(index)
		if err != nil {
			p.stats.Incr("processor.select_json.error.json_parse", 1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		var gPart *gabs.Container
		if gPart, err = gabs.Consume(jsonPart); err != nil {
			p.stats.Incr("processor.select_json.error.json_parse", 1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		switch t := gPart.Search(p.target...).Data().(type) {
		case string:
			newMsg.Set(index, []byte(t))
		case json.Number:
			newMsg.Set(index, []byte(t.String()))
		default:
			if err = newMsg.SetJSON(index, t); err != nil {
				p.stats.Incr("processor.select_json.error.json_set", 1)
				p.log.Debugf("Failed to convert json into part: %v\n", err)
			}
		}

		if err == nil {
			p.stats.Incr("processor.select_json.success", 1)
		}
	}

	msgs := [1]types.Message{newMsg}

	p.stats.Incr("processor.select_json.sent", 1)
	return msgs[:], nil

}

//------------------------------------------------------------------------------
