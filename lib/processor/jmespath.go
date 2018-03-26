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
	"fmt"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	jmespath "github.com/jmespath/go-jmespath"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["jmespath"] = TypeSpec{
		constructor: NewJMESPath,
		description: `
Parses a message part as a JSON blob and attempts to apply a JMESPath expression
to it, replacing the contents of the part with the result. Please refer to the
[JMESPath website](http://jmespath.org/) for information and tutorials regarding
the syntax of expressions.

For example, with the following config:

` + "``` yaml" + `
jmespath:
  part: 0
  query: locations[?state == 'WA'].name | sort(@) | {Cities: join(', ', @)}
` + "```" + `

If the initial contents of part 0 were:

` + "``` json" + `
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
` + "```" + `

Then the resulting contents of part 0 would be:

` + "``` json" + `
{"Cities": "Bellevue, Olympia, Seattle"}
` + "```" + `

It is possible to create boolean queries with JMESPath, in order to filter
messages with boolean queries please instead use the
` + "[`jmespath`](../conditions/README.md#jmespath)" + ` condition instead.

The part index can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// JMESPathConfig contains any configuration for the JMESPath processor.
type JMESPathConfig struct {
	Part  int    `json:"part" yaml:"part"`
	Query string `json:"query" yaml:"query"`
}

// NewJMESPathConfig returns a JMESPathConfig with default values.
func NewJMESPathConfig() JMESPathConfig {
	return JMESPathConfig{
		Part:  0,
		Query: "",
	}
}

//------------------------------------------------------------------------------

// JMESPath is a processor that executes JMESPath queries on a message part and
// replaces the contents with the result.
type JMESPath struct {
	part  int
	query *jmespath.JMESPath

	conf  Config
	log   log.Modular
	stats metrics.Type
}

// NewJMESPath returns a JMESPath processor.
func NewJMESPath(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	query, err := jmespath.Compile(conf.JMESPath.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JMESPath query: %v", err)
	}
	j := &JMESPath{
		part:  conf.JMESPath.Part,
		query: query,
		conf:  conf,
		log:   log.NewModule(".processor.jmespath"),
		stats: stats,
	}
	return j, nil
}

//------------------------------------------------------------------------------

// ProcessMessage prepends a new message part to the message.
func (p *JMESPath) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.stats.Incr("processor.jmespath.count", 1)

	msgs := [1]types.Message{msg}

	index := p.part
	if index < 0 {
		index = msg.Len() + index
	}

	if index < 0 || index >= msg.Len() {
		p.stats.Incr("processor.jmespath.skipped", 1)
		p.stats.Incr("processor.jmespath.dropped", 1)
		return msgs[:], nil
	}

	jsonPart, err := msg.GetJSON(index)
	if err != nil {
		p.stats.Incr("processor.jmespath.error.json_parse", 1)
		p.stats.Incr("processor.jmespath.dropped", 1)
		p.log.Errorf("Failed to parse part into json: %v\n", err)
		return msgs[:], nil
	}

	var result interface{}
	if result, err = p.query.Search(jsonPart); err != nil {
		p.stats.Incr("processor.jmespath.error.jmespath_search", 1)
		p.stats.Incr("processor.jmespath.dropped", 1)
		p.log.Errorf("Failed to search json: %v\n", err)
		return msgs[:], nil
	}

	newMsg := msg.ShallowCopy()
	msgs[0] = newMsg

	if err = newMsg.SetJSON(index, result); err != nil {
		p.stats.Incr("processor.jmespath.error.json_set", 1)
		p.log.Errorf("Failed to convert jmespath result into part: %v\n", err)
	}

	p.stats.Incr("processor.jmespath.sent", 1)
	return msgs[:], nil
}

//------------------------------------------------------------------------------
