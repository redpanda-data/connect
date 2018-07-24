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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
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
  parts: [ 0 ]
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
` + "[`jmespath`](../conditions/README.md#jmespath)" + ` condition.`,
	}
}

//------------------------------------------------------------------------------

// JMESPathConfig contains any configuration for the JMESPath processor.
type JMESPathConfig struct {
	Parts []int  `json:"parts" yaml:"parts"`
	Query string `json:"query" yaml:"query"`
}

// NewJMESPathConfig returns a JMESPathConfig with default values.
func NewJMESPathConfig() JMESPathConfig {
	return JMESPathConfig{
		Parts: []int{},
		Query: "",
	}
}

//------------------------------------------------------------------------------

// JMESPath is a processor that executes JMESPath queries on a message part and
// replaces the contents with the result.
type JMESPath struct {
	parts []int
	query *jmespath.JMESPath

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErrJSONP  metrics.StatCounter
	mErrJMES   metrics.StatCounter
	mErrJSONS  metrics.StatCounter
	mSucc      metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
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
		parts: conf.JMESPath.Parts,
		query: query,
		conf:  conf,
		log:   log.NewModule(".processor.jmespath"),
		stats: stats,

		mCount:     stats.GetCounter("processor.jmespath.count"),
		mErrJSONP:  stats.GetCounter("processor.jmespath.error.json_parse"),
		mErrJMES:   stats.GetCounter("processor.jmespath.error.jmespath_search"),
		mErrJSONS:  stats.GetCounter("processor.jmespath.error.json_set"),
		mSucc:      stats.GetCounter("processor.jmespath.success"),
		mSent:      stats.GetCounter("processor.jmespath.sent"),
		mSentParts: stats.GetCounter("processor.jmespath.parts.sent"),
	}
	return j, nil
}

//------------------------------------------------------------------------------

func safeSearch(part interface{}, j *jmespath.JMESPath) (res interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jmespath panic: %v", r)
		}
	}()
	return j.Search(part)
}

// ProcessMessage prepends a new message part to the message.
func (p *JMESPath) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	p.mCount.Incr(1)

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
			p.mErrJSONP.Incr(1)
			p.log.Debugf("Failed to parse part into json: %v\n", err)
			continue
		}

		var result interface{}
		if result, err = safeSearch(jsonPart, p.query); err != nil {
			p.mErrJMES.Incr(1)
			p.log.Debugf("Failed to search json: %v\n", err)
			continue
		}

		if err = newMsg.SetJSON(index, result); err != nil {
			p.mErrJSONS.Incr(1)
			p.log.Debugf("Failed to convert jmespath result into part: %v\n", err)
		} else {
			p.mSucc.Incr(1)
		}
	}

	msgs := [1]types.Message{newMsg}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(newMsg.Len()))
	return msgs[:], nil
}

//------------------------------------------------------------------------------
