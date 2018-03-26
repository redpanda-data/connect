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

package condition

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
to it, expecting a boolean response. If the response is true the condition
passes, otherwise it does not. Please refer to the
[JMESPath website](http://jmespath.org/) for information and tutorials regarding
the syntax of expressions.

For example, with the following config:

` + "``` yaml" + `
jmespath:
  part: 0
  query: a == 'foo'
` + "```" + `

If the initial jmespaths of part 0 were:

` + "``` json" + `
{
	"a": "foo"
}
` + "```" + `

Then the condition would pass.

JMESPath is traditionally used for mutating JSON jmespath, in order to do this
please instead use the ` + "[`jmespath`](../processors/README.md#jmespath)" + `
processor instead.`,
	}
}

//------------------------------------------------------------------------------

// JMESPathConfig is a configuration struct containing fields for the jmespath
// condition.
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

// JMESPath is a condition that checks message against a jmespath query.
type JMESPath struct {
	stats metrics.Type
	log   log.Modular
	part  int
	query *jmespath.JMESPath
}

// NewJMESPath returns a JMESPath processor.
func NewJMESPath(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	query, err := jmespath.Compile(conf.JMESPath.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JMESPath query: %v", err)
	}

	return &JMESPath{
		stats: stats,
		log:   log,
		part:  conf.JMESPath.Part,
		query: query,
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *JMESPath) Check(msg types.Message) bool {
	index := c.part
	if index < 0 {
		index = msg.Len() + index
	}

	if index < 0 || index >= msg.Len() {
		c.stats.Incr("condition.jmespath.skipped", 1)
		return false
	}

	jsonPart, err := msg.GetJSON(index)
	if err != nil {
		c.stats.Incr("condition.jmespath.error.json_parse", 1)
		c.stats.Incr("condition.jmespath.dropped", 1)
		c.log.Errorf("Failed to parse part into json: %v\n", err)
		return false
	}

	var result interface{}
	if result, err = c.query.Search(jsonPart); err != nil {
		c.stats.Incr("condition.jmespath.error.jmespath_search", 1)
		c.stats.Incr("condition.jmespath.dropped", 1)
		c.log.Errorf("Failed to search json: %v\n", err)
		return false
	}
	c.stats.Incr("condition.jmespath.applied", 1)

	resultBool, _ := result.(bool)
	return resultBool
}

//------------------------------------------------------------------------------
