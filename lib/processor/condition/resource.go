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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["resource"] = TypeSpec{
		constructor: NewResource,
		description: `
Resource is a condition type that runs a condition resource by its name. This
condition allows you to run the same configured condition resource in multiple
processors, or as a branch of another condition.

For example, let's imagine we have two outputs, one of which only receives
messages that satisfy a condition and the other receives the logical NOT of that
same condition. In this example we can save ourselves the trouble of configuring
the same condition twice by referring to it as a resource, like this:

` + "``` yaml" + `
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        processors:
        - type: filter
          filter:
            type: resource
            resource: foobar
    - type: bar
      bar:
        processors:
        - type: filter
		  filter:
            type: not
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      type: text
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
` + "```" + `

It is also worth noting that when conditions are used as resources in this way
they will only be executed once per message, regardless of how many times they
are referenced (unless the content is modified). Therefore, resource conditions
can act as a runtime optimisation as well as a config optimisation.`,
	}
}

//------------------------------------------------------------------------------

// Resource is a condition that returns the result of a condition resource.
type Resource struct {
	mgr  types.Manager
	name string
	log  log.Modular
}

// NewResource returns a resource processor.
func NewResource(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if _, err := mgr.GetCondition(conf.Resource); err != nil {
		return nil, fmt.Errorf("failed to obtain condition resource '%v': %v", conf.Resource, err)
	}
	return &Resource{
		mgr:  mgr,
		name: conf.Resource,
		log:  log,
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Resource) Check(msg types.Message) bool {
	cond, err := c.mgr.GetCondition(c.name)
	if err != nil {
		c.log.Debugf("Failed to obtain condition resource '%v': %v", c.name, err)
		return false
	}
	return msg.LazyCondition(c.name, cond)
}

//------------------------------------------------------------------------------
