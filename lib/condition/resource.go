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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeResource] = TypeSpec{
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
  broker:
    pattern: fan_out
    outputs:
    - foo:
        processors:
        - filter:
            type: resource
            resource: foobar
    - bar:
        processors:
        - filter:
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// Resource is a condition that returns the result of a condition resource.
type Resource struct {
	mgr  types.Manager
	name string
	log  log.Modular

	mCount       metrics.StatCounter
	mTrue        metrics.StatCounter
	mFalse       metrics.StatCounter
	mErr         metrics.StatCounter
	mErrNotFound metrics.StatCounter
}

// NewResource returns a resource condition.
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

		mCount:       stats.GetCounter("count"),
		mTrue:        stats.GetCounter("true"),
		mFalse:       stats.GetCounter("false"),
		mErrNotFound: stats.GetCounter("error_not_found"),
		mErr:         stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Resource) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	cond, err := c.mgr.GetCondition(c.name)
	if err != nil {
		c.log.Debugf("Failed to obtain condition resource '%v': %v", c.name, err)
		c.mErrNotFound.Incr(1)
		c.mErr.Incr(1)
		c.mFalse.Incr(1)
		return false
	}

	res := cond.Check(msg)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
