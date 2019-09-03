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
	Constructors[TypeOr] = TypeSpec{
		constructor: NewOr,
		description: `
Or is a condition that returns the logical OR of its children conditions.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.Or))
			for i, cConf := range conf.Or {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// OrConfig is a configuration struct containing fields for the Or condition.
type OrConfig []Config

// NewOrConfig returns a OrConfig with default values.
func NewOrConfig() OrConfig {
	return OrConfig{}
}

//------------------------------------------------------------------------------

// Or is a condition that returns the logical or of all children.
type Or struct {
	children []Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewOr returns an Or condition.
func NewOr(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for i, childConf := range conf.Or {
		ns := fmt.Sprintf("%v", i)
		child, err := New(childConf, mgr, log.NewModule("."+ns), metrics.Namespaced(stats, ns))
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &Or{
		children: children,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Or) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	for _, child := range c.children {
		if child.Check(msg) {
			c.mTrue.Incr(1)
			return true
		}
	}
	c.mFalse.Incr(1)
	return false
}

//------------------------------------------------------------------------------
