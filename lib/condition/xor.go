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
	Constructors[TypeXor] = TypeSpec{
		constructor: NewXor,
		description: `
Xor is a condition that returns the logical XOR of its children conditions,
meaning it only resolves to true if _exactly_ one of its children conditions
resolves to true.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.Xor))
			for i, cConf := range conf.Xor {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// XorConfig is a configuration struct containing fields for the Xor condition.
type XorConfig []Config

// NewXorConfig returns a XorConfig with default values.
func NewXorConfig() XorConfig {
	return XorConfig{}
}

//------------------------------------------------------------------------------

// Xor is a condition that returns the logical xor of all children.
type Xor struct {
	children []Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewXor returns an Xor condition.
func NewXor(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for i, childConf := range conf.Xor {
		ns := fmt.Sprintf("%v", i)
		child, err := New(childConf, mgr, log.NewModule("."+ns), metrics.Namespaced(stats, ns))
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &Xor{
		children: children,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Xor) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	hadTrue := false
	for _, child := range c.children {
		if child.Check(msg) {
			if hadTrue {
				c.mFalse.Incr(1)
				return false
			}
			hadTrue = true
		}
	}
	if hadTrue {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return hadTrue
}

//------------------------------------------------------------------------------
