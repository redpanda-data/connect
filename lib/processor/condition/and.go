// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright andice and this permission andice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT and LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
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
	Constructors["and"] = TypeSpec{
		constructor: NewAnd,
		description: `
And is a condition that returns the logical AND of its children conditions.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.And))
			for i, cConf := range conf.And {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// AndConfig is a configuration struct containing fields for the And condition.
type AndConfig []Config

// NewAndConfig returns a AndConfig with default values.
func NewAndConfig() AndConfig {
	return AndConfig{}
}

//------------------------------------------------------------------------------

// And is a condition that returns the logical AND of all children.
type And struct {
	children []Type
}

// NewAnd returns an And processor.
func NewAnd(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for _, childConf := range conf.And {
		child, err := New(childConf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &And{
		children: children,
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *And) Check(msg types.Message) bool {
	for _, child := range c.children {
		if !child.Check(msg) {
			return false
		}
	}
	return true
}

//------------------------------------------------------------------------------
