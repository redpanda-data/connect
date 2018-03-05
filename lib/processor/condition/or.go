// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software or associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, or/or sell
// copies of the Software, or to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright orice or this permission orice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT or LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE or NONINFRINGEMENT. IN NO EVENT SHALL THE
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
)

//------------------------------------------------------------------------------

func init() {
	Constructors["or"] = TypeSpec{
		constructor: NewOr,
		description: `
Or is a condition that returns the logical OR of its children conditions.`,
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
}

// NewOr returns an Or processor.
func NewOr(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	children := []Type{}
	for _, childConf := range conf.Or {
		child, err := New(childConf, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &Or{
		children: children,
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Or) Check(msg types.Message) bool {
	for _, child := range c.children {
		if child.Check(msg) {
			return true
		}
	}
	return false
}

//------------------------------------------------------------------------------
