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
	"sync"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCount] = TypeSpec{
		constructor: NewCount,
		description: `
Counts messages starting from one, returning true until the counter reaches its
target, at which point it will return false and reset the counter. This
condition is useful when paired with the ` + "`read_until`" + ` input, as it can
be used to cut the input stream off once a certain number of messages have been
read.

It is worth noting that each discrete count condition will have its own counter.
Parallel processors containing a count condition will therefore count
independently. It is, however, possible to share the counter across processor
pipelines by defining the count condition as a resource.`,
	}
}

//------------------------------------------------------------------------------

// CountConfig is a configuration struct containing fields for the Count
// condition.
type CountConfig struct {
	Arg int `json:"arg" yaml:"arg"`
}

// NewCountConfig returns a CountConfig with default values.
func NewCountConfig() CountConfig {
	return CountConfig{
		Arg: 100,
	}
}

//------------------------------------------------------------------------------

// Count is a condition that counts each message and returns false once a target
// count has been reached, at which point it resets the counter and starts
// again.
type Count struct {
	arg int
	ctr int

	mut sync.Mutex

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewCount returns a Count condition.
func NewCount(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return &Count{
		arg: conf.Count.Arg,
		ctr: 0,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Count) Check(msg types.Message) bool {
	c.mut.Lock()
	defer c.mut.Unlock()

	c.mCount.Incr(1)
	c.ctr++
	if c.ctr < c.arg {
		c.mFalse.Incr(1)
		return true
	}
	c.ctr = 0
	c.mTrue.Incr(1)
	return false
}

//------------------------------------------------------------------------------
