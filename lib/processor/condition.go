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

	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["condition"] = TypeSpec{
		constructor: NewCondition,
		description: `
Tests each message against a condition, if the condition fails then the message
is dropped. You can read a [full list of conditions here](../conditions).`,
	}
}

//------------------------------------------------------------------------------

// ConditionConfig contains configuration fields for the Condition processor.
type ConditionConfig struct {
	condition.Config `json:",inline" yaml:",inline"`
}

// NewConditionConfig returns a ConditionConfig with default values.
func NewConditionConfig() ConditionConfig {
	return ConditionConfig{
		Config: condition.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Condition is a processor that checks each message against a set of bounds
// and rejects messages if they aren't within them.
type Condition struct {
	log   log.Modular
	stats metrics.Type

	condition condition.Type
}

// NewCondition returns a Condition processor.
func NewCondition(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cond, err := condition.New(conf.Condition.Config, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to construct condition '%v': %v",
			conf.Condition.Config.Type, err,
		)
	}
	return &Condition{
		log:       log.NewModule(".processor.condition"),
		stats:     stats,
		condition: cond,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage checks each message against a set of bounds.
func (c *Condition) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.stats.Incr("processor.condition.count", 1)

	if !c.condition.Check(msg) {
		c.stats.Incr("processor.condition.dropped", 1)
		return nil, types.NewSimpleResponse(nil)
	}

	c.stats.Incr("processor.condition.sent", 1)
	msgs := [1]types.Message{msg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
