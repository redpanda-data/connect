// Copyright (c) 2019 Ashley Jeffs
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
	"errors"
	"fmt"
	"strconv"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNumber] = TypeSpec{
		constructor: NewNumber,
		description: `
Number is a condition that checks the contents of a message parsed as a 64-bit
floating point number against a logical operator and an argument.

It's possible to use the ` + "[`check_field`](#check_field)" + ` and
` + "[`check_interpolation`](#check_interpolation)" + ` conditions to check a
number condition against arbitrary metadata or fields of messages. For example,
you can test a number condition against the size of a message batch with:

` + "``` yaml" + `
check_interpolation:
  value: ${!batch_size}
  condition:
    number:
      operator: greater_than
      arg: 1
` + "```" + `

Available logical operators are:

### ` + "`equals`" + `

Checks whether the value equals the argument.

### ` + "`greater_than`" + `

Checks whether the value is greater than the argument. Returns false if the
value cannot be parsed as a number.

### ` + "`less_than`" + `

Checks whether the value is less than the argument. Returns false if the value
cannot be parsed as a number.`,
	}
}

//------------------------------------------------------------------------------

// Errors for the number condition.
var (
	ErrInvalidNumberOperator = errors.New("invalid number operator type")
)

// NumberConfig is a configuration struct containing fields for the number
// condition.
type NumberConfig struct {
	Operator string  `json:"operator" yaml:"operator"`
	Part     int     `json:"part" yaml:"part"`
	Arg      float64 `json:"arg" yaml:"arg"`
}

// NewNumberConfig returns a NumberConfig with default values.
func NewNumberConfig() NumberConfig {
	return NumberConfig{
		Operator: "equals",
		Part:     0,
		Arg:      0,
	}
}

//------------------------------------------------------------------------------

type numberOperator func(c float64) bool

func numberEqualsOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return arg == c
	}
}

func numberGreaterThanOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return c > arg
	}
}

func numberLessThanOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return c < arg
	}
}

func strToNumberOperator(str string, arg float64) (numberOperator, error) {
	switch str {
	case "equals":
		return numberEqualsOperator(arg), nil
	case "greater_than":
		return numberGreaterThanOperator(arg), nil
	case "less_than":
		return numberLessThanOperator(arg), nil
	}
	return nil, ErrInvalidNumberOperator
}

//------------------------------------------------------------------------------

// Number is a condition that checks messages interpretted as numbers against
// logical operators.
type Number struct {
	stats    metrics.Type
	operator numberOperator
	part     int

	log    log.Modular
	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewNumber returns a number condition.
func NewNumber(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToNumberOperator(conf.Number.Operator, conf.Number.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Number.Operator, err)
	}
	return &Number{
		stats:    stats,
		operator: op,
		part:     conf.Number.Part,

		log:    log,
		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Number) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mFalse.Incr(1)
		return false
	}

	msgPart := msg.Get(index).Get()
	if msgPart == nil {
		c.mFalse.Incr(1)
		return false
	}

	floatVal, err := strconv.ParseFloat(string(msgPart), 64)
	if err != nil {
		c.log.Debugf("Failed to parse message as number: %v\n", err)
		c.mFalse.Incr(1)
		return false
	}

	res := c.operator(floatVal)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
