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

package processor

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNumber] = TypeSpec{
		constructor: NewNumber,
		description: `
Parses message contents into a 64-bit floating point number and performs an
operator on it. In order to execute this processor on a sub field of a document
use it with the ` + "[`process_field`](#process_field)" + ` processor.

The value field can either be a number or a string type. If it is a string type
then this processor will interpolate functions within it, you can find a list of
functions [here](../config_interpolation.md#functions).

For example, if we wanted to subtract the current unix timestamp from the field
'foo' of a JSON document ` + "`{\"foo\":1561219142}`" + ` we could use the
following config:

` + "``` yaml" + `
process_field:
  path: foo
  result_type: float
  processors:
  - number:
      operator: subtract
      value: "${!timestamp_unix}"
` + "```" + `

Value interpolations are resolved once per message batch, in order to resolve it
for each message of the batch place it within a
` + "[`for_each`](#for_each)" + ` processor.

### Operators

#### ` + "`add`" + `

Adds a value.

#### ` + "`subtract`" + `

Subtracts a value.`,
	}
}

//------------------------------------------------------------------------------

// NumberConfig contains configuration fields for the Number processor.
type NumberConfig struct {
	Parts    []int       `json:"parts" yaml:"parts"`
	Operator string      `json:"operator" yaml:"operator"`
	Value    interface{} `json:"value" yaml:"value"`
}

// NewNumberConfig returns a NumberConfig with default values.
func NewNumberConfig() NumberConfig {
	return NumberConfig{
		Parts:    []int{},
		Operator: "add",
		Value:    0,
	}
}

//------------------------------------------------------------------------------

type numberOperator func(content, value float64) float64

func newNumberAddOperator() numberOperator {
	return func(content, value float64) float64 {
		return content + value
	}
}

func newNumberSubtractOperator() numberOperator {
	return func(content, value float64) float64 {
		return content - value
	}
}

func getNumberOperator(opStr string) (numberOperator, error) {
	switch opStr {
	case "add":
		return newNumberAddOperator(), nil
	case "subtract":
		return newNumberSubtractOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

// Number is a processor that performs number based operations on payloads.
type Number struct {
	parts []int

	interpolatedValue *text.InterpolatedString
	value             float64
	operator          numberOperator

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewNumber returns a Number processor.
func NewNumber(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	n := &Number{
		parts: conf.Number.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	switch t := conf.Number.Value.(type) {
	case string:
		if text.ContainsFunctionVariables([]byte(t)) {
			n.interpolatedValue = text.NewInterpolatedString(t)
		} else {
			n.value, err = strconv.ParseFloat(t, 64)
		}
	case float64:
		n.value = t
	case int:
		n.value = float64(t)
	case json.Number:
		n.value, err = t.Float64()
	default:
		err = fmt.Errorf("value type '%T' not allowed", t)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse value: %v", err)
	}

	if n.operator, err = getNumberOperator(conf.Number.Operator); err != nil {
		return nil, err
	}
	return n, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (n *Number) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	n.mCount.Incr(1)
	newMsg := msg.Copy()

	value := n.value
	if n.interpolatedValue != nil {
		interpStr := n.interpolatedValue.Get(msg)
		var err error
		if value, err = strconv.ParseFloat(interpStr, 64); err != nil {
			n.log.Errorf("Failed to parse interpolated value '%v' into float: %v\n", interpStr, err)
			newMsg.Iter(func(i int, p types.Part) error {
				FlagErr(p, err)
				return nil
			})

			n.mBatchSent.Incr(1)
			n.mSent.Incr(int64(newMsg.Len()))
			return []types.Message{newMsg}, nil
		}
	}

	proc := func(index int, span opentracing.Span, part types.Part) error {
		data, err := strconv.ParseFloat(string(part.Get()), 64)
		if err != nil {
			n.mErr.Incr(1)
			n.log.Debugf("Failed to parse content into float: %v\n", err)
			return err
		}
		data = n.operator(data, value)
		part.Set([]byte(strconv.FormatFloat(data, 'f', -1, 64)))
		return nil
	}

	IteratePartsWithSpan(TypeNumber, n.parts, newMsg, proc)

	n.mBatchSent.Incr(1)
	n.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (n *Number) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (n *Number) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
