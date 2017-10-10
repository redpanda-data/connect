// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"errors"

	"github.com/jeffail/benthos/lib/broker"
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

var (
	// ErrRoundRobinNoOutputs is returned when creating a RoundRobin type with
	// zero outputs.
	ErrRoundRobinNoOutputs = errors.New("attempting to create round_robin output with no outputs")
)

//------------------------------------------------------------------------------

func init() {
	constructors["round_robin"] = typeSpec{
		constructor: NewRoundRobin,
		description: `
The 'round_robin' output type allows you to send messages across multiple
outputs, where each message is sent to exactly one output following a strict
order.

If an output applies back pressure this will also block other outputs from
receiving content.`,
	}
}

//------------------------------------------------------------------------------

// RoundRobinConfig is configuration for the RoundRobin output type.
type RoundRobinConfig struct {
	Outputs []interface{} `json:"outputs" yaml:"outputs"`
}

// NewRoundRobinConfig creates a new RoundRobinConfig with default values.
func NewRoundRobinConfig() RoundRobinConfig {
	return RoundRobinConfig{
		Outputs: []interface{}{},
	}
}

//------------------------------------------------------------------------------

// NewRoundRobin creates a new RoundRobin output type. Messages will be sent out
// to an output chosen by following their original order. If an output blocks
// this will block all throughput.
func NewRoundRobin(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.RoundRobin.Outputs) == 0 {
		return nil, ErrRoundRobinNoOutputs
	}

	outputConfs, err := parseOutputConfsWithDefaults(conf.RoundRobin.Outputs)
	if err != nil {
		return nil, err
	}

	outputs := make([]types.Consumer, len(outputConfs))

	for i, oConf := range outputConfs {
		outputs[i], err = New(oConf, log, stats)
		if err != nil {
			return nil, err
		}
	}

	return broker.NewRoundRobin(outputs, stats)
}

//------------------------------------------------------------------------------
