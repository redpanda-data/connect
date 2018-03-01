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
	"fmt"

	"github.com/Jeffail/benthos/lib/broker"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

var (
	// ErrBrokerNoOutputs is returned when creating a Broker type with zero
	// outputs.
	ErrBrokerNoOutputs = errors.New("attempting to create broker output type with no outputs")
)

//------------------------------------------------------------------------------

func init() {
	constructors["broker"] = typeSpec{
		constructor: NewBroker,
		description: `
The broker output type allows you to configure multiple output targets following
a broker pattern from this list:

### ` + "`fan_out`" + `

With the fan out pattern all outputs will be sent every message that passes
through Benthos. If an output applies back pressure it will block all subsequent
messages, and if an output fails to send a message it will be retried
continuously until completion or service shut down.

### ` + "`round_robin`" + `

With the round robin pattern each message will be assigned a single output
following their order. If an output applies back pressure it will block all
subsequent messages. If an output fails to send a message then the message will
be re-attempted with the next input, and so on.

### ` + "`greedy`" + `

The greedy pattern results in higher output throughput at the cost of
disproportionate message allocations to those outputs. Each message is sent to a
single output, and the output chosen is randomly selected only from outputs
ready to process a message. It is therefore possible for certain outputs to
receive a disproportionate number of messages depending on their throughput.`,
	}
}

//------------------------------------------------------------------------------

// BrokerConfig is configuration for the Broker output type.
type BrokerConfig struct {
	Pattern string           `json:"pattern" yaml:"pattern"`
	Outputs brokerOutputList `json:"outputs" yaml:"outputs"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		Pattern: "fan_out",
		Outputs: brokerOutputList{},
	}
}

//------------------------------------------------------------------------------

// NewBroker creates a new Broker output type. Messages will be sent out to the
// list of outputs according to the chosen broker pattern.
func NewBroker(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	outputConfs := conf.Broker.Outputs

	if len(outputConfs) == 0 {
		return nil, ErrBrokerNoOutputs
	} else if len(outputConfs) == 1 {
		return New(outputConfs[0], mgr, log, stats)
	}

	outputs := make([]types.Output, len(outputConfs))

	var err error
	for i, oConf := range outputConfs {
		outputs[i], err = New(oConf, mgr, log, stats)
		if err != nil {
			return nil, err
		}
	}

	switch conf.Broker.Pattern {
	case "fan_out":
		return broker.NewFanOut(outputs, log, stats)
	case "round_robin":
		return broker.NewRoundRobin(outputs, stats)
	case "greedy":
		return broker.NewGreedy(outputs)
	}

	return nil, fmt.Errorf("broker pattern was not recognised: %v", conf.Broker.Pattern)
}

//------------------------------------------------------------------------------
