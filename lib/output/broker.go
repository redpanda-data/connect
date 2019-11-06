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

	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var (
	// ErrBrokerNoOutputs is returned when creating a Broker type with zero
	// outputs.
	ErrBrokerNoOutputs = errors.New("attempting to create broker output type with no outputs")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBroker] = TypeSpec{
		brokerConstructor: NewBroker,
		description: `
The broker output type allows you to configure multiple output targets by
listing them:

` + "``` yaml" + `
output:
  broker:
    pattern: fan_out
    outputs:
    - foo:
        foo_field_1: value1
    - bar:
        bar_field_1: value2
        bar_field_2: value3
    - baz:
        baz_field_1: value4
      processors:
      # Processor only applied to messages sent to baz.
      - type: baz_processor
  processors:
  # Processor applied to messages sent to any brokered output.
  - type: some_processor
` + "```" + `

The broker pattern determines the way in which messages are allocated to outputs
and can be chosen from the following:

#### ` + "`fan_out`" + `

With the fan out pattern all outputs will be sent every message that passes
through Benthos in parallel.

If an output applies back pressure it will block all subsequent messages, and if
an output fails to send a message it will be retried continuously until
completion or service shut down.

#### ` + "`fan_out_sequential`" + `

Similar to the fan out pattern except outputs are written to sequentially,
meaning an output is only written to once the preceding output has confirmed
receipt of the same message.

#### ` + "`round_robin`" + `

With the round robin pattern each message will be assigned a single output
following their order. If an output applies back pressure it will block all
subsequent messages. If an output fails to send a message then the message will
be re-attempted with the next input, and so on.

#### ` + "`greedy`" + `

The greedy pattern results in higher output throughput at the cost of
potentially disproportionate message allocations to those outputs. Each message
is sent to a single output, which is determined by allowing outputs to claim
messages as soon as they are able to process them. This results in certain
faster outputs potentially processing more messages at the cost of slower
outputs.

#### ` + "`try`" + `

The try pattern attempts to send each message to only one output, starting from
the first output on the list. If an output attempt fails then the broker
attempts to send to the next output in the list and so on.

This pattern is useful for triggering events in the case where certain output
targets have broken. For example, if you had an output type ` + "`http_client`" + `
but wished to reroute messages whenever the endpoint becomes unreachable you
could use a try broker.

### Batching

It's possible to configure a [batch policy](../batching.md#batch-policy) with a
broker using the ` + "`batching`" + ` fields, allowing you to create batches
after your processing stages. Some inputs do not support broker based batching
and specify this in their documentation.

### Utilising More Outputs

When using brokered outputs with patterns such as round robin or greedy it is
possible to have multiple messages in-flight at the same time. In order to fully
utilise this you either need to have a greater number of input sources than
output sources [or use a buffer](../buffers/README.md).

### Processors

It is possible to configure [processors](../processors/README.md) at the broker
level, where they will be applied to _all_ child outputs, as well as on the
individual child outputs. If you have processors at both the broker level _and_
on child outputs then the broker processors will be applied _before_ the child
nodes processors.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			nestedOutputs := conf.Broker.Outputs
			outSlice := []interface{}{}
			for _, output := range nestedOutputs {
				sanOutput, err := SanitiseConfig(output)
				if err != nil {
					return nil, err
				}
				outSlice = append(outSlice, sanOutput)
			}
			batchSanit, err := batch.SanitisePolicyConfig(conf.Broker.Batching)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"copies":   conf.Broker.Copies,
				"pattern":  conf.Broker.Pattern,
				"outputs":  outSlice,
				"batching": batchSanit,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// BrokerConfig contains configuration fields for the Broker output type.
type BrokerConfig struct {
	Copies   int                `json:"copies" yaml:"copies"`
	Pattern  string             `json:"pattern" yaml:"pattern"`
	Outputs  brokerOutputList   `json:"outputs" yaml:"outputs"`
	Batching batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	batching := batch.NewPolicyConfig()
	batching.Count = 1
	return BrokerConfig{
		Copies:   1,
		Pattern:  "fan_out",
		Outputs:  brokerOutputList{},
		Batching: batching,
	}
}

//------------------------------------------------------------------------------

// NewBroker creates a new Broker output type. Messages will be sent out to the
// list of outputs according to the chosen broker pattern.
func NewBroker(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	outputConfs := conf.Broker.Outputs

	lOutputs := len(outputConfs) * conf.Broker.Copies

	if lOutputs <= 0 {
		return nil, ErrBrokerNoOutputs
	}
	if lOutputs == 1 {
		b, err := New(outputConfs[0], mgr, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
		if !conf.Broker.Batching.IsNoop() {
			policy, err := batch.NewPolicy(conf.Broker.Batching, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
			if err != nil {
				return nil, fmt.Errorf("failed to construct batch policy: %v", err)
			}
			b = NewBatcher(policy, b, log, stats)
		}
		return b, nil
	}

	outputs := make([]types.Output, lOutputs)

	_, isThreaded := map[string]struct{}{
		"round_robin": {},
		"greedy":      {},
	}[conf.Broker.Pattern]

	var err error
	for j := 0; j < conf.Broker.Copies; j++ {
		for i, oConf := range outputConfs {
			ns := fmt.Sprintf("broker.outputs.%v", i)
			var pipes []types.PipelineConstructorFunc
			if isThreaded {
				pipes = pipelines
			}
			outputs[j*len(outputConfs)+i], err = New(
				oConf, mgr,
				log.NewModule("."+ns),
				metrics.Combine(stats, metrics.Namespaced(stats, ns)),
				pipes...)
			if err != nil {
				return nil, fmt.Errorf("failed to create output '%v' type '%v': %v", i, oConf.Type, err)
			}
		}
	}

	var b Type
	switch conf.Broker.Pattern {
	case "fan_out":
		b, err = broker.NewFanOut(outputs, log, stats)
	case "fan_out_sequential":
		b, err = broker.NewFanOutSequential(outputs, log, stats)
	case "round_robin":
		b, err = broker.NewRoundRobin(outputs, stats)
	case "greedy":
		b, err = broker.NewGreedy(outputs)
	case "try":
		b, err = broker.NewTry(outputs, stats)
	default:
		return nil, fmt.Errorf("broker pattern was not recognised: %v", conf.Broker.Pattern)
	}
	if err == nil && !isThreaded {
		b, err = WrapWithPipelines(b, pipelines...)
	}

	if !conf.Broker.Batching.IsNoop() {
		policy, err := batch.NewPolicy(conf.Broker.Batching, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
		if err != nil {
			return nil, fmt.Errorf("failed to construct batch policy: %v", err)
		}
		b = NewBatcher(policy, b, log, stats)
	}
	return b, err
}

//------------------------------------------------------------------------------
