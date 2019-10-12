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

package input

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var (
	// ErrBrokerNoInputs is returned when creating a broker with zero inputs.
	ErrBrokerNoInputs = errors.New("attempting to create broker input type with no inputs")
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBroker] = TypeSpec{
		brokerConstructor:                  NewBroker,
		brokerConstructorHasBatchProcessor: newBrokerHasBatchProcessor,
		description: `
The broker type allows you to combine multiple inputs, where each input will be
read in parallel. A broker type is configured with its own list of input
configurations and a field to specify how many copies of the list of inputs
should be created.

Adding more input types allows you to merge streams from multiple sources into
one. For example, reading from both RabbitMQ and Kafka:

` + "``` yaml" + `
input:
  broker:
    copies: 1
    inputs:
    - amqp:
        url: amqp://guest:guest@localhost:5672/
        consumer_tag: benthos-consumer
        queue: benthos-queue
    - kafka:
        addresses:
        - localhost:9092
        client_id: benthos_kafka_input
        consumer_group: benthos_consumer_group
        partition: 0
        topic: benthos_stream
` + "```" + `

If the number of copies is greater than zero the list will be copied that number
of times. For example, if your inputs were of type foo and bar, with 'copies'
set to '2', you would end up with two 'foo' inputs and two 'bar' inputs.

### Batching

It's possible to configure a [batch policy](../batching.md#batch-policy) with a
broker using the ` + "`batching`" + ` fields. When doing this the feeds from all
child inputs are combined. Some inputs do not support broker based batching and
specify this in their documentation.

### Processors

It is possible to configure [processors](../processors/README.md) at the broker
level, where they will be applied to _all_ child inputs, as well as on the
individual child inputs. If you have processors at both the broker level _and_
on child inputs then the broker processors will be applied _after_ the child
nodes processors.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			inSlice := []interface{}{}
			for _, input := range conf.Broker.Inputs {
				var sanInput interface{}
				if sanInput, err = SanitiseConfig(input); err != nil {
					return nil, err
				}
				inSlice = append(inSlice, sanInput)
			}
			var batchSanit interface{}
			if batchSanit, err = batch.SanitisePolicyConfig(conf.Broker.Batching); err != nil {
				return nil, err
			}
			return map[string]interface{}{
				"copies":   conf.Broker.Copies,
				"inputs":   inSlice,
				"batching": batchSanit,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// BrokerConfig contains configuration fields for the Broker input type.
type BrokerConfig struct {
	Copies   int                `json:"copies" yaml:"copies"`
	Inputs   brokerInputList    `json:"inputs" yaml:"inputs"`
	Batching batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewBrokerConfig creates a new BrokerConfig with default values.
func NewBrokerConfig() BrokerConfig {
	batching := batch.NewPolicyConfig()
	batching.Count = 1
	return BrokerConfig{
		Copies:   1,
		Inputs:   brokerInputList{},
		Batching: batching,
	}
}

//------------------------------------------------------------------------------

type brokerInputList []Config

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerInputList) UnmarshalJSON(bytes []byte) error {
	genericInputs := []interface{}{}
	if err := json.Unmarshal(bytes, &genericInputs); err != nil {
		return err
	}

	inputConfs, err := parseInputConfsWithDefaults(genericInputs)
	if err != nil {
		return err
	}

	*b = inputConfs
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (b *brokerInputList) UnmarshalYAML(unmarshal func(interface{}) error) error {
	genericInputs := []interface{}{}
	if err := unmarshal(&genericInputs); err != nil {
		return err
	}

	inputConfs, err := parseInputConfsWithDefaults(genericInputs)
	if err != nil {
		return err
	}

	*b = inputConfs
	return nil
}

//------------------------------------------------------------------------------

// parseInputConfsWithDefaults takes a slice of generic input configs and
// returns a slice of input configs with default values in place of omitted
// values. This is necessary because when unmarshalling config files using
// structs you can pre-populate non-reference type struct fields with default
// values, but array objects will lose those defaults.
//
// In order to ensure that omitted values are set to default we initially parse
// the array as interface{} types and then individually apply the defaults by
// marshalling and unmarshalling. The correct way to do this would be to use
// json.RawMessage, but our config files can contain a range of different
// formats that we do not know at this stage, therefore we use the more hacky
// method as performance is not an issue at this stage.
func parseInputConfsWithDefaults(rawInputs []interface{}) ([]Config, error) {
	inputConfs := []Config{}

	// NOTE: Use yaml here as it supports more types than JSON
	// (map[interface{}]interface{}).
	for i, boxedConfig := range rawInputs {
		newConfigs := make([]Config, 1)
		label := broker.GetGenericType(boxedConfig)

		if i > 0 && strings.Index(label, "ditto") == 0 {
			broker.RemoveGenericType(boxedConfig)

			// Check if there is a ditto multiplier.
			if len(label) > 5 && label[5] == '_' {
				if label[6:] == "0" {
					// This is a special case where we are expressing that
					// we want to end up with zero duplicates.
					newConfigs = nil
				} else {
					n, err := strconv.Atoi(label[6:])
					if err != nil {
						return nil, fmt.Errorf("failed to parse ditto multiplier: %v", err)
					}
					newConfigs = make([]Config, n)
				}
			} else {
				newConfigs = make([]Config, 1)
			}

			broker.ComplementGenericConfig(boxedConfig, rawInputs[i-1])
		}

		for _, conf := range newConfigs {
			rawBytes, err := yaml.Marshal(boxedConfig)
			if err != nil {
				return nil, err
			}
			if err = yaml.Unmarshal(rawBytes, &conf); err != nil {
				return nil, err
			}
			inputConfs = append(inputConfs, conf)
		}
	}

	return inputConfs, nil
}

//------------------------------------------------------------------------------

// NewBroker creates a new Broker input type.
func NewBroker(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	return newBrokerHasBatchProcessor(false, conf, mgr, log, stats, pipelines...)
}

// DEPRECATED: This is a hack for until the batch processor is removed.
// TODO: V4 Remove this.
func newBrokerHasBatchProcessor(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	lInputs := len(conf.Broker.Inputs) * conf.Broker.Copies

	if lInputs <= 0 {
		return nil, ErrBrokerNoInputs
	}
	if lInputs == 1 {
		return newHasBatchProcessor(hasBatchProc, conf.Broker.Inputs[0], mgr, log, stats, pipelines...)
	}

	inputs := make([]types.Producer, lInputs)

	var err error
	for j := 0; j < conf.Broker.Copies; j++ {
		for i, iConf := range conf.Broker.Inputs {
			ns := fmt.Sprintf("broker.inputs.%v", i)
			inputs[len(conf.Broker.Inputs)*j+i], err = newHasBatchProcessor(
				hasBatchProc,
				iConf, mgr,
				log.NewModule("."+ns),
				metrics.Combine(stats, metrics.Namespaced(stats, ns)),
				pipelines...,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create input '%v' type '%v': %v", i, iConf.Type, err)
			}
		}
	}

	var b Type
	if b, err = broker.NewFanIn(inputs, stats); err != nil {
		return nil, err
	}

	if conf.Broker.Batching.IsNoop() {
		return b, nil
	}

	policy, err := batch.NewPolicy(conf.Broker.Batching, mgr, log.NewModule(".batching"), metrics.Namespaced(stats, "batching"))
	if err != nil {
		return nil, fmt.Errorf("failed to construct batch policy: %v", err)
	}

	return NewBatcher(policy, b, log, stats), nil
}

//------------------------------------------------------------------------------
