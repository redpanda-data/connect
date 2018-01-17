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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/Jeffail/benthos/lib/broker"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

var (
	// ErrFanInNoInputs is returned when creating a FanIn type with zero inputs.
	ErrFanInNoInputs = errors.New("attempting to create fan_in input type with no inputs")
)

//------------------------------------------------------------------------------

func init() {
	constructors["fan_in"] = typeSpec{
		brokerConstructor: NewFanIn,
		description: `
The fan in type allows you to combine multiple inputs. Each input will be read
in parallel. In order to configure a fan in type you simply add an array of
input configuration objects into the inputs field.

Adding more input types allows you to merge streams from multiple sources into
one. For example, having both a ZMQ4 PULL socket and a Nanomsg PULL socket:

` + "``` yaml" + `
type: fan_in
fan_in:
  inputs:
  -
    type: scalability_protocols
    scalability_protocols:
      address: tcp://nanoserver:3003
      bind_address: false
      socket_type: PULL
  -
    type: zmq4
    zmq4:
      addresses:
      - tcp://zmqserver:3004
      socket_type: PULL
` + "```" + `

Sometimes you will want several inputs of very similar configuration. You can
use the special type ditto in this case to duplicate the previous config and
apply selective changes.

For example, if combining two kafka inputs with the same set up, reading
different partitions you can use this shortcut:

` + "``` yaml" + `
inputs:
-
  type: kafka
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
    partition: 0
-
  type: ditto
  kafka:
    partition: 1
` + "```" + `

Which will result in two inputs targeting the same kafka brokers, on the same
consumer group etc, but consuming their own partitions. Ditto can also be
specified with a multiplier, which is useful if you want multiple inputs that do
not differ in config, like this:

` + "``` yaml" + `
inputs:
-
  type: kafka_balanced
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
-
  type: ditto_3
` + "```" + `

Which results in a total of four kafka_balanced inputs. Note that ditto_0 will
result in no duplicate configs, this might be useful if the config is generated
and there's a chance you won't want any duplicates.`,
	}
}

//------------------------------------------------------------------------------

// FanInConfig is configuration for the FanIn input type.
type FanInConfig struct {
	Inputs []interface{} `json:"inputs" yaml:"inputs"`
}

// NewFanInConfig creates a new FanInConfig with default values.
func NewFanInConfig() FanInConfig {
	return FanInConfig{
		Inputs: []interface{}{},
	}
}

//------------------------------------------------------------------------------

// parseInputConfsWithDefaults takes a fan in config and returns an array of
// input configs with default values in place of omitted values. This is
// necessary because when unmarshalling config files using structs you can
// pre-populate non-reference type struct fields with default values, but array
// objects will lose those defaults.
//
// In order to ensure that omitted values are set to default we initially parse
// the array as interface{} types and then individually apply the defaults by
// marshalling and unmarshalling. The correct way to do this would be to use
// json.RawMessage, but our config files can contain a range of different
// formats that we do not know at this stage, therefore we use the more hacky
// method as performance is not an issue at this stage.
func parseInputConfsWithDefaults(conf FanInConfig) ([]Config, error) {
	inputConfs := []Config{}

	// NOTE: Use yaml here as it supports more types than JSON
	// (map[interface{}]interface{}).
	for i, boxedConfig := range conf.Inputs {
		newConfs := []Config{NewConfig()}
		if i > 0 {
			// If the type of this output is 'ditto' we want to start with a
			// duplicate of the previous config.
			newConfsFromDitto := func(label string) error {
				// Remove the vanilla config.
				newConfs = []Config{}

				// Check if there is a ditto multiplier.
				if len(label) > 5 && label[5] == '_' {
					if label[6:] == "0" {
						// This is a special case where we are expressing that
						// we want to end up with zero duplicates.
						return nil
					}
					n, err := strconv.Atoi(label[6:])
					if err != nil {
						return fmt.Errorf("failed to parse ditto multiplier: %v", err)
					}
					for j := 0; j < n; j++ {
						newConfs = append(newConfs, inputConfs[i-1])
					}
				} else {
					// Otherwise just add a single dupe of the previous config.
					newConfs = append(newConfs, inputConfs[i-1])
				}
				return nil
			}
			switch unboxed := boxedConfig.(type) {
			case map[string]interface{}:
				if t, ok := unboxed["type"].(string); ok && strings.Index(t, "ditto") == 0 {
					if err := newConfsFromDitto(t); err != nil {
						return nil, err
					}
					if len(newConfs) > 0 {
						unboxed["type"] = newConfs[0].Type
					}
				}
			case map[interface{}]interface{}:
				if t, ok := unboxed["type"].(string); ok && strings.Index(t, "ditto") == 0 {
					if err := newConfsFromDitto(t); err != nil {
						return nil, err
					}
					if len(newConfs) > 0 {
						unboxed["type"] = newConfs[0].Type
					}
				}
			}
		}
		for _, conf := range newConfs {
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

// NewFanIn creates a new FanIn input type.
func NewFanIn(
	conf Config,
	log log.Modular,
	stats metrics.Type,
	pipelines ...pipeline.ConstructorFunc,
) (Type, error) {
	if len(conf.FanIn.Inputs) == 0 {
		return nil, ErrFanInNoInputs
	}

	inputConfs, err := parseInputConfsWithDefaults(conf.FanIn)
	if err != nil {
		return nil, err
	}

	inputs := make([]types.Producer, len(inputConfs))

	for i, iConf := range inputConfs {
		inputs[i], err = New(iConf, log, stats, pipelines...)
		if err != nil {
			return nil, err
		}
	}

	return broker.NewFanIn(inputs, stats)
}

//------------------------------------------------------------------------------
