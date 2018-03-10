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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each output type.
type TypeSpec struct {
	constructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error)
	description string
}

// Constructors is a map of all output types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all output types.
// Note that some configs are empty structs, as the type has no optional values
// but we want to list it as an option.
type Config struct {
	Type        string                 `json:"type" yaml:"type"`
	AmazonS3    writer.AmazonS3Config  `json:"amazon_s3" yaml:"amazon_s3"`
	AmazonSQS   writer.AmazonSQSConfig `json:"amazon_sqs" yaml:"amazon_sqs"`
	AMQP        AMQPConfig             `json:"amqp" yaml:"amqp"`
	Broker      BrokerConfig           `json:"broker" yaml:"broker"`
	Dynamic     DynamicConfig          `json:"dynamic" yaml:"dynamic"`
	File        FileConfig             `json:"file" yaml:"file"`
	Files       writer.FilesConfig     `json:"files" yaml:"files"`
	HTTPClient  HTTPClientConfig       `json:"http_client" yaml:"http_client"`
	HTTPServer  HTTPServerConfig       `json:"http_server" yaml:"http_server"`
	Kafka       writer.KafkaConfig     `json:"kafka" yaml:"kafka"`
	MQTT        writer.MQTTConfig      `json:"mqtt" yaml:"mqtt"`
	NATS        NATSConfig             `json:"nats" yaml:"nats"`
	NATSStream  NATSStreamConfig       `json:"nats_stream" yaml:"nats_stream"`
	NSQ         NSQConfig              `json:"nsq" yaml:"nsq"`
	RedisList   writer.RedisListConfig `json:"redis_list" yaml:"redis_list"`
	RedisPubSub RedisPubSubConfig      `json:"redis_pubsub" yaml:"redis_pubsub"`
	ScaleProto  ScaleProtoConfig       `json:"scalability_protocols" yaml:"scalability_protocols"`
	STDOUT      STDOUTConfig           `json:"stdout" yaml:"stdout"`
	ZMQ4        *writer.ZMQ4Config     `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors  []processor.Config     `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:        "stdout",
		AmazonS3:    writer.NewAmazonS3Config(),
		AmazonSQS:   writer.NewAmazonSQSConfig(),
		AMQP:        NewAMQPConfig(),
		Broker:      NewBrokerConfig(),
		Dynamic:     NewDynamicConfig(),
		File:        NewFileConfig(),
		Files:       writer.NewFilesConfig(),
		HTTPClient:  NewHTTPClientConfig(),
		HTTPServer:  NewHTTPServerConfig(),
		Kafka:       writer.NewKafkaConfig(),
		MQTT:        writer.NewMQTTConfig(),
		NATS:        NewNATSConfig(),
		NATSStream:  NewNATSStreamConfig(),
		NSQ:         NewNSQConfig(),
		RedisList:   writer.NewRedisListConfig(),
		RedisPubSub: NewRedisPubSubConfig(),
		ScaleProto:  NewScaleProtoConfig(),
		STDOUT:      NewSTDOUTConfig(),
		ZMQ4:        writer.NewZMQ4Config(),
		Processors:  []processor.Config{},
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := map[string]interface{}{}

	t := conf.Type
	outputMap["type"] = t

	var nestedOutputs []Config
	if t == "broker" {
		nestedOutputs = conf.Broker.Outputs
	}
	if len(nestedOutputs) > 0 {
		outSlice := []interface{}{}
		for _, output := range nestedOutputs {
			var sanOutput interface{}
			if sanOutput, err = SanitiseConfig(output); err != nil {
				return nil, err
			}
			outSlice = append(outSlice, sanOutput)
		}
		outputMap[t] = map[string]interface{}{
			"pattern": conf.Broker.Pattern,
			"outputs": outSlice,
		}
	} else {
		outputMap[t] = hashMap[t]
	}

	if len(conf.Processors) == 0 {
		return outputMap, nil
	}

	procSlice := []interface{}{}
	for _, proc := range conf.Processors {
		var procSanitised interface{}
		procSanitised, err = processor.SanitiseConfig(proc)
		if err != nil {
			return nil, err
		}
		procSlice = append(procSlice, procSanitised)
	}
	outputMap["processors"] = procSlice

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c *Config) UnmarshalJSON(bytes []byte) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := json.Unmarshal(bytes, &aliased); err != nil {
		return err
	}

	*c = Config(aliased)
	return nil
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	*c = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-outputs`" + `

An output is a sink where we wish to send our consumed data after applying an
array of [processors](../processors). Only one output is configured at the root
of a Benthos config. However, the output can be a [broker](#broker) which
combines multiple outputs under a specific pattern. For example, if we wanted
three outputs, a 'foo' a 'bar' and a 'baz', where each output received every
message we could use the 'broker' output type at our root:

` + "``` yaml" + `
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        foo_field_1: value1
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
    - type: baz
      baz:
        baz_field_1: value4
      processors:
      - type: baz_processor
  processors:
  - type: some_processor
` + "```" + `

Note that in this example we have specified a processor at the broker level
which will be applied to messages sent to _all_ outputs, and we also have a
processor at the baz level which is only applied to messages sent to the baz
output.

If we wanted each message to go to a single output then we could use the
'round_robin' broker pattern, or the 'greedy' broker pattern if we wanted to
maximize throughput. For more information regarding these patterns please read
[the broker section](#broker).

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific
outputs using a broker with the 'fan_out' pattern and a
[condition processor](../processors/list.md#condition) on each output, which is
a processor that drops messages if the condition does not pass. Conditions are
content aware logical operators that can be combined using boolean logic.

For example, say we have an output 'foo' that we only want to receive messages
that contain the word 'foo', and an output 'bar' that we wish to send everything
that 'foo' doesn't receive, we can achieve that with this config:

` + "``` yaml" + `
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        foo_field_1: value1
      processors:
      - type: condition
        condition:
          type: content
          content:
            operator: contains
            part: 0
            arg: foo
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
      processors:
      - type: condition
        condition:
          type: not
          not:
            type: content
            content:
              operator: contains
              part: 0
              arg: foo
` + "```" + `

For more information regarding conditions please
[read the docs here](../conditions/README.md)`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our output types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("OUTPUTS\n")
	buf.WriteString(strings.Repeat("=", 7))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		buf.WriteString(Constructors[name].description)
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...pipeline.ConstructorFunc,
) (Type, error) {
	if len(conf.Processors) > 0 {
		pipelines = append([]pipeline.ConstructorFunc{func() (pipeline.Type, error) {
			processors := make([]processor.Type, len(conf.Processors))
			for i, procConf := range conf.Processors {
				var err error
				processors[i], err = processor.New(procConf, log.NewModule("."+conf.Type), stats)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		output, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create output '%v': %v", conf.Type, err)
		}
		return WrapWithPipelines(output, pipelines...)
	}
	return nil, types.ErrInvalidOutputType
}

//------------------------------------------------------------------------------
