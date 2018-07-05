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
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/config"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each input type.
type TypeSpec struct {
	brokerConstructor func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelineConstructors ...pipeline.ConstructorFunc,
	) (Type, error)
	constructor        func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error)
	description        string
	sanitiseConfigFunc func(conf Config) (interface{}, error)
}

// Constructors is a map of all input types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types. Note
// that some configs are empty structs, as the type has no optional values but
// we want to list it as an option.
type Config struct {
	Type          string                     `json:"type" yaml:"type"`
	AmazonS3      reader.AmazonS3Config      `json:"amazon_s3" yaml:"amazon_s3"`
	AmazonSQS     reader.AmazonSQSConfig     `json:"amazon_sqs" yaml:"amazon_sqs"`
	AMQP          reader.AMQPConfig          `json:"amqp" yaml:"amqp"`
	Broker        BrokerConfig               `json:"broker" yaml:"broker"`
	Dynamic       DynamicConfig              `json:"dynamic" yaml:"dynamic"`
	File          FileConfig                 `json:"file" yaml:"file"`
	Files         reader.FilesConfig         `json:"files" yaml:"files"`
	HTTPClient    HTTPClientConfig           `json:"http_client" yaml:"http_client"`
	HTTPServer    HTTPServerConfig           `json:"http_server" yaml:"http_server"`
	Inproc        InprocConfig               `json:"inproc" yaml:"inproc"`
	Kafka         reader.KafkaConfig         `json:"kafka" yaml:"kafka"`
	KafkaBalanced reader.KafkaBalancedConfig `json:"kafka_balanced" yaml:"kafka_balanced"`
	MQTT          reader.MQTTConfig          `json:"mqtt" yaml:"mqtt"`
	NATS          reader.NATSConfig          `json:"nats" yaml:"nats"`
	NATSStream    reader.NATSStreamConfig    `json:"nats_stream" yaml:"nats_stream"`
	NSQ           reader.NSQConfig           `json:"nsq" yaml:"nsq"`
	ReadUntil     ReadUntilConfig            `json:"read_until" yaml:"read_until"`
	RedisList     reader.RedisListConfig     `json:"redis_list" yaml:"redis_list"`
	RedisPubSub   reader.RedisPubSubConfig   `json:"redis_pubsub" yaml:"redis_pubsub"`
	ScaleProto    reader.ScaleProtoConfig    `json:"scalability_protocols" yaml:"scalability_protocols"`
	STDIN         STDINConfig                `json:"stdin" yaml:"stdin"`
	Websocket     reader.WebsocketConfig     `json:"websocket" yaml:"websocket"`
	ZMQ4          *reader.ZMQ4Config         `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors    []processor.Config         `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          "stdin",
		AmazonS3:      reader.NewAmazonS3Config(),
		AmazonSQS:     reader.NewAmazonSQSConfig(),
		AMQP:          reader.NewAMQPConfig(),
		Broker:        NewBrokerConfig(),
		Dynamic:       NewDynamicConfig(),
		File:          NewFileConfig(),
		Files:         reader.NewFilesConfig(),
		HTTPClient:    NewHTTPClientConfig(),
		HTTPServer:    NewHTTPServerConfig(),
		Inproc:        NewInprocConfig(),
		Kafka:         reader.NewKafkaConfig(),
		KafkaBalanced: reader.NewKafkaBalancedConfig(),
		MQTT:          reader.NewMQTTConfig(),
		NATS:          reader.NewNATSConfig(),
		NATSStream:    reader.NewNATSStreamConfig(),
		NSQ:           reader.NewNSQConfig(),
		ReadUntil:     NewReadUntilConfig(),
		RedisList:     reader.NewRedisListConfig(),
		RedisPubSub:   reader.NewRedisPubSubConfig(),
		ScaleProto:    reader.NewScaleProtoConfig(),
		STDIN:         NewSTDINConfig(),
		Websocket:     reader.NewWebsocketConfig(),
		ZMQ4:          reader.NewZMQ4Config(),
		Processors:    []processor.Config{},
	}
}

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

	outputMap := config.Sanitised{}

	t := conf.Type
	outputMap["type"] = t
	if sfunc := Constructors[t].sanitiseConfigFunc; sfunc != nil {
		if outputMap[t], err = sfunc(conf); err != nil {
			return nil, err
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

var header = "This document was generated with `benthos --list-inputs`" + `

An input is a source of data piped through an array of
[processors](../processors). Only one input is configured at the root of a
Benthos config. However, the input can be a [broker](#broker) which combines
multiple inputs. For example, if we wanted three inputs, a 'foo' a 'bar' and a
'baz' we could use the 'broker' input type at our root:

` + "``` yaml" + `
input:
  type: broker
  broker:
    inputs:
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
which will be applied to _all_ inputs, and we also have a processor at the baz
level which is only applied to messages from the baz input.`

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Inputs\n")
	buf.WriteString(strings.Repeat("=", 6))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		conf.Processors = nil
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = yaml.Marshal(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
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
				processors[i], err = processor.New(procConf, mgr, log.NewModule("."+conf.Type), stats)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		if c.brokerConstructor != nil {
			return c.brokerConstructor(conf, mgr, log, stats, pipelines...)
		}
		input, err := c.constructor(conf, mgr, log, stats)
		for err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %v", conf.Type, err)
		}
		return WrapWithPipelines(input, pipelines...)
	}
	return nil, types.ErrInvalidInputType
}

//------------------------------------------------------------------------------
