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
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// typeSpec is a constructor and a usage description for each input type.
type typeSpec struct {
	brokerConstructor func(
		conf Config,
		log log.Modular,
		stats metrics.Type,
		pipelineConstructors ...pipeline.ConstructorFunc,
	) (Type, error)
	constructor func(conf Config, log log.Modular, stats metrics.Type) (Type, error)
	description string
}

var constructors = map[string]typeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types. Note
// that some configs are empty structs, as the type has no optional values but
// we want to list it as an option.
type Config struct {
	Type          string              `json:"type" yaml:"type"`
	HTTPServer    HTTPServerConfig    `json:"http_server" yaml:"http_server"`
	HTTPClient    HTTPClientConfig    `json:"http_client" yaml:"http_client"`
	ScaleProto    ScaleProtoConfig    `json:"scalability_protocols" yaml:"scalability_protocols"`
	ZMQ4          *ZMQ4Config         `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Kafka         KafkaConfig         `json:"kafka" yaml:"kafka"`
	KafkaBalanced KafkaBalancedConfig `json:"kafka_balanced" yaml:"kafka_balanced"`
	AMQP          AMQPConfig          `json:"amqp" yaml:"amqp"`
	NSQ           NSQConfig           `json:"nsq" yaml:"nsq"`
	NATS          NATSConfig          `json:"nats" yaml:"nats"`
	NATSStream    NATSStreamConfig    `json:"nats_stream" yaml:"nats_stream"`
	RedisPubSub   RedisPubSubConfig   `json:"redis_pubsub" yaml:"redis_pubsub"`
	File          FileConfig          `json:"file" yaml:"file"`
	STDIN         STDINConfig         `json:"stdin" yaml:"stdin"`
	FanIn         FanInConfig         `json:"fan_in" yaml:"fan_in"`
	Processors    []processor.Config  `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          "stdin",
		HTTPServer:    NewHTTPServerConfig(),
		HTTPClient:    NewHTTPClientConfig(),
		ScaleProto:    NewScaleProtoConfig(),
		ZMQ4:          NewZMQ4Config(),
		Kafka:         NewKafkaConfig(),
		KafkaBalanced: NewKafkaBalancedConfig(),
		AMQP:          NewAMQPConfig(),
		NSQ:           NewNSQConfig(),
		NATS:          NewNATSConfig(),
		NATSStream:    NewNATSStreamConfig(),
		RedisPubSub:   NewRedisPubSubConfig(),
		File:          NewFileConfig(),
		STDIN:         NewSTDINConfig(),
		FanIn:         NewFanInConfig(),
		Processors:    []processor.Config{processor.NewConfig()},
	}
}

//------------------------------------------------------------------------------

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our input types alphabetically
	names := []string{}
	for name := range constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("INPUTS\n")
	buf.WriteString(strings.Repeat("=", 6))
	buf.WriteString("\n\n")
	buf.WriteString("This document has been generated with `benthos --list-inputs`.")
	buf.WriteString("\n\n")

	// Append each description
	for i, name := range names {
		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		buf.WriteString(constructors[name].description)
		if i != (len(names) - 1) {
			buf.WriteString("\n\n")
		}
	}
	return buf.String()
}

// New creates an input type based on an input configuration.
func New(
	conf Config,
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
					return nil, err
				}
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	if c, ok := constructors[conf.Type]; ok {
		if c.brokerConstructor != nil {
			return c.brokerConstructor(conf, log, stats, pipelines...)
		}
		input, err := c.constructor(conf, log, stats)
		for err != nil {
			return nil, err
		}
		return WrapWithPipelines(input, pipelines...)
	}
	return nil, types.ErrInvalidInputType
}

//------------------------------------------------------------------------------
