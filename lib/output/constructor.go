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

// typeSpec is a constructor and a usage description for each output type.
type typeSpec struct {
	constructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error)
	description string
}

var constructors = map[string]typeSpec{}

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all output types.
// Note that some configs are empty structs, as the type has no optional values
// but we want to list it as an option.
type Config struct {
	Type        string                 `json:"type" yaml:"type"`
	AmazonS3    writer.AmazonS3Config  `json:"amazon_s3" yaml:"amazon_s3"`
	AmazonSQS   writer.AmazonSQSConfig `json:"amazon_sqs" yaml:"amazon_sqs"`
	AMQP        AMQPConfig             `json:"amqp" yaml:"amqp"`
	Dynamic     DynamicConfig          `json:"dynamic" yaml:"dynamic"`
	FanOut      FanOutConfig           `json:"fan_out" yaml:"fan_out"`
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
	RoundRobin  RoundRobinConfig       `json:"round_robin" yaml:"round_robin"`
	ScaleProto  ScaleProtoConfig       `json:"scalability_protocols" yaml:"scalability_protocols"`
	STDOUT      STDOUTConfig           `json:"stdout" yaml:"stdout"`
	ZMQ4        *ZMQ4Config            `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors  []processor.Config     `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:        "stdout",
		AmazonS3:    writer.NewAmazonS3Config(),
		AmazonSQS:   writer.NewAmazonSQSConfig(),
		AMQP:        NewAMQPConfig(),
		Dynamic:     NewDynamicConfig(),
		FanOut:      NewFanOutConfig(),
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
		RoundRobin:  NewRoundRobinConfig(),
		ScaleProto:  NewScaleProtoConfig(),
		STDOUT:      NewSTDOUTConfig(),
		ZMQ4:        NewZMQ4Config(),
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
	outputMap["type"] = hashMap["type"]
	outputMap[conf.Type] = hashMap[conf.Type]

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

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our output types alphabetically
	names := []string{}
	for name := range constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("OUTPUTS\n")
	buf.WriteString(strings.Repeat("=", 7))
	buf.WriteString("\n\n")
	buf.WriteString("This document has been generated with `benthos --list-outputs`.")
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
					return nil, err
				}
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	if c, ok := constructors[conf.Type]; ok {
		output, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		return WrapWithPipelines(output, pipelines...)
	}
	return nil, types.ErrInvalidOutputType
}

//------------------------------------------------------------------------------
