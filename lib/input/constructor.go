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
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// TypeSpec is a struct containing constructors, markdown descriptions and an
// optional sanitisation function for each input type.
type TypeSpec struct {
	brokerConstructor func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelineConstructors ...types.PipelineConstructorFunc,
	) (Type, error)
	constructor        func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error)
	description        string
	sanitiseConfigFunc func(conf Config) (interface{}, error)
}

// Constructors is a map of all input types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each input type.
const (
	TypeAMQP          = "amqp"
	TypeBroker        = "broker"
	TypeDynamic       = "dynamic"
	TypeFile          = "file"
	TypeFiles         = "files"
	TypeGCPPubSub     = "gcp_pubsub"
	TypeHDFS          = "hdfs"
	TypeHTTPClient    = "http_client"
	TypeHTTPServer    = "http_server"
	TypeInproc        = "inproc"
	TypeKafka         = "kafka"
	TypeKafkaBalanced = "kafka_balanced"
	TypeKinesis       = "kinesis"
	TypeMQTT          = "mqtt"
	TypeNanomsg       = "nanomsg"
	TypeNATS          = "nats"
	TypeNATSStream    = "nats_stream"
	TypeNSQ           = "nsq"
	TypeReadUntil     = "read_until"
	TypeRedisList     = "redis_list"
	TypeRedisPubSub   = "redis_pubsub"
	TypeRedisStreams  = "redis_streams"
	TypeS3            = "s3"
	TypeSQS           = "sqs"
	TypeSTDIN         = "stdin"
	TypeWebsocket     = "websocket"
	TypeZMQ4          = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types.
type Config struct {
	Type          string                     `json:"type" yaml:"type"`
	AMQP          reader.AMQPConfig          `json:"amqp" yaml:"amqp"`
	Broker        BrokerConfig               `json:"broker" yaml:"broker"`
	Dynamic       DynamicConfig              `json:"dynamic" yaml:"dynamic"`
	File          FileConfig                 `json:"file" yaml:"file"`
	Files         reader.FilesConfig         `json:"files" yaml:"files"`
	GCPPubSub     reader.GCPPubSubConfig     `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	HDFS          reader.HDFSConfig          `json:"hdfs" yaml:"hdfs"`
	HTTPClient    HTTPClientConfig           `json:"http_client" yaml:"http_client"`
	HTTPServer    HTTPServerConfig           `json:"http_server" yaml:"http_server"`
	Inproc        InprocConfig               `json:"inproc" yaml:"inproc"`
	Kafka         reader.KafkaConfig         `json:"kafka" yaml:"kafka"`
	KafkaBalanced reader.KafkaBalancedConfig `json:"kafka_balanced" yaml:"kafka_balanced"`
	Kinesis       reader.KinesisConfig       `json:"kinesis" yaml:"kinesis"`
	MQTT          reader.MQTTConfig          `json:"mqtt" yaml:"mqtt"`
	Nanomsg       reader.ScaleProtoConfig    `json:"nanomsg" yaml:"nanomsg"`
	NATS          reader.NATSConfig          `json:"nats" yaml:"nats"`
	NATSStream    reader.NATSStreamConfig    `json:"nats_stream" yaml:"nats_stream"`
	NSQ           reader.NSQConfig           `json:"nsq" yaml:"nsq"`
	Plugin        interface{}                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ReadUntil     ReadUntilConfig            `json:"read_until" yaml:"read_until"`
	RedisList     reader.RedisListConfig     `json:"redis_list" yaml:"redis_list"`
	RedisPubSub   reader.RedisPubSubConfig   `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams  reader.RedisStreamsConfig  `json:"redis_streams" yaml:"redis_streams"`
	S3            reader.AmazonS3Config      `json:"s3" yaml:"s3"`
	SQS           reader.AmazonSQSConfig     `json:"sqs" yaml:"sqs"`
	STDIN         STDINConfig                `json:"stdin" yaml:"stdin"`
	Websocket     reader.WebsocketConfig     `json:"websocket" yaml:"websocket"`
	ZMQ4          *reader.ZMQ4Config         `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors    []processor.Config         `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          "stdin",
		AMQP:          reader.NewAMQPConfig(),
		Broker:        NewBrokerConfig(),
		Dynamic:       NewDynamicConfig(),
		File:          NewFileConfig(),
		Files:         reader.NewFilesConfig(),
		GCPPubSub:     reader.NewGCPPubSubConfig(),
		HDFS:          reader.NewHDFSConfig(),
		HTTPClient:    NewHTTPClientConfig(),
		HTTPServer:    NewHTTPServerConfig(),
		Inproc:        NewInprocConfig(),
		Kafka:         reader.NewKafkaConfig(),
		KafkaBalanced: reader.NewKafkaBalancedConfig(),
		Kinesis:       reader.NewKinesisConfig(),
		MQTT:          reader.NewMQTTConfig(),
		Nanomsg:       reader.NewScaleProtoConfig(),
		NATS:          reader.NewNATSConfig(),
		NATSStream:    reader.NewNATSStreamConfig(),
		NSQ:           reader.NewNSQConfig(),
		Plugin:        nil,
		ReadUntil:     NewReadUntilConfig(),
		RedisList:     reader.NewRedisListConfig(),
		RedisPubSub:   reader.NewRedisPubSubConfig(),
		RedisStreams:  reader.NewRedisStreamsConfig(),
		S3:            reader.NewAmazonS3Config(),
		SQS:           reader.NewAmazonSQSConfig(),
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
		if _, exists := hashMap[t]; exists {
			outputMap[t] = hashMap[t]
		}
		if spec, exists := pluginSpecs[conf.Type]; exists {
			if spec.confSanitiser != nil {
				outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
			} else {
				outputMap["plugin"] = hashMap["plugin"]
			}
		}
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

	if spec, exists := pluginSpecs[aliased.Type]; exists {
		dummy := struct {
			Conf interface{} `json:"plugin"`
		}{
			Conf: spec.confConstructor(),
		}
		if err := json.Unmarshal(bytes, &dummy); err != nil {
			return fmt.Errorf("failed to parse plugin config: %v", err)
		}
		aliased.Plugin = dummy.Conf
	} else {
		aliased.Plugin = nil
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

	var raw interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("unable to infer type, multiple candidates '%v' and '%v'", inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("unable to infer type, candidates were: %v", typeCandidates)
		}
		aliased.Type = inferredType
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return err
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return err
		}
		aliased.Plugin = conf
	} else {
		aliased.Plugin = nil
	}

	*c = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-inputs`" + `

An input is a source of data piped through an array of optional
[processors](../processors). Only one input is configured at the root of a
Benthos config. However, the root input can be a [broker](#broker) which
combines multiple inputs.

An input config section looks like this:

` + "``` yaml" + `
input:
  type: foo
  foo:
    bar: baz
  processors:
  - type: qux
` + "```" + ``

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
			confBytes, _ = config.MarshalYAML(confSanit)
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
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	if len(conf.Processors) > 0 {
		pipelines = append([]types.PipelineConstructorFunc{func(i *int) (types.Pipeline, error) {
			if i == nil {
				procs := 0
				i = &procs
			}
			processors := make([]types.Processor, len(conf.Processors))
			for j, procConf := range conf.Processors {
				prefix := fmt.Sprintf("processor.%v", *i)
				var err error
				processors[j], err = processor.New(procConf, mgr, log.NewModule("."+prefix), metrics.Namespaced(stats, prefix))
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
				*i++
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		if c.brokerConstructor != nil {
			return c.brokerConstructor(conf, mgr, log, stats, pipelines...)
		}
		input, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %v", conf.Type, err)
		}
		return WrapWithPipelines(input, pipelines...)
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		input, err := c.constructor(conf.Plugin, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		return WrapWithPipelines(input, pipelines...)
	}
	return nil, types.ErrInvalidInputType
}

//------------------------------------------------------------------------------
