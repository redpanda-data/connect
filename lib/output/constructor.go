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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/config"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// TypeSpec is a constructor and a usage description for each output type.
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

// Constructors is a map of all output types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each output type.
const (
	TypeAMQP          = "amqp"
	TypeBroker        = "broker"
	TypeDynamic       = "dynamic"
	TypeElasticsearch = "elasticsearch"
	TypeFile          = "file"
	TypeFiles         = "files"
	TypeHDFS          = "hdfs"
	TypeHTTPClient    = "http_client"
	TypeHTTPServer    = "http_server"
	TypeInproc        = "inproc"
	TypeKafka         = "kafka"
	TypeKinesis       = "kinesis"
	TypeMQTT          = "mqtt"
	TypeNanomsg       = "nanomsg"
	TypeNATS          = "nats"
	TypeNATSStream    = "nats_stream"
	TypeNSQ           = "nsq"
	TypeRedisList     = "redis_list"
	TypeRedisPubSub   = "redis_pubsub"
	TypeRedisStreams  = "redis_streams"
	TypeRetry         = "retry"
	TypeS3            = "s3"
	TypeSQS           = "sqs"
	TypeSTDOUT        = "stdout"
	TypeWebsocket     = "websocket"
	TypeZMQ4          = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all output types.
type Config struct {
	Type          string                     `json:"type" yaml:"type"`
	AMQP          writer.AMQPConfig          `json:"amqp" yaml:"amqp"`
	Broker        BrokerConfig               `json:"broker" yaml:"broker"`
	Dynamic       DynamicConfig              `json:"dynamic" yaml:"dynamic"`
	Elasticsearch writer.ElasticsearchConfig `json:"elasticsearch" yaml:"elasticsearch"`
	File          FileConfig                 `json:"file" yaml:"file"`
	Files         writer.FilesConfig         `json:"files" yaml:"files"`
	HDFS          writer.HDFSConfig          `json:"hdfs" yaml:"hdfs"`
	HTTPClient    writer.HTTPClientConfig    `json:"http_client" yaml:"http_client"`
	HTTPServer    HTTPServerConfig           `json:"http_server" yaml:"http_server"`
	Inproc        InprocConfig               `json:"inproc" yaml:"inproc"`
	Kafka         writer.KafkaConfig         `json:"kafka" yaml:"kafka"`
	Kinesis       writer.KinesisConfig       `json:"kinesis" yaml:"kinesis"`
	MQTT          writer.MQTTConfig          `json:"mqtt" yaml:"mqtt"`
	Nanomsg       NanomsgConfig              `json:"nanomsg" yaml:"nanomsg"`
	NATS          NATSConfig                 `json:"nats" yaml:"nats"`
	NATSStream    NATSStreamConfig           `json:"nats_stream" yaml:"nats_stream"`
	NSQ           NSQConfig                  `json:"nsq" yaml:"nsq"`
	Plugin        interface{}                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	RedisList     writer.RedisListConfig     `json:"redis_list" yaml:"redis_list"`
	RedisPubSub   writer.RedisPubSubConfig   `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams  writer.RedisStreamsConfig  `json:"redis_streams" yaml:"redis_streams"`
	Retry         RetryConfig                `json:"retry" yaml:"retry"`
	S3            writer.AmazonS3Config      `json:"s3" yaml:"s3"`
	SQS           writer.AmazonSQSConfig     `json:"sqs" yaml:"sqs"`
	STDOUT        STDOUTConfig               `json:"stdout" yaml:"stdout"`
	Websocket     writer.WebsocketConfig     `json:"websocket" yaml:"websocket"`
	ZMQ4          *writer.ZMQ4Config         `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors    []processor.Config         `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:          "stdout",
		AMQP:          writer.NewAMQPConfig(),
		Broker:        NewBrokerConfig(),
		Dynamic:       NewDynamicConfig(),
		Elasticsearch: writer.NewElasticsearchConfig(),
		File:          NewFileConfig(),
		Files:         writer.NewFilesConfig(),
		HDFS:          writer.NewHDFSConfig(),
		HTTPClient:    writer.NewHTTPClientConfig(),
		HTTPServer:    NewHTTPServerConfig(),
		Inproc:        NewInprocConfig(),
		Kafka:         writer.NewKafkaConfig(),
		Kinesis:       writer.NewKinesisConfig(),
		MQTT:          writer.NewMQTTConfig(),
		Nanomsg:       NewNanomsgConfig(),
		NATS:          NewNATSConfig(),
		NATSStream:    NewNATSStreamConfig(),
		NSQ:           NewNSQConfig(),
		Plugin:        nil,
		RedisList:     writer.NewRedisListConfig(),
		RedisPubSub:   writer.NewRedisPubSubConfig(),
		RedisStreams:  writer.NewRedisStreamsConfig(),
		Retry:         NewRetryConfig(),
		S3:            writer.NewAmazonS3Config(),
		SQS:           writer.NewAmazonSQSConfig(),
		STDOUT:        NewSTDOUTConfig(),
		Websocket:     writer.NewWebsocketConfig(),
		ZMQ4:          writer.NewZMQ4Config(),
		Processors:    []processor.Config{},
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
	}

	*c = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-outputs`" + `

An output is a sink where we wish to send our consumed data after applying an
array of [processors](../processors). Only one output is configured at the root
of a Benthos config. However, the output can be a [broker](#broker) which
combines multiple outputs under a specific pattern.

### Multiplexing Outputs

It is possible to perform
[content based multiplexing](../concepts.md#content-based-multiplexing) of
messages to specific outputs using a broker with the 'fan_out' pattern and a
[filter processor](../processors/README.md#filter) on each output, which
is a processor that drops messages if the condition does not pass. Conditions
are content aware logical operators that can be combined using boolean logic.

For more information regarding conditions, including a full list of available
conditions please [read the docs here](../conditions/README.md)

### Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using
a ` + "[`broker`](#broker)" + ` output with the 'try' pattern.`

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
	buf.WriteString("Outputs\n")
	buf.WriteString(strings.Repeat("=", 7))
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

// New creates an output type based on an output configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	if len(conf.Processors) > 0 {
		pipelines = append(pipelines, []types.PipelineConstructorFunc{func() (types.Pipeline, error) {
			processors := make([]types.Processor, len(conf.Processors))
			for i, procConf := range conf.Processors {
				var err error
				processors[i], err = processor.New(procConf, mgr, log.NewModule("."+conf.Type), stats)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		if c.brokerConstructor != nil {
			return c.brokerConstructor(conf, mgr, log, stats, pipelines...)
		}
		output, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create output '%v': %v", conf.Type, err)
		}
		return WrapWithPipelines(output, pipelines...)
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		output, err := c.constructor(conf.Plugin, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		return WrapWithPipelines(output, pipelines...)
	}
	return nil, types.ErrInvalidOutputType
}

//------------------------------------------------------------------------------
