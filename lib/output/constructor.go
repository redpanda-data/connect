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

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
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
	TypeAMQP            = "amqp"
	TypeAMQP09          = "amqp_0_9"
	TypeBroker          = "broker"
	TypeCache           = "cache"
	TypeDrop            = "drop"
	TypeDropOnError     = "drop_on_error"
	TypeDynamic         = "dynamic"
	TypeDynamoDB        = "dynamodb"
	TypeElasticsearch   = "elasticsearch"
	TypeFile            = "file"
	TypeFiles           = "files"
	TypeGCPPubSub       = "gcp_pubsub"
	TypeHDFS            = "hdfs"
	TypeHTTPClient      = "http_client"
	TypeHTTPServer      = "http_server"
	TypeInproc          = "inproc"
	TypeKafka           = "kafka"
	TypeKinesis         = "kinesis"
	TypeKinesisFirehose = "kinesis_firehose"
	TypeMQTT            = "mqtt"
	TypeNanomsg         = "nanomsg"
	TypeNATS            = "nats"
	TypeNATSStream      = "nats_stream"
	TypeNSQ             = "nsq"
	TypeRedisHash       = "redis_hash"
	TypeRedisList       = "redis_list"
	TypeRedisPubSub     = "redis_pubsub"
	TypeRedisStreams    = "redis_streams"
	TypeRetry           = "retry"
	TypeS3              = "s3"
	TypeSNS             = "sns"
	TypeSQS             = "sqs"
	TypeSTDOUT          = "stdout"
	TypeSwitch          = "switch"
	TypeSyncResponse    = "sync_response"
	TypeTCP             = "tcp"
	TypeUDP             = "udp"
	TypeWebsocket       = "websocket"
	TypeZMQ4            = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all output types.
type Config struct {
	Type            string                       `json:"type" yaml:"type"`
	AMQP            writer.AMQPConfig            `json:"amqp" yaml:"amqp"`
	AMQP09          writer.AMQPConfig            `json:"amqp_0_9" yaml:"amqp_0_9"`
	Broker          BrokerConfig                 `json:"broker" yaml:"broker"`
	Cache           writer.CacheConfig           `json:"cache" yaml:"cache"`
	Drop            writer.DropConfig            `json:"drop" yaml:"drop"`
	DropOnError     DropOnErrorConfig            `json:"drop_on_error" yaml:"drop_on_error"`
	Dynamic         DynamicConfig                `json:"dynamic" yaml:"dynamic"`
	DynamoDB        writer.DynamoDBConfig        `json:"dynamodb" yaml:"dynamodb"`
	Elasticsearch   writer.ElasticsearchConfig   `json:"elasticsearch" yaml:"elasticsearch"`
	File            FileConfig                   `json:"file" yaml:"file"`
	Files           writer.FilesConfig           `json:"files" yaml:"files"`
	GCPPubSub       writer.GCPPubSubConfig       `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	HDFS            writer.HDFSConfig            `json:"hdfs" yaml:"hdfs"`
	HTTPClient      writer.HTTPClientConfig      `json:"http_client" yaml:"http_client"`
	HTTPServer      HTTPServerConfig             `json:"http_server" yaml:"http_server"`
	Inproc          InprocConfig                 `json:"inproc" yaml:"inproc"`
	Kafka           writer.KafkaConfig           `json:"kafka" yaml:"kafka"`
	Kinesis         writer.KinesisConfig         `json:"kinesis" yaml:"kinesis"`
	KinesisFirehose writer.KinesisFirehoseConfig `json:"kinesis_firehose" yaml:"kinesis_firehose"`
	MQTT            writer.MQTTConfig            `json:"mqtt" yaml:"mqtt"`
	Nanomsg         writer.NanomsgConfig         `json:"nanomsg" yaml:"nanomsg"`
	NATS            writer.NATSConfig            `json:"nats" yaml:"nats"`
	NATSStream      writer.NATSStreamConfig      `json:"nats_stream" yaml:"nats_stream"`
	NSQ             writer.NSQConfig             `json:"nsq" yaml:"nsq"`
	Plugin          interface{}                  `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	RedisHash       writer.RedisHashConfig       `json:"redis_hash" yaml:"redis_hash"`
	RedisList       writer.RedisListConfig       `json:"redis_list" yaml:"redis_list"`
	RedisPubSub     writer.RedisPubSubConfig     `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams    writer.RedisStreamsConfig    `json:"redis_streams" yaml:"redis_streams"`
	Retry           RetryConfig                  `json:"retry" yaml:"retry"`
	S3              writer.AmazonS3Config        `json:"s3" yaml:"s3"`
	SNS             writer.SNSConfig             `json:"sns" yaml:"sns"`
	SQS             writer.AmazonSQSConfig       `json:"sqs" yaml:"sqs"`
	STDOUT          STDOUTConfig                 `json:"stdout" yaml:"stdout"`
	Switch          SwitchConfig                 `json:"switch" yaml:"switch"`
	SyncResponse    struct{}                     `json:"sync_response" yaml:"sync_response"`
	TCP             writer.TCPConfig             `json:"tcp" yaml:"tcp"`
	UDP             writer.UDPConfig             `json:"udp" yaml:"udp"`
	Websocket       writer.WebsocketConfig       `json:"websocket" yaml:"websocket"`
	ZMQ4            *writer.ZMQ4Config           `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors      []processor.Config           `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:            "stdout",
		AMQP:            writer.NewAMQPConfig(),
		AMQP09:          writer.NewAMQPConfig(),
		Broker:          NewBrokerConfig(),
		Cache:           writer.NewCacheConfig(),
		Drop:            writer.NewDropConfig(),
		DropOnError:     NewDropOnErrorConfig(),
		Dynamic:         NewDynamicConfig(),
		DynamoDB:        writer.NewDynamoDBConfig(),
		Elasticsearch:   writer.NewElasticsearchConfig(),
		File:            NewFileConfig(),
		Files:           writer.NewFilesConfig(),
		GCPPubSub:       writer.NewGCPPubSubConfig(),
		HDFS:            writer.NewHDFSConfig(),
		HTTPClient:      writer.NewHTTPClientConfig(),
		HTTPServer:      NewHTTPServerConfig(),
		Inproc:          NewInprocConfig(),
		Kafka:           writer.NewKafkaConfig(),
		Kinesis:         writer.NewKinesisConfig(),
		KinesisFirehose: writer.NewKinesisFirehoseConfig(),
		MQTT:            writer.NewMQTTConfig(),
		Nanomsg:         writer.NewNanomsgConfig(),
		NATS:            writer.NewNATSConfig(),
		NATSStream:      writer.NewNATSStreamConfig(),
		NSQ:             writer.NewNSQConfig(),
		Plugin:          nil,
		RedisHash:       writer.NewRedisHashConfig(),
		RedisList:       writer.NewRedisListConfig(),
		RedisPubSub:     writer.NewRedisPubSubConfig(),
		RedisStreams:    writer.NewRedisStreamsConfig(),
		Retry:           NewRetryConfig(),
		S3:              writer.NewAmazonS3Config(),
		SNS:             writer.NewSNSConfig(),
		SQS:             writer.NewAmazonSQSConfig(),
		STDOUT:          NewSTDOUTConfig(),
		Switch:          NewSwitchConfig(),
		SyncResponse:    struct{}{},
		TCP:             writer.NewTCPConfig(),
		UDP:             writer.NewUDPConfig(),
		Websocket:       writer.NewWebsocketConfig(),
		ZMQ4:            writer.NewZMQ4Config(),
		Processors:      []processor.Config{},
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
			var plugSanit interface{}
			if spec.confSanitiser != nil {
				plugSanit = spec.confSanitiser(conf.Plugin)
			} else {
				plugSanit = hashMap["plugin"]
			}
			if plugSanit != nil {
				outputMap["plugin"] = plugSanit
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

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("line %v: unable to infer type, multiple candidates '%v' and '%v'", value.Line, inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("line %v: unable to infer type, candidates were: %v", value.Line, typeCandidates)
		}
		aliased.Type = inferredType
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = conf
	} else {
		aliased.Plugin = nil
	}

	*c = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-outputs`" + `

An output is a sink where we wish to send our consumed data after applying an
optional array of [processors](../processors). Only one output is configured at
the root of a Benthos config. However, the output can be a [broker](#broker)
which combines multiple outputs under a chosen brokering pattern.

An output config section looks like this:

` + "``` yaml" + `
output:
  type: foo
  foo:
    bar: baz
  processors:
  - type: qux
` + "```" + `

### Back Pressure

Benthos outputs apply back pressure to components upstream. This means if your
output target starts blocking traffic Benthos will gracefully stop consuming
until the issue is resolved.

### Retries

When a Benthos output fails to send a message the error is propagated back up to
the input, where depending on the protocol it will either be pushed back to the
source as a Noack (AMQP) or will be reattempted indefinitely with the commit
withheld until success (Kafka).

It's possible to instead have Benthos indefinitely retry an output until success
with a [` + "`retry`" + `](#retry) output. Some other outputs, such as the
[` + "`broker`" + `](#broker), might also retry indefinitely depending on their
configuration.

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific
outputs either by using the ` + "[`switch`](#switch)" + ` output, or a
` + "[`broker`](#broker)" + ` with the ` + "`fan_out`" + ` pattern and a
[filter processor](../processors/README.md#filter_parts) on each output, which
is a processor that drops messages if the condition does not pass.
Conditions are content aware logical operators that can be combined using
boolean logic.

For more information regarding conditions, including a full list of available
conditions please [read the docs here](../conditions/README.md).

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

// New creates an output type based on an output configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	if len(conf.Processors) > 0 {
		pipelines = append(pipelines, []types.PipelineConstructorFunc{func(i *int) (types.Pipeline, error) {
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
