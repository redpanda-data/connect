package input

import (
	"encoding/json"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
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
	sanitiseConfigFunc func(conf Config) (interface{}, error)

	// DEPRECATED: This is a hack for until the batch processor is removed.
	// TODO: V4 Remove this.
	brokerConstructorHasBatchProcessor func(
		hasBatchProc bool,
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelineConstructors ...types.PipelineConstructorFunc,
	) (Type, error)
	constructorHasBatchProcessor func(
		hasBatchProc bool,
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
	) (Type, error)

	Description string

	// Deprecated indicates whether this component is deprecated.
	Deprecated bool
}

// Constructors is a map of all input types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each input type.
const (
	TypeAMQP            = "amqp"
	TypeAMQP09          = "amqp_0_9"
	TypeBroker          = "broker"
	TypeDynamic         = "dynamic"
	TypeFile            = "file"
	TypeFiles           = "files"
	TypeGCPPubSub       = "gcp_pubsub"
	TypeHDFS            = "hdfs"
	TypeHTTPClient      = "http_client"
	TypeHTTPServer      = "http_server"
	TypeInproc          = "inproc"
	TypeKafka           = "kafka"
	TypeKafkaBalanced   = "kafka_balanced"
	TypeKinesis         = "kinesis"
	TypeKinesisBalanced = "kinesis_balanced"
	TypeMQTT            = "mqtt"
	TypeNanomsg         = "nanomsg"
	TypeNATS            = "nats"
	TypeNATSStream      = "nats_stream"
	TypeNSQ             = "nsq"
	TypeReadUntil       = "read_until"
	TypeRedisList       = "redis_list"
	TypeRedisPubSub     = "redis_pubsub"
	TypeRedisStreams    = "redis_streams"
	TypeS3              = "s3"
	TypeSQS             = "sqs"
	TypeSTDIN           = "stdin"
	TypeTCP             = "tcp"
	TypeTCPServer       = "tcp_server"
	TypeUDPServer       = "udp_server"
	TypeWebsocket       = "websocket"
	TypeZMQ4            = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types.
type Config struct {
	Type            string                       `json:"type" yaml:"type"`
	AMQP            reader.AMQPConfig            `json:"amqp" yaml:"amqp"`
	AMQP09          reader.AMQP09Config          `json:"amqp_0_9" yaml:"amqp_0_9"`
	Broker          BrokerConfig                 `json:"broker" yaml:"broker"`
	Dynamic         DynamicConfig                `json:"dynamic" yaml:"dynamic"`
	File            FileConfig                   `json:"file" yaml:"file"`
	Files           reader.FilesConfig           `json:"files" yaml:"files"`
	GCPPubSub       reader.GCPPubSubConfig       `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	HDFS            reader.HDFSConfig            `json:"hdfs" yaml:"hdfs"`
	HTTPClient      HTTPClientConfig             `json:"http_client" yaml:"http_client"`
	HTTPServer      HTTPServerConfig             `json:"http_server" yaml:"http_server"`
	Inproc          InprocConfig                 `json:"inproc" yaml:"inproc"`
	Kafka           reader.KafkaConfig           `json:"kafka" yaml:"kafka"`
	KafkaBalanced   reader.KafkaBalancedConfig   `json:"kafka_balanced" yaml:"kafka_balanced"`
	Kinesis         reader.KinesisConfig         `json:"kinesis" yaml:"kinesis"`
	KinesisBalanced reader.KinesisBalancedConfig `json:"kinesis_balanced" yaml:"kinesis_balanced"`
	MQTT            reader.MQTTConfig            `json:"mqtt" yaml:"mqtt"`
	Nanomsg         reader.ScaleProtoConfig      `json:"nanomsg" yaml:"nanomsg"`
	NATS            reader.NATSConfig            `json:"nats" yaml:"nats"`
	NATSStream      reader.NATSStreamConfig      `json:"nats_stream" yaml:"nats_stream"`
	NSQ             reader.NSQConfig             `json:"nsq" yaml:"nsq"`
	Plugin          interface{}                  `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ReadUntil       ReadUntilConfig              `json:"read_until" yaml:"read_until"`
	RedisList       reader.RedisListConfig       `json:"redis_list" yaml:"redis_list"`
	RedisPubSub     reader.RedisPubSubConfig     `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams    reader.RedisStreamsConfig    `json:"redis_streams" yaml:"redis_streams"`
	S3              reader.AmazonS3Config        `json:"s3" yaml:"s3"`
	SQS             reader.AmazonSQSConfig       `json:"sqs" yaml:"sqs"`
	STDIN           STDINConfig                  `json:"stdin" yaml:"stdin"`
	TCP             TCPConfig                    `json:"tcp" yaml:"tcp"`
	TCPServer       TCPServerConfig              `json:"tcp_server" yaml:"tcp_server"`
	UDPServer       UDPServerConfig              `json:"udp_server" yaml:"udp_server"`
	Websocket       reader.WebsocketConfig       `json:"websocket" yaml:"websocket"`
	ZMQ4            *reader.ZMQ4Config           `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors      []processor.Config           `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:            "stdin",
		AMQP:            reader.NewAMQPConfig(),
		AMQP09:          reader.NewAMQP09Config(),
		Broker:          NewBrokerConfig(),
		Dynamic:         NewDynamicConfig(),
		File:            NewFileConfig(),
		Files:           reader.NewFilesConfig(),
		GCPPubSub:       reader.NewGCPPubSubConfig(),
		HDFS:            reader.NewHDFSConfig(),
		HTTPClient:      NewHTTPClientConfig(),
		HTTPServer:      NewHTTPServerConfig(),
		Inproc:          NewInprocConfig(),
		Kafka:           reader.NewKafkaConfig(),
		KafkaBalanced:   reader.NewKafkaBalancedConfig(),
		Kinesis:         reader.NewKinesisConfig(),
		KinesisBalanced: reader.NewKinesisBalancedConfig(),
		MQTT:            reader.NewMQTTConfig(),
		Nanomsg:         reader.NewScaleProtoConfig(),
		NATS:            reader.NewNATSConfig(),
		NATSStream:      reader.NewNATSStreamConfig(),
		NSQ:             reader.NewNSQConfig(),
		Plugin:          nil,
		ReadUntil:       NewReadUntilConfig(),
		RedisList:       reader.NewRedisListConfig(),
		RedisPubSub:     reader.NewRedisPubSubConfig(),
		RedisStreams:    reader.NewRedisStreamsConfig(),
		S3:              reader.NewAmazonS3Config(),
		SQS:             reader.NewAmazonSQSConfig(),
		STDIN:           NewSTDINConfig(),
		TCP:             NewTCPConfig(),
		TCPServer:       NewTCPServerConfig(),
		UDPServer:       NewUDPServerConfig(),
		Websocket:       reader.NewWebsocketConfig(),
		ZMQ4:            reader.NewZMQ4Config(),
		Processors:      []processor.Config{},
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

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	return newHasBatchProcessor(false, conf, mgr, log, stats, pipelines...)
}

// DEPRECATED: This is a hack for until the batch processor is removed.
// TODO: V4 Remove this.
func newHasBatchProcessor(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	if len(conf.Processors) > 0 {
		// TODO: V4 Remove this.
		for _, procConf := range conf.Processors {
			if procConf.Type == processor.TypeBatch {
				hasBatchProc = true
			}
		}

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
		// TODO: V4 Remove this.
		if c.brokerConstructorHasBatchProcessor != nil {
			return c.brokerConstructorHasBatchProcessor(hasBatchProc, conf, mgr, log, stats, pipelines...)
		}
		if c.brokerConstructor != nil {
			return c.brokerConstructor(conf, mgr, log, stats, pipelines...)
		}
		var input Type
		var err error
		// TODO: V4 Remove this.
		if c.constructorHasBatchProcessor != nil {
			input, err = c.constructorHasBatchProcessor(hasBatchProc, conf, mgr, log, stats)
		} else {
			input, err = c.constructor(conf, mgr, log, stats)
		}
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
