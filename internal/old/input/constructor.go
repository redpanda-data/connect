package input

import (
	"fmt"
	"strconv"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
	"github.com/benthosdev/benthos/v4/internal/pipeline"
)

// TypeSpec is a struct containing constructors, markdown descriptions and an
// optional sanitisation function for each input type.
type TypeSpec struct {
	constructor ConstructorFunc

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Categories  []string
	Footnotes   string
	Config      docs.FieldSpec
	Examples    []docs.AnnotatedExample
}

// ConstructorFunc is a func signature able to construct an input.
type ConstructorFunc func(Config, interop.Manager, log.Modular, metrics.Type, ...iprocessor.PipelineConstructorFunc) (input.Streamed, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.Config
		conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
		spec := docs.ComponentSpec{
			Type:        docs.TypeInput,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Categories:  v.Categories,
			Config:      conf,
			Examples:    v.Examples,
			Status:      v.Status,
			Version:     v.Version,
		}
		fn(v.constructor, spec)
	}
}

// AppendProcessorsFromConfig takes a variant arg of pipeline constructor
// functions and returns a new slice of them where the processors of the
// provided input configuration will also be initialized.
func AppendProcessorsFromConfig(conf Config, mgr interop.Manager, pipelines ...iprocessor.PipelineConstructorFunc) []iprocessor.PipelineConstructorFunc {
	if len(conf.Processors) > 0 {
		pipelines = append([]iprocessor.PipelineConstructorFunc{func() (iprocessor.Pipeline, error) {
			processors := make([]iprocessor.V1, len(conf.Processors))
			for j, procConf := range conf.Processors {
				newMgr := mgr.IntoPath("processors", strconv.Itoa(j))
				var err error
				processors[j], err = processor.New(procConf, newMgr, newMgr.Logger(), newMgr.Metrics())
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
			}
			return pipeline.NewProcessor(processors...), nil
		}}, pipelines...)
	}
	return pipelines
}

func fromSimpleConstructor(fn func(Config, interop.Manager, log.Modular, metrics.Type) (input.Streamed, error)) ConstructorFunc {
	return func(
		conf Config,
		mgr interop.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelines ...iprocessor.PipelineConstructorFunc,
	) (input.Streamed, error) {
		input, err := fn(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %w", conf.Type, err)
		}
		pipelines = AppendProcessorsFromConfig(conf, mgr, pipelines...)
		return WrapWithPipelines(input, pipelines...)
	}
}

// Constructors is a map of all input types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each input type.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
const (
	TypeAMQP09            = "amqp_0_9"
	TypeAWSKinesis        = "aws_kinesis"
	TypeAWSS3             = "aws_s3"
	TypeAWSSQS            = "aws_sqs"
	TypeAzureBlobStorage  = "azure_blob_storage"
	TypeAzureQueueStorage = "azure_queue_storage"
	TypeBroker            = "broker"
	TypeCSVFile           = "csv"
	TypeDynamic           = "dynamic"
	TypeFile              = "file"
	TypeGCPCloudStorage   = "gcp_cloud_storage"
	TypeGCPPubSub         = "gcp_pubsub"
	TypeGenerate          = "generate"
	TypeHDFS              = "hdfs"
	TypeHTTPClient        = "http_client"
	TypeHTTPServer        = "http_server"
	TypeInproc            = "inproc"
	TypeKafka             = "kafka"
	TypeKinesis           = "kinesis"
	TypeMQTT              = "mqtt"
	TypeNanomsg           = "nanomsg"
	TypeNATS              = "nats"
	TypeNATSJetStream     = "nats_jetstream"
	TypeNATSStream        = "nats_stream"
	TypeNSQ               = "nsq"
	TypeReadUntil         = "read_until"
	TypeRedisList         = "redis_list"
	TypeRedisPubSub       = "redis_pubsub"
	TypeRedisStreams      = "redis_streams"
	TypeResource          = "resource"
	TypeSequence          = "sequence"
	TypeSFTP              = "sftp"
	TypeSocket            = "socket"
	TypeSocketServer      = "socket_server"
	TypeSTDIN             = "stdin"
	TypeSubprocess        = "subprocess"
	TypeWebsocket         = "websocket"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label             string                    `json:"label" yaml:"label"`
	Type              string                    `json:"type" yaml:"type"`
	AMQP09            AMQP09Config              `json:"amqp_0_9" yaml:"amqp_0_9"`
	AMQP1             AMQP1Config               `json:"amqp_1" yaml:"amqp_1"`
	AWSKinesis        AWSKinesisConfig          `json:"aws_kinesis" yaml:"aws_kinesis"`
	AWSS3             AWSS3Config               `json:"aws_s3" yaml:"aws_s3"`
	AWSSQS            AWSSQSConfig              `json:"aws_sqs" yaml:"aws_sqs"`
	AzureBlobStorage  AzureBlobStorageConfig    `json:"azure_blob_storage" yaml:"azure_blob_storage"`
	AzureQueueStorage AzureQueueStorageConfig   `json:"azure_queue_storage" yaml:"azure_queue_storage"`
	Broker            BrokerConfig              `json:"broker" yaml:"broker"`
	CSVFile           CSVFileConfig             `json:"csv" yaml:"csv"`
	Dynamic           DynamicConfig             `json:"dynamic" yaml:"dynamic"`
	File              FileConfig                `json:"file" yaml:"file"`
	GCPCloudStorage   GCPCloudStorageConfig     `json:"gcp_cloud_storage" yaml:"gcp_cloud_storage"`
	GCPPubSub         reader.GCPPubSubConfig    `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	Generate          BloblangConfig            `json:"generate" yaml:"generate"`
	HDFS              reader.HDFSConfig         `json:"hdfs" yaml:"hdfs"`
	HTTPClient        HTTPClientConfig          `json:"http_client" yaml:"http_client"`
	HTTPServer        HTTPServerConfig          `json:"http_server" yaml:"http_server"`
	Inproc            InprocConfig              `json:"inproc" yaml:"inproc"`
	Kafka             KafkaConfig               `json:"kafka" yaml:"kafka"`
	MQTT              MQTTConfig                `json:"mqtt" yaml:"mqtt"`
	Nanomsg           reader.ScaleProtoConfig   `json:"nanomsg" yaml:"nanomsg"`
	NATS              reader.NATSConfig         `json:"nats" yaml:"nats"`
	NATSStream        reader.NATSStreamConfig   `json:"nats_stream" yaml:"nats_stream"`
	NSQ               reader.NSQConfig          `json:"nsq" yaml:"nsq"`
	Plugin            interface{}               `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ReadUntil         ReadUntilConfig           `json:"read_until" yaml:"read_until"`
	RedisList         reader.RedisListConfig    `json:"redis_list" yaml:"redis_list"`
	RedisPubSub       reader.RedisPubSubConfig  `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams      reader.RedisStreamsConfig `json:"redis_streams" yaml:"redis_streams"`
	Resource          string                    `json:"resource" yaml:"resource"`
	Sequence          SequenceConfig            `json:"sequence" yaml:"sequence"`
	SFTP              SFTPConfig                `json:"sftp" yaml:"sftp"`
	Socket            SocketConfig              `json:"socket" yaml:"socket"`
	SocketServer      SocketServerConfig        `json:"socket_server" yaml:"socket_server"`
	STDIN             STDINConfig               `json:"stdin" yaml:"stdin"`
	Subprocess        SubprocessConfig          `json:"subprocess" yaml:"subprocess"`
	Websocket         reader.WebsocketConfig    `json:"websocket" yaml:"websocket"`
	Processors        []processor.Config        `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
func NewConfig() Config {
	return Config{
		Label:             "",
		Type:              "stdin",
		AMQP09:            NewAMQP09Config(),
		AMQP1:             NewAMQP1Config(),
		AWSKinesis:        NewAWSKinesisConfig(),
		AWSS3:             NewAWSS3Config(),
		AWSSQS:            NewAWSSQSConfig(),
		AzureBlobStorage:  NewAzureBlobStorageConfig(),
		AzureQueueStorage: NewAzureQueueStorageConfig(),
		Broker:            NewBrokerConfig(),
		CSVFile:           NewCSVFileConfig(),
		Dynamic:           NewDynamicConfig(),
		File:              NewFileConfig(),
		GCPCloudStorage:   NewGCPCloudStorageConfig(),
		GCPPubSub:         reader.NewGCPPubSubConfig(),
		Generate:          NewBloblangConfig(),
		HDFS:              reader.NewHDFSConfig(),
		HTTPClient:        NewHTTPClientConfig(),
		HTTPServer:        NewHTTPServerConfig(),
		Inproc:            NewInprocConfig(),
		Kafka:             NewKafkaConfig(),
		MQTT:              NewMQTTConfig(),
		Nanomsg:           reader.NewScaleProtoConfig(),
		NATS:              reader.NewNATSConfig(),
		NATSStream:        reader.NewNATSStreamConfig(),
		NSQ:               reader.NewNSQConfig(),
		Plugin:            nil,
		ReadUntil:         NewReadUntilConfig(),
		RedisList:         reader.NewRedisListConfig(),
		RedisPubSub:       reader.NewRedisPubSubConfig(),
		RedisStreams:      reader.NewRedisStreamsConfig(),
		Resource:          "",
		Sequence:          NewSequenceConfig(),
		SFTP:              NewSFTPConfig(),
		Socket:            NewSocketConfig(),
		SocketServer:      NewSocketServerConfig(),
		STDIN:             NewSTDINConfig(),
		Subprocess:        NewSubprocessConfig(),
		Websocket:         reader.NewWebsocketConfig(),
		Processors:        []processor.Config{},
	}
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeInput, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...iprocessor.PipelineConstructorFunc,
) (input.Streamed, error) {
	if mgrV2, ok := mgr.(interface {
		NewInput(Config, ...iprocessor.PipelineConstructorFunc) (input.Streamed, error)
	}); ok {
		return mgrV2.NewInput(conf, pipelines...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats, pipelines...)
	}
	return nil, component.ErrInvalidType("input", conf.Type)
}
