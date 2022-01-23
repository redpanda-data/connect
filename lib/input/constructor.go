package input

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
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

// Category describes the general category of an input.
type Category string

// Input categories
var (
	CategoryLocal    Category = "Local"
	CategoryAWS      Category = "AWS"
	CategoryGCP      Category = "GCP"
	CategoryAzure    Category = "Azure"
	CategoryServices Category = "Services"
	CategoryNetwork  Category = "Network"
	CategoryUtility  Category = "Utility"
)

// TypeSpec is a struct containing constructors, markdown descriptions and an
// optional sanitisation function for each input type.
type TypeSpec struct {
	constructor ConstructorFunc

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Categories  []Category
	Footnotes   string
	config      docs.FieldSpec
	FieldSpecs  docs.FieldSpecs
	Examples    []docs.AnnotatedExample
}

// ConstructorFunc is a func signature able to construct an input.
type ConstructorFunc func(bool, Config, types.Manager, log.Modular, metrics.Type, ...types.PipelineConstructorFunc) (Type, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.config
		if len(v.FieldSpecs) > 0 {
			conf = docs.FieldComponent().WithChildren(v.FieldSpecs.DefaultAndTypeFrom(inferred[k])...)
		} else {
			conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
		}
		spec := docs.ComponentSpec{
			Type:        docs.TypeInput,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Examples:    v.Examples,
			Status:      v.Status,
			Version:     v.Version,
		}
		if len(v.Categories) > 0 {
			spec.Categories = make([]string, 0, len(v.Categories))
			for _, cat := range v.Categories {
				spec.Categories = append(spec.Categories, string(cat))
			}
		}
		fn(v.constructor, spec)
	}
	for k, v := range pluginSpecs {
		spec := docs.ComponentSpec{
			Type:   docs.TypeInput,
			Name:   k,
			Status: docs.StatusExperimental,
			Plugin: true,
			Config: docs.FieldComponent().Unlinted(),
		}
		fn(v.constructor, spec)
	}
}

// AppendProcessorsFromConfig takes a variant arg of pipeline constructor
// functions and returns a new slice of them where the processors of the
// provided input configuration will also be initialized.
func AppendProcessorsFromConfig(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) []types.PipelineConstructorFunc {
	if len(conf.Processors) > 0 {
		pipelines = append([]types.PipelineConstructorFunc{func(i *int) (types.Pipeline, error) {
			if i == nil {
				procs := 0
				i = &procs
			}
			processors := make([]types.Processor, len(conf.Processors))
			for j, procConf := range conf.Processors {
				newMgr, newLog, newStats := interop.LabelChild(fmt.Sprintf("processor.%v", *i), mgr, log, stats)
				var err error
				processors[j], err = processor.New(procConf, newMgr, newLog, newStats)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
				*i++
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	return pipelines
}

// TODO: V4 Remove this.
func appendProcessorsFromConfigBatchAware(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (bool, []types.PipelineConstructorFunc) {
	if len(conf.Processors) > 0 {
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
				newMgr, newLog, newStats := interop.LabelChild(fmt.Sprintf("processor.%v", *i), mgr, log, stats)
				var err error
				processors[j], err = processor.New(procConf, newMgr, newLog, newStats)
				if err != nil {
					return nil, fmt.Errorf("failed to create processor '%v': %v", procConf.Type, err)
				}
				*i++
			}
			return pipeline.NewProcessor(log, stats, processors...), nil
		}}, pipelines...)
	}
	return hasBatchProc, pipelines
}

func fromSimpleConstructor(fn func(Config, types.Manager, log.Modular, metrics.Type) (Type, error)) ConstructorFunc {
	return func(
		hasBatchProc bool,
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelines ...types.PipelineConstructorFunc,
	) (Type, error) {
		input, err := fn(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %w", conf.Type, err)
		}
		pipelines = AppendProcessorsFromConfig(conf, mgr, log, stats, pipelines...)
		return WrapWithPipelines(input, pipelines...)
	}
}

func fromBatchAwareConstructor(fn func(bool, Config, types.Manager, log.Modular, metrics.Type) (Type, error)) ConstructorFunc {
	return func(
		hasBatchProc bool,
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
		pipelines ...types.PipelineConstructorFunc,
	) (Type, error) {
		input, err := fn(hasBatchProc, conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create input '%v': %w", conf.Type, err)
		}
		pipelines = AppendProcessorsFromConfig(conf, mgr, log, stats, pipelines...)
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
	TypeAMQP              = "amqp"
	TypeAMQP09            = "amqp_0_9"
	TypeAMQP1             = "amqp_1"
	TypeAWSKinesis        = "aws_kinesis"
	TypeAWSS3             = "aws_s3"
	TypeAWSSQS            = "aws_sqs"
	TypeAzureBlobStorage  = "azure_blob_storage"
	TypeAzureQueueStorage = "azure_queue_storage"
	TypeBloblang          = "bloblang"
	TypeBroker            = "broker"
	TypeCSVFile           = "csv"
	TypeDynamic           = "dynamic"
	TypeFile              = "file"
	TypeFiles             = "files"
	TypeGCPCloudStorage   = "gcp_cloud_storage"
	TypeGCPPubSub         = "gcp_pubsub"
	TypeGenerate          = "generate"
	TypeHDFS              = "hdfs"
	TypeHTTPClient        = "http_client"
	TypeHTTPServer        = "http_server"
	TypeInproc            = "inproc"
	TypeKafka             = "kafka"
	TypeKafkaBalanced     = "kafka_balanced"
	TypeKinesis           = "kinesis"
	TypeKinesisBalanced   = "kinesis_balanced"
	TypeMQTT              = "mqtt"
	TypeNanomsg           = "nanomsg"
	TypeNATS              = "nats"
	TypeNATSJetStream     = "nats_jetstream"
	TypeNATSStream        = "nats_stream"
	TypeNSQ               = "nsq"
	TypePulsar            = "pulsar"
	TypeReadUntil         = "read_until"
	TypeRedisList         = "redis_list"
	TypeRedisPubSub       = "redis_pubsub"
	TypeRedisStreams      = "redis_streams"
	TypeResource          = "resource"
	TypeS3                = "s3"
	TypeSequence          = "sequence"
	TypeSFTP              = "sftp"
	TypeSocket            = "socket"
	TypeSocketServer      = "socket_server"
	TypeSQS               = "sqs"
	TypeSTDIN             = "stdin"
	TypeSubprocess        = "subprocess"
	TypeTCP               = "tcp"
	TypeTCPServer         = "tcp_server"
	TypeUDPServer         = "udp_server"
	TypeWebsocket         = "websocket"
	TypeZMQ4              = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all input types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label             string                    `json:"label" yaml:"label"`
	Type              string                    `json:"type" yaml:"type"`
	AMQP              reader.AMQPConfig         `json:"amqp" yaml:"amqp"`
	AMQP09            reader.AMQP09Config       `json:"amqp_0_9" yaml:"amqp_0_9"`
	AMQP1             reader.AMQP1Config        `json:"amqp_1" yaml:"amqp_1"`
	AWSKinesis        AWSKinesisConfig          `json:"aws_kinesis" yaml:"aws_kinesis"`
	AWSS3             AWSS3Config               `json:"aws_s3" yaml:"aws_s3"`
	AWSSQS            AWSSQSConfig              `json:"aws_sqs" yaml:"aws_sqs"`
	AzureBlobStorage  AzureBlobStorageConfig    `json:"azure_blob_storage" yaml:"azure_blob_storage"`
	AzureQueueStorage AzureQueueStorageConfig   `json:"azure_queue_storage" yaml:"azure_queue_storage"`
	Bloblang          BloblangConfig            `json:"bloblang" yaml:"bloblang"`
	Broker            BrokerConfig              `json:"broker" yaml:"broker"`
	CSVFile           CSVFileConfig             `json:"csv" yaml:"csv"`
	Dynamic           DynamicConfig             `json:"dynamic" yaml:"dynamic"`
	File              FileConfig                `json:"file" yaml:"file"`
	Files             reader.FilesConfig        `json:"files" yaml:"files"`
	GCPCloudStorage   GCPCloudStorageConfig     `json:"gcp_cloud_storage" yaml:"gcp_cloud_storage"`
	GCPPubSub         reader.GCPPubSubConfig    `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	Generate          BloblangConfig            `json:"generate" yaml:"generate"`
	HDFS              reader.HDFSConfig         `json:"hdfs" yaml:"hdfs"`
	HTTPClient        HTTPClientConfig          `json:"http_client" yaml:"http_client"`
	HTTPServer        HTTPServerConfig          `json:"http_server" yaml:"http_server"`
	Inproc            InprocConfig              `json:"inproc" yaml:"inproc"`
	Kafka             reader.KafkaConfig        `json:"kafka" yaml:"kafka"`
	MQTT              reader.MQTTConfig         `json:"mqtt" yaml:"mqtt"`
	Nanomsg           reader.ScaleProtoConfig   `json:"nanomsg" yaml:"nanomsg"`
	NATS              reader.NATSConfig         `json:"nats" yaml:"nats"`
	NATSJetStream     NATSJetStreamConfig       `json:"nats_jetstream" yaml:"nats_jetstream"`
	NATSStream        reader.NATSStreamConfig   `json:"nats_stream" yaml:"nats_stream"`
	NSQ               reader.NSQConfig          `json:"nsq" yaml:"nsq"`
	Plugin            interface{}               `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Pulsar            PulsarConfig              `json:"pulsar" yaml:"pulsar"`
	ReadUntil         ReadUntilConfig           `json:"read_until" yaml:"read_until"`
	RedisList         reader.RedisListConfig    `json:"redis_list" yaml:"redis_list"`
	RedisPubSub       reader.RedisPubSubConfig  `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams      reader.RedisStreamsConfig `json:"redis_streams" yaml:"redis_streams"`
	Resource          string                    `json:"resource" yaml:"resource"`
	S3                reader.AmazonS3Config     `json:"s3" yaml:"s3"`
	Sequence          SequenceConfig            `json:"sequence" yaml:"sequence"`
	SFTP              SFTPConfig                `json:"sftp" yaml:"sftp"`
	Socket            SocketConfig              `json:"socket" yaml:"socket"`
	SocketServer      SocketServerConfig        `json:"socket_server" yaml:"socket_server"`
	SQS               reader.AmazonSQSConfig    `json:"sqs" yaml:"sqs"`
	STDIN             STDINConfig               `json:"stdin" yaml:"stdin"`
	Subprocess        SubprocessConfig          `json:"subprocess" yaml:"subprocess"`
	Websocket         reader.WebsocketConfig    `json:"websocket" yaml:"websocket"`
	ZMQ4              *reader.ZMQ4Config        `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors        []processor.Config        `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
func NewConfig() Config {
	return Config{
		Label:             "",
		Type:              "stdin",
		AMQP:              reader.NewAMQPConfig(),
		AMQP09:            reader.NewAMQP09Config(),
		AMQP1:             reader.NewAMQP1Config(),
		AWSKinesis:        NewAWSKinesisConfig(),
		AWSS3:             NewAWSS3Config(),
		AWSSQS:            NewAWSSQSConfig(),
		AzureBlobStorage:  NewAzureBlobStorageConfig(),
		AzureQueueStorage: NewAzureQueueStorageConfig(),
		Bloblang:          NewBloblangConfig(),
		Broker:            NewBrokerConfig(),
		CSVFile:           NewCSVFileConfig(),
		Dynamic:           NewDynamicConfig(),
		File:              NewFileConfig(),
		Files:             reader.NewFilesConfig(),
		GCPCloudStorage:   NewGCPCloudStorageConfig(),
		GCPPubSub:         reader.NewGCPPubSubConfig(),
		Generate:          NewBloblangConfig(),
		HDFS:              reader.NewHDFSConfig(),
		HTTPClient:        NewHTTPClientConfig(),
		HTTPServer:        NewHTTPServerConfig(),
		Inproc:            NewInprocConfig(),
		Kafka:             reader.NewKafkaConfig(),
		MQTT:              reader.NewMQTTConfig(),
		Nanomsg:           reader.NewScaleProtoConfig(),
		NATS:              reader.NewNATSConfig(),
		NATSJetStream:     NewNATSJetStreamConfig(),
		NATSStream:        reader.NewNATSStreamConfig(),
		NSQ:               reader.NewNSQConfig(),
		Plugin:            nil,
		Pulsar:            NewPulsarConfig(),
		ReadUntil:         NewReadUntilConfig(),
		RedisList:         reader.NewRedisListConfig(),
		RedisPubSub:       reader.NewRedisPubSubConfig(),
		RedisStreams:      reader.NewRedisStreamsConfig(),
		Resource:          "",
		S3:                reader.NewAmazonS3Config(),
		Sequence:          NewSequenceConfig(),
		SFTP:              NewSFTPConfig(),
		Socket:            NewSocketConfig(),
		SocketServer:      NewSocketServerConfig(),
		SQS:               reader.NewAmazonSQSConfig(),
		STDIN:             NewSTDINConfig(),
		Subprocess:        NewSubprocessConfig(),
		Websocket:         reader.NewWebsocketConfig(),
		ZMQ4:              reader.NewZMQ4Config(),
		Processors:        []processor.Config{},
	}
}

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	outputMap, err := config.SanitizeComponent(conf)
	if err != nil {
		return nil, err
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		if spec.confSanitiser != nil {
			outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
		}
	}
	if err := docs.SanitiseComponentConfig(
		docs.TypeInput,
		map[string]interface{}(outputMap),
		docs.ShouldDropDeprecated(removeDeprecated),
	); err != nil {
		return nil, err
	}
	return outputMap, nil
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeInput, aliased.Type, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
			conf := spec.confConstructor()
			if err = pluginNode.Decode(conf); err != nil {
				return fmt.Errorf("line %v: %v", value.Line, err)
			}
			aliased.Plugin = conf
		} else {
			aliased.Plugin = &pluginNode
		}
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
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	return newHasBatchProcessor(false, conf, mgr, log, stats, pipelines...)
}

// Deprecated: This is a hack for until the batch processor is removed.
// TODO: V4 Remove this.
func newHasBatchProcessor(
	hasBatchProc bool,
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
	pipelines ...types.PipelineConstructorFunc,
) (Type, error) {
	if mgrV2, ok := mgr.(interface {
		NewInput(Config, bool, ...types.PipelineConstructorFunc) (types.Input, error)
	}); ok {
		return mgrV2.NewInput(conf, hasBatchProc, pipelines...)
	}
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(hasBatchProc, conf, mgr, log, stats, pipelines...)
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		return c.constructor(hasBatchProc, conf, mgr, log, stats, pipelines...)
	}
	return nil, types.ErrInvalidInputType
}

//------------------------------------------------------------------------------
