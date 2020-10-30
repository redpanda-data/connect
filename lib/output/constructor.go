package output

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/pipeline"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Category describes the general category of an output.
type Category string

// Output categories
var (
	CategoryLocal    Category = "Local"
	CategoryAWS      Category = "AWS"
	CategoryGCP      Category = "GCP"
	CategoryAzure    Category = "Azure"
	CategoryServices Category = "Services"
	CategoryNetwork  Category = "Network"
	CategoryUtility  Category = "Utility"
)

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
	sanitiseConfigFunc func(conf Config) (interface{}, error)

	// Async indicates whether this output benefits from sending multiple
	// messages asynchronously over the protocol.
	Async bool

	// Batches indicates whether this output benefits from batching of messages.
	Batches bool

	Status      docs.Status
	Summary     string
	Description string
	Categories  []Category
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
	Examples    []docs.AnnotatedExample
}

// Constructors is a map of all output types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each output type.
const (
	TypeAMQP            = "amqp"
	TypeAMQP09          = "amqp_0_9"
	TypeAMQP1           = "amqp_1"
	TypeBlobStorage     = "blob_storage"
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
	TypeGRPCClient      = "grpc_client"
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
	TypeResource        = "resource"
	TypeRetry           = "retry"
	TypeS3              = "s3"
	TypeSNS             = "sns"
	TypeSQS             = "sqs"
	TypeSTDOUT          = "stdout"
	TypeSubprocess      = "subprocess"
	TypeSwitch          = "switch"
	TypeSyncResponse    = "sync_response"
	TypeTableStorage    = "table_storage"
	TypeTCP             = "tcp"
	TypeTry             = "try"
	TypeUDP             = "udp"
	TypeSocket          = "socket"
	TypeWebsocket       = "websocket"
	TypeZMQ4            = "zmq4"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all output types.
type Config struct {
	Type            string                         `json:"type" yaml:"type"`
	AMQP            writer.AMQPConfig              `json:"amqp" yaml:"amqp"`
	AMQP09          writer.AMQPConfig              `json:"amqp_0_9" yaml:"amqp_0_9"`
	AMQP1           writer.AMQP1Config             `json:"amqp_1" yaml:"amqp_1"`
	BlobStorage     writer.AzureBlobStorageConfig  `json:"blob_storage" yaml:"blob_storage"`
	Broker          BrokerConfig                   `json:"broker" yaml:"broker"`
	Cache           writer.CacheConfig             `json:"cache" yaml:"cache"`
	Drop            writer.DropConfig              `json:"drop" yaml:"drop"`
	DropOnError     DropOnErrorConfig              `json:"drop_on_error" yaml:"drop_on_error"`
	Dynamic         DynamicConfig                  `json:"dynamic" yaml:"dynamic"`
	DynamoDB        writer.DynamoDBConfig          `json:"dynamodb" yaml:"dynamodb"`
	Elasticsearch   writer.ElasticsearchConfig     `json:"elasticsearch" yaml:"elasticsearch"`
	File            FileConfig                     `json:"file" yaml:"file"`
	Files           writer.FilesConfig             `json:"files" yaml:"files"`
	GCPPubSub       writer.GCPPubSubConfig         `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	GRPCClient      writer.GRPCClientConfig        `json:"grpc_client" yaml:"grpc_client"`
	HDFS            writer.HDFSConfig              `json:"hdfs" yaml:"hdfs"`
	HTTPClient      writer.HTTPClientConfig        `json:"http_client" yaml:"http_client"`
	HTTPServer      HTTPServerConfig               `json:"http_server" yaml:"http_server"`
	Inproc          InprocConfig                   `json:"inproc" yaml:"inproc"`
	Kafka           writer.KafkaConfig             `json:"kafka" yaml:"kafka"`
	Kinesis         writer.KinesisConfig           `json:"kinesis" yaml:"kinesis"`
	KinesisFirehose writer.KinesisFirehoseConfig   `json:"kinesis_firehose" yaml:"kinesis_firehose"`
	MQTT            writer.MQTTConfig              `json:"mqtt" yaml:"mqtt"`
	Nanomsg         writer.NanomsgConfig           `json:"nanomsg" yaml:"nanomsg"`
	NATS            writer.NATSConfig              `json:"nats" yaml:"nats"`
	NATSStream      writer.NATSStreamConfig        `json:"nats_stream" yaml:"nats_stream"`
	NSQ             writer.NSQConfig               `json:"nsq" yaml:"nsq"`
	Plugin          interface{}                    `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	RedisHash       writer.RedisHashConfig         `json:"redis_hash" yaml:"redis_hash"`
	RedisList       writer.RedisListConfig         `json:"redis_list" yaml:"redis_list"`
	RedisPubSub     writer.RedisPubSubConfig       `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams    writer.RedisStreamsConfig      `json:"redis_streams" yaml:"redis_streams"`
	Resource        string                         `json:"resource" yaml:"resource"`
	Retry           RetryConfig                    `json:"retry" yaml:"retry"`
	S3              writer.AmazonS3Config          `json:"s3" yaml:"s3"`
	SNS             writer.SNSConfig               `json:"sns" yaml:"sns"`
	SQS             writer.AmazonSQSConfig         `json:"sqs" yaml:"sqs"`
	STDOUT          STDOUTConfig                   `json:"stdout" yaml:"stdout"`
	Subprocess      SubprocessConfig               `json:"subprocess" yaml:"subprocess"`
	Switch          SwitchConfig                   `json:"switch" yaml:"switch"`
	SyncResponse    struct{}                       `json:"sync_response" yaml:"sync_response"`
	TableStorage    writer.AzureTableStorageConfig `json:"table_storage" yaml:"table_storage"`
	TCP             writer.TCPConfig               `json:"tcp" yaml:"tcp"`
	Try             TryConfig                      `json:"try" yaml:"try"`
	UDP             writer.UDPConfig               `json:"udp" yaml:"udp"`
	Socket          writer.SocketConfig            `json:"socket" yaml:"socket"`
	Websocket       writer.WebsocketConfig         `json:"websocket" yaml:"websocket"`
	ZMQ4            *writer.ZMQ4Config             `json:"zmq4,omitempty" yaml:"zmq4,omitempty"`
	Processors      []processor.Config             `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:            "stdout",
		AMQP:            writer.NewAMQPConfig(),
		AMQP09:          writer.NewAMQPConfig(),
		AMQP1:           writer.NewAMQP1Config(),
		BlobStorage:     writer.NewAzureBlobStorageConfig(),
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
		Resource:        "",
		Retry:           NewRetryConfig(),
		S3:              writer.NewAmazonS3Config(),
		SNS:             writer.NewSNSConfig(),
		SQS:             writer.NewAmazonSQSConfig(),
		STDOUT:          NewSTDOUTConfig(),
		Subprocess:      NewSubprocessConfig(),
		Switch:          NewSwitchConfig(),
		SyncResponse:    struct{}{},
		TableStorage:    writer.NewAzureTableStorageConfig(),
		TCP:             writer.NewTCPConfig(),
		Try:             NewTryConfig(),
		UDP:             writer.NewUDPConfig(),
		Socket:          writer.NewSocketConfig(),
		Websocket:       writer.NewWebsocketConfig(),
		ZMQ4:            writer.NewZMQ4Config(),
		Processors:      []processor.Config{},
	}
}

//------------------------------------------------------------------------------

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
	if sfunc := Constructors[conf.Type].sanitiseConfigFunc; sfunc != nil {
		if outputMap[conf.Type], err = sfunc(conf); err != nil {
			return nil, err
		}
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		if spec.confSanitiser != nil {
			outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
		}
	}
	if removeDeprecated {
		Constructors[conf.Type].FieldSpecs.RemoveDeprecated(outputMap)
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
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
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
		if !exists {
			if _, exists = Constructors[aliased.Type]; !exists {
				return fmt.Errorf("line %v: type '%v' was not recognised", value.Line, aliased.Type)
			}
		}
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

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
