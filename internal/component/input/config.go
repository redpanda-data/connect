package input

import (
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all input types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label             string                  `json:"label" yaml:"label"`
	Type              string                  `json:"type" yaml:"type"`
	AMQP09            AMQP09Config            `json:"amqp_0_9" yaml:"amqp_0_9"`
	AMQP1             AMQP1Config             `json:"amqp_1" yaml:"amqp_1"`
	AWSKinesis        AWSKinesisConfig        `json:"aws_kinesis" yaml:"aws_kinesis"`
	AWSS3             AWSS3Config             `json:"aws_s3" yaml:"aws_s3"`
	AWSSQS            AWSSQSConfig            `json:"aws_sqs" yaml:"aws_sqs"`
	AzureBlobStorage  AzureBlobStorageConfig  `json:"azure_blob_storage" yaml:"azure_blob_storage"`
	AzureQueueStorage AzureQueueStorageConfig `json:"azure_queue_storage" yaml:"azure_queue_storage"`
	AzureTableStorage AzureTableStorageConfig `json:"azure_table_storage" yaml:"azure_table_storage"`
	Broker            BrokerConfig            `json:"broker" yaml:"broker"`
	CSVFile           CSVFileConfig           `json:"csv" yaml:"csv"`
	Dynamic           DynamicConfig           `json:"dynamic" yaml:"dynamic"`
	File              FileConfig              `json:"file" yaml:"file"`
	GCPCloudStorage   GCPCloudStorageConfig   `json:"gcp_cloud_storage" yaml:"gcp_cloud_storage"`
	GCPPubSub         GCPPubSubConfig         `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	Generate          GenerateConfig          `json:"generate" yaml:"generate"`
	HDFS              HDFSConfig              `json:"hdfs" yaml:"hdfs"`
	HTTPServer        HTTPServerConfig        `json:"http_server" yaml:"http_server"`
	Inproc            InprocConfig            `json:"inproc" yaml:"inproc"`
	Kafka             KafkaConfig             `json:"kafka" yaml:"kafka"`
	MQTT              MQTTConfig              `json:"mqtt" yaml:"mqtt"`
	Nanomsg           NanomsgConfig           `json:"nanomsg" yaml:"nanomsg"`
	NATS              NATSConfig              `json:"nats" yaml:"nats"`
	NATSStream        NATSStreamConfig        `json:"nats_stream" yaml:"nats_stream"`
	NSQ               NSQConfig               `json:"nsq" yaml:"nsq"`
	Plugin            any                     `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	ReadUntil         ReadUntilConfig         `json:"read_until" yaml:"read_until"`
	RedisPubSub       RedisPubSubConfig       `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams      RedisStreamsConfig      `json:"redis_streams" yaml:"redis_streams"`
	Resource          string                  `json:"resource" yaml:"resource"`
	Sequence          SequenceConfig          `json:"sequence" yaml:"sequence"`
	SFTP              SFTPConfig              `json:"sftp" yaml:"sftp"`
	Socket            SocketConfig            `json:"socket" yaml:"socket"`
	SocketServer      SocketServerConfig      `json:"socket_server" yaml:"socket_server"`
	STDIN             STDINConfig             `json:"stdin" yaml:"stdin"`
	Subprocess        SubprocessConfig        `json:"subprocess" yaml:"subprocess"`
	Processors        []processor.Config      `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
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
		GCPPubSub:         NewGCPPubSubConfig(),
		Generate:          NewGenerateConfig(),
		HDFS:              NewHDFSConfig(),
		HTTPServer:        NewHTTPServerConfig(),
		Inproc:            NewInprocConfig(),
		Kafka:             NewKafkaConfig(),
		MQTT:              NewMQTTConfig(),
		Nanomsg:           NewNanomsgConfig(),
		NATS:              NewNATSConfig(),
		NATSStream:        NewNATSStreamConfig(),
		NSQ:               NewNSQConfig(),
		Plugin:            nil,
		ReadUntil:         NewReadUntilConfig(),
		RedisPubSub:       NewRedisPubSubConfig(),
		RedisStreams:      NewRedisStreamsConfig(),
		Resource:          "",
		Sequence:          NewSequenceConfig(),
		SFTP:              NewSFTPConfig(),
		Socket:            NewSocketConfig(),
		SocketServer:      NewSocketServerConfig(),
		STDIN:             NewSTDINConfig(),
		Subprocess:        NewSubprocessConfig(),
		Processors:        []processor.Config{},
	}
}

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeInput, value); err != nil {
		return docs.NewLintError(value.Line, docs.LintComponentMissing, err.Error())
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}
