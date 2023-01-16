package output

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all output types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label              string                  `json:"label" yaml:"label"`
	Type               string                  `json:"type" yaml:"type"`
	AMQP09             AMQPConfig              `json:"amqp_0_9" yaml:"amqp_0_9"`
	AMQP1              AMQP1Config             `json:"amqp_1" yaml:"amqp_1"`
	AWSDynamoDB        DynamoDBConfig          `json:"aws_dynamodb" yaml:"aws_dynamodb"`
	AWSKinesis         KinesisConfig           `json:"aws_kinesis" yaml:"aws_kinesis"`
	AWSKinesisFirehose KinesisFirehoseConfig   `json:"aws_kinesis_firehose" yaml:"aws_kinesis_firehose"`
	AWSS3              AmazonS3Config          `json:"aws_s3" yaml:"aws_s3"`
	AWSSNS             SNSConfig               `json:"aws_sns" yaml:"aws_sns"`
	AWSSQS             AmazonSQSConfig         `json:"aws_sqs" yaml:"aws_sqs"`
	AzureBlobStorage   AzureBlobStorageConfig  `json:"azure_blob_storage" yaml:"azure_blob_storage"`
	AzureQueueStorage  AzureQueueStorageConfig `json:"azure_queue_storage" yaml:"azure_queue_storage"`
	AzureTableStorage  AzureTableStorageConfig `json:"azure_table_storage" yaml:"azure_table_storage"`
	Broker             BrokerConfig            `json:"broker" yaml:"broker"`
	Cache              CacheConfig             `json:"cache" yaml:"cache"`
	Cassandra          CassandraConfig         `json:"cassandra" yaml:"cassandra"`
	Drop               DropConfig              `json:"drop" yaml:"drop"`
	DropOn             DropOnConfig            `json:"drop_on" yaml:"drop_on"`
	Dynamic            DynamicConfig           `json:"dynamic" yaml:"dynamic"`
	Elasticsearch      ElasticsearchConfig     `json:"elasticsearch" yaml:"elasticsearch"`
	Fallback           TryConfig               `json:"fallback" yaml:"fallback"`
	File               FileConfig              `json:"file" yaml:"file"`
	GCPCloudStorage    GCPCloudStorageConfig   `json:"gcp_cloud_storage" yaml:"gcp_cloud_storage"`
	GCPPubSub          GCPPubSubConfig         `json:"gcp_pubsub" yaml:"gcp_pubsub"`
	HDFS               HDFSConfig              `json:"hdfs" yaml:"hdfs"`
	HTTPServer         HTTPServerConfig        `json:"http_server" yaml:"http_server"`
	Inproc             string                  `json:"inproc" yaml:"inproc"`
	Kafka              KafkaConfig             `json:"kafka" yaml:"kafka"`
	MongoDB            MongoDBConfig           `json:"mongodb" yaml:"mongodb"`
	MQTT               MQTTConfig              `json:"mqtt" yaml:"mqtt"`
	Nanomsg            NanomsgConfig           `json:"nanomsg" yaml:"nanomsg"`
	NATS               NATSConfig              `json:"nats" yaml:"nats"`
	NATSStream         NATSStreamConfig        `json:"nats_stream" yaml:"nats_stream"`
	NSQ                NSQConfig               `json:"nsq" yaml:"nsq"`
	Plugin             any                     `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	RedisHash          RedisHashConfig         `json:"redis_hash" yaml:"redis_hash"`
	RedisList          RedisListConfig         `json:"redis_list" yaml:"redis_list"`
	RedisPubSub        RedisPubSubConfig       `json:"redis_pubsub" yaml:"redis_pubsub"`
	RedisStreams       RedisStreamsConfig      `json:"redis_streams" yaml:"redis_streams"`
	Reject             string                  `json:"reject" yaml:"reject"`
	Resource           string                  `json:"resource" yaml:"resource"`
	Retry              RetryConfig             `json:"retry" yaml:"retry"`
	SFTP               SFTPConfig              `json:"sftp" yaml:"sftp"`
	STDOUT             STDOUTConfig            `json:"stdout" yaml:"stdout"`
	Subprocess         SubprocessConfig        `json:"subprocess" yaml:"subprocess"`
	Switch             SwitchConfig            `json:"switch" yaml:"switch"`
	SyncResponse       struct{}                `json:"sync_response" yaml:"sync_response"`
	Socket             SocketConfig            `json:"socket" yaml:"socket"`
	Processors         []processor.Config      `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
func NewConfig() Config {
	return Config{
		Label:              "",
		Type:               "stdout",
		AMQP09:             NewAMQPConfig(),
		AMQP1:              NewAMQP1Config(),
		AWSDynamoDB:        NewDynamoDBConfig(),
		AWSKinesis:         NewKinesisConfig(),
		AWSKinesisFirehose: NewKinesisFirehoseConfig(),
		AWSS3:              NewAmazonS3Config(),
		AWSSNS:             NewSNSConfig(),
		AWSSQS:             NewAmazonSQSConfig(),
		AzureBlobStorage:   NewAzureBlobStorageConfig(),
		AzureQueueStorage:  NewAzureQueueStorageConfig(),
		AzureTableStorage:  NewAzureTableStorageConfig(),
		Broker:             NewBrokerConfig(),
		Cache:              NewCacheConfig(),
		Cassandra:          NewCassandraConfig(),
		Drop:               NewDropConfig(),
		DropOn:             NewDropOnConfig(),
		Dynamic:            NewDynamicConfig(),
		Elasticsearch:      NewElasticsearchConfig(),
		Fallback:           NewTryConfig(),
		File:               NewFileConfig(),
		GCPCloudStorage:    NewGCPCloudStorageConfig(),
		GCPPubSub:          NewGCPPubSubConfig(),
		HDFS:               NewHDFSConfig(),
		HTTPServer:         NewHTTPServerConfig(),
		Inproc:             "",
		Kafka:              NewKafkaConfig(),
		MQTT:               NewMQTTConfig(),
		MongoDB:            NewMongoDBConfig(),
		Nanomsg:            NewNanomsgConfig(),
		NATS:               NewNATSConfig(),
		NATSStream:         NewNATSStreamConfig(),
		NSQ:                NewNSQConfig(),
		Plugin:             nil,
		RedisHash:          NewRedisHashConfig(),
		RedisList:          NewRedisListConfig(),
		RedisPubSub:        NewRedisPubSubConfig(),
		RedisStreams:       NewRedisStreamsConfig(),
		Reject:             "",
		Resource:           "",
		Retry:              NewRetryConfig(),
		SFTP:               NewSFTPConfig(),
		STDOUT:             NewSTDOUTConfig(),
		Subprocess:         NewSubprocessConfig(),
		Switch:             NewSwitchConfig(),
		SyncResponse:       struct{}{},
		Socket:             NewSocketConfig(),
		Processors:         []processor.Config{},
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeOutput, value); err != nil {
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
