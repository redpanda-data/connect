package kafka

import (
	"context"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/syncmap"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/span"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	oskFieldAddresses                    = "addresses"
	oskFieldTopic                        = "topic"
	oskFieldTargetVersion                = "target_version"
	oskFieldTLS                          = "tls"
	oskFieldClientID                     = "client_id"
	oskFieldRackID                       = "rack_id"
	oskFieldKey                          = "key"
	oskFieldPartitioner                  = "partitioner"
	oskFieldPartition                    = "partition"
	oskFieldCustomTopic                  = "custom_topic_creation"
	oskFieldCustomTopicEnabled           = "enabled"
	oskFieldCustomTopicPartitions        = "partitions"
	oskFieldCustomTopicReplicationFactor = "replication_factor"
	oskFieldCompression                  = "compression"
	oskFieldStaticHeaders                = "static_headers"
	oskFieldMetadata                     = "metadata"
	oskFieldAckReplicas                  = "ack_replicas"
	oskFieldMaxMsgBytes                  = "max_msg_bytes"
	oskFieldTimeout                      = "timeout"
	oskFieldIdempotentWrite              = "idempotent_write"
	oskFieldRetryAsBatch                 = "retry_as_batch"
	oskFieldBatching                     = "batching"
	oskFieldMaxRetries                   = "max_retries"
	oskFieldBackoff                      = "backoff"
)

// OSKConfigSpec creates a new config spec for a kafka output.
func OSKConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`The kafka output type writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.`).
		Description(output.Description(true, true, `
The config field `+"`ack_replicas`"+` determines whether we wait for acknowledgement from all replicas or just a single broker.

Both the `+"`key` and `topic`"+` fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

[Metadata](/docs/configuration/metadata) will be added to each message sent as headers (version 0.11+), but can be restricted using the field `+"[`metadata`](#metadata)"+`.

### Strict Ordering and Retries

When strict ordering is required for messages written to topic partitions it is important to ensure that both the field `+"`max_in_flight` is set to `1` and that the field `retry_as_batch` is set to `true`"+`.

You must also ensure that failed batches are never rerouted back to the same output. This can be done by setting the field `+"`max_retries` to `0` and `backoff.max_elapsed_time`"+` to empty, which will apply back pressure indefinitely until the batch is sent successfully.

However, this also means that manual intervention will eventually be required in cases where the batch cannot be sent due to configuration problems such as an incorrect `+"`max_msg_bytes`"+` estimate. A less strict but automated alternative would be to route failed batches to a dead letter queue using a `+"[`fallback` broker](/docs/components/outputs/fallback)"+`, but this would allow subsequent batches to be delivered in the meantime whilst those failed batches are dealt with.

### Troubleshooting

If you're seeing issues writing to or reading from Kafka with this component then it's worth trying out the newer `+"[`kafka_franz` output](/docs/components/outputs/kafka_franz)"+`.

- I'm seeing logs that report `+"`Failed to connect to kafka: kafka: client has run out of available brokers to talk to (Is your cluster reachable?)`"+`, but the brokers are definitely reachable.

Unfortunately this error message will appear for a wide range of connection problems even when the broker endpoint can be reached. Double check your authentication configuration and also ensure that you have [enabled TLS](#tlsenabled) if applicable.`)).
		Fields(
			service.NewStringListField(oskFieldAddresses).
				Description("A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.").
				Examples(
					[]string{"localhost:9092"},
					[]string{"localhost:9041,localhost:9042"},
					[]string{"localhost:9041", "localhost:9042"},
				),
			service.NewTLSToggledField(oskFieldTLS),
			SaramaSASLField(),
			service.NewInterpolatedStringField(oskFieldTopic).
				Description("The topic to publish messages to."),
			service.NewStringField(oskFieldClientID).
				Description("An identifier for the client connection.").
				Advanced().Default("benthos"),
			service.NewStringField(oskFieldTargetVersion).
				Description("The version of the Kafka protocol to use. This limits the capabilities used by the client and should ideally match the version of your brokers. Defaults to the oldest supported stable version.").
				Examples(sarama.DefaultVersion.String(), "3.1.0").
				Optional(),
			service.NewStringField(oskFieldRackID).
				Description("A rack identifier for this client.").
				Advanced().Default(""),
			service.NewInterpolatedStringField(oskFieldKey).
				Description("The key to publish messages with.").
				Default(""),
			service.NewStringEnumField(oskFieldPartitioner, "fnv1a_hash", "murmur2_hash", "random", "round_robin", "manual").
				Description("The partitioning algorithm to use.").
				Default("fnv1a_hash"),
			service.NewInterpolatedStringField(oskFieldPartition).
				Description("The manually-specified partition to publish messages to, relevant only when the field `partitioner` is set to `manual`. Must be able to parse as a 32-bit integer.").
				Advanced().Default(""),
			service.NewObjectField(oskFieldCustomTopic,
				service.NewBoolField(oskFieldCustomTopicEnabled).
					Description("Whether to enable custom topic creation.").Default(false),
				service.NewIntField(oskFieldCustomTopicPartitions).
					Description("The number of partitions to create for new topics. Leave at -1 to use the broker configured default. Must be >= 1.").
					Default(-1),
				service.NewIntField(oskFieldCustomTopicReplicationFactor).
					Description("The replication factor to use for new topics. Leave at -1 to use the broker configured default. Must be an odd number, and less then or equal to the number of brokers.").
					Default(-1),
			).Description("If enabled, topics will be created with the specified number of partitions and replication factor if they do not already exist.").
				Advanced().Optional(),
			service.NewStringEnumField(oskFieldCompression, "none", "snappy", "lz4", "gzip", "zstd").
				Description("The compression algorithm to use.").
				Default("none"),
			service.NewStringMapField(oskFieldStaticHeaders).
				Description("An optional map of static headers that should be added to messages in addition to metadata.").
				Example(map[string]string{"first-static-header": "value-1", "second-static-header": "value-2"}).
				Optional(),
			service.NewMetadataExcludeFilterField(oskFieldMetadata).
				Description("Specify criteria for which metadata values are sent with messages as headers."),
			span.InjectTracingSpanMappingDocs(),
			service.NewOutputMaxInFlightField(),
			service.NewBoolField(oskFieldIdempotentWrite).
				Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
				Default(false).
				Advanced(),
			service.NewBoolField(oskFieldAckReplicas).
				Description("Ensure that messages have been copied across all replicas before acknowledging receipt.").
				Advanced().Default(false),
			service.NewIntField(oskFieldMaxMsgBytes).
				Description("The maximum size in bytes of messages sent to the target topic.").
				Advanced().Default(1000000),
			service.NewDurationField(oskFieldTimeout).
				Description("The maximum period of time to wait for message sends before abandoning the request and retrying.").
				Advanced().Default("5s"),
			service.NewBoolField(oskFieldRetryAsBatch).
				Description("When enabled forces an entire batch of messages to be retried if any individual message fails on a send, otherwise only the individual messages that failed are retried. Disabling this helps to reduce message duplicates during intermittent errors, but also makes it impossible to guarantee strict ordering of messages.").
				Advanced().Default(false),
			service.NewBatchPolicyField(oskFieldBatching),
			service.NewIntField(oskFieldMaxRetries).
				Description("The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.").
				Advanced().Default(0),
			service.NewBackOffField(oskFieldBackoff, true, &backoff.ExponentialBackOff{
				InitialInterval: time.Second * 3,
				MaxInterval:     time.Second * 10,
				MaxElapsedTime:  time.Second * 30,
			}).Description("Control time intervals between retry attempts.").Advanced(),
		)
}

func init() {
	err := service.RegisterBatchOutput("kafka", OSKConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (o service.BatchOutput, batchPol service.BatchPolicy, mIF int, err error) {
		if o, err = NewKafkaWriterFromParsed(conf, mgr); err != nil {
			return
		}

		if batchPol, err = conf.FieldBatchPolicy(oskFieldBatching); err != nil {
			return
		}

		if mIF, err = conf.FieldMaxInFlight(); err != nil {
			return
		}

		o, err = span.NewBatchOutput("kafka", conf, o, mgr)
		return
	})
	if err != nil {
		panic(err)
	}
}

type kafkaWriter struct {
	saramConf *sarama.Config

	addresses     []string
	key           *service.InterpolatedString
	topic         *service.InterpolatedString
	partition     *service.InterpolatedString
	staticHeaders map[string]string
	metaFilter    *service.MetadataExcludeFilter
	retryAsBatch  bool

	customTopicCreation bool
	customTopicParts    int
	customTopicRepls    int

	mgr         *service.Resources
	backoffCtor func() backoff.BackOff

	admin    sarama.ClusterAdmin
	producer sarama.SyncProducer

	connMut    sync.RWMutex
	topicCache syncmap.Map
}

// NewKafkaWriterFromParsed returns a kafka output from a parsed config.
func NewKafkaWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, error) {
	k := kafkaWriter{
		mgr: mgr,
	}

	cAddresses, err := conf.FieldStringList(oskFieldAddresses)
	if err != nil {
		return nil, err
	}
	for _, addr := range cAddresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); trimmed != "" {
				k.addresses = append(k.addresses, trimmed)
			}
		}
	}

	if conf.Contains(oskFieldStaticHeaders) {
		if k.staticHeaders, err = conf.FieldStringMap(oskFieldStaticHeaders); err != nil {
			return nil, err
		}
	} else {
		k.staticHeaders = map[string]string{}
	}

	if k.metaFilter, err = conf.FieldMetadataExcludeFilter(oskFieldMetadata); err != nil {
		return nil, err
	}

	if k.key, err = conf.FieldInterpolatedString(oskFieldKey); err != nil {
		return nil, err
	}
	if k.topic, err = conf.FieldInterpolatedString(oskFieldTopic); err != nil {
		return nil, err
	}
	if partStr, _ := conf.FieldString(oskFieldPartition); partStr != "" {
		if k.partition, err = conf.FieldInterpolatedString(oskFieldPartition); err != nil {
			return nil, err
		}
	}

	var expBackoff *backoff.ExponentialBackOff
	if expBackoff, err = conf.FieldBackOff(oskFieldBackoff); err != nil {
		return nil, err
	}
	var maxRetries int
	if maxRetries, err = conf.FieldInt(oskFieldMaxRetries); err != nil {
		return nil, err
	}

	k.backoffCtor = func() backoff.BackOff {
		boff := *expBackoff
		if maxRetries <= 0 {
			return &boff
		}
		return backoff.WithMaxRetries(&boff, uint64(maxRetries))
	}

	if k.retryAsBatch, err = conf.FieldBool(oskFieldRetryAsBatch); err != nil {
		return nil, err
	}

	if conf.Contains(oskFieldCustomTopic) {
		cConf := conf.Namespace(oskFieldCustomTopic)
		if k.customTopicCreation, err = cConf.FieldBool(oskFieldCustomTopicEnabled); err != nil {
			return nil, err
		}
		if k.customTopicParts, err = cConf.FieldInt(oskFieldCustomTopicPartitions); err != nil {
			return nil, err
		}
		if k.customTopicRepls, err = cConf.FieldInt(oskFieldCustomTopicReplicationFactor); err != nil {
			return nil, err
		}
	}

	if k.customTopicCreation {
		if k.customTopicParts != -1 && k.customTopicParts < 2 {
			return nil, fmt.Errorf("topic_partitions must be greater than one, got %v", k.customTopicParts)
		}
		if k.customTopicRepls != -1 && k.customTopicRepls%2 == 0 {
			return nil, fmt.Errorf("topic_replication_factor must be an odd number, got %v", k.customTopicRepls)
		}
	}

	if k.saramConf, err = k.saramaConfigFromParsed(conf); err != nil {
		return nil, err
	}

	if k.admin, err = sarama.NewClusterAdmin(k.addresses, k.saramConf); err != nil {
		return nil, err
	}

	return &k, nil
}

//------------------------------------------------------------------------------

func strToCompressionCodec(str string) (sarama.CompressionCodec, error) {
	switch str {
	case "none":
		return sarama.CompressionNone, nil
	case "snappy":
		return sarama.CompressionSnappy, nil
	case "lz4":
		return sarama.CompressionLZ4, nil
	case "gzip":
		return sarama.CompressionGZIP, nil
	case "zstd":
		return sarama.CompressionZSTD, nil
	}
	return sarama.CompressionNone, fmt.Errorf("compression codec not recognised: %v", str)
}

//------------------------------------------------------------------------------

func strToPartitioner(str string) (sarama.PartitionerConstructor, error) {
	switch str {
	case "fnv1a_hash":
		return sarama.NewHashPartitioner, nil
	case "murmur2_hash":
		return sarama.NewCustomPartitioner(
			sarama.WithAbsFirst(),
			sarama.WithCustomHashFunction(newMurmur2Hash32),
		), nil
	case "random":
		return sarama.NewRandomPartitioner, nil
	case "round_robin":
		return sarama.NewRoundRobinPartitioner, nil
	case "manual":
		return sarama.NewManualPartitioner, nil
	default:
	}
	return nil, fmt.Errorf("partitioner not recognised: %v", str)
}

//------------------------------------------------------------------------------

func (k *kafkaWriter) buildSystemHeaders(part *service.Message) []sarama.RecordHeader {
	if k.saramConf.Version.IsAtLeast(sarama.V0_11_0_0) {
		out := []sarama.RecordHeader{}
		_ = k.metaFilter.Walk(part, func(k, v string) error {
			out = append(out, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(value.IToString(v)),
			})
			return nil
		})
		return out
	}

	// no headers before version 0.11
	return nil
}

//------------------------------------------------------------------------------

func (k *kafkaWriter) buildUserDefinedHeaders(staticHeaders map[string]string) []sarama.RecordHeader {
	if k.saramConf.Version.IsAtLeast(sarama.V0_11_0_0) {
		out := make([]sarama.RecordHeader, 0, len(staticHeaders))

		for name, value := range staticHeaders {
			out = append(out, sarama.RecordHeader{
				Key:   []byte(name),
				Value: []byte(value),
			})
		}

		return out
	}

	// no headers before version 0.11
	return nil
}

//------------------------------------------------------------------------------

func (k *kafkaWriter) saramaConfigFromParsed(conf *service.ParsedConfig) (*sarama.Config, error) {
	config := sarama.NewConfig()

	var err error
	if targetVersionStr, _ := conf.FieldString(oskFieldTargetVersion); targetVersionStr != "" {
		if config.Version, err = sarama.ParseKafkaVersion(targetVersionStr); err != nil {
			return nil, err
		}
	}

	if config.ClientID, err = conf.FieldString(oskFieldClientID); err != nil {
		return nil, err
	}

	if config.RackID, err = conf.FieldString(oskFieldRackID); err != nil {
		return nil, err
	}

	if config.Net.TLS.Config, config.Net.TLS.Enable, err = conf.FieldTLSToggled(oskFieldTLS); err != nil {
		return nil, err
	}

	var compressionStr string
	if compressionStr, err = conf.FieldString(oskFieldCompression); err != nil {
		return nil, err
	}
	if config.Producer.Compression, err = strToCompressionCodec(compressionStr); err != nil {
		return nil, err
	}

	var partitionerStr string
	if partitionerStr, err = conf.FieldString(oskFieldPartitioner); err != nil {
		return nil, err
	}
	if k.partition == nil && partitionerStr == "manual" {
		return nil, errors.New("partition field required for 'manual' partitioner")
	} else if k.partition != nil && partitionerStr != "manual" {
		return nil, errors.New("partition field can only be specified for 'manual' partitioner")
	}
	if config.Producer.Partitioner, err = strToPartitioner(partitionerStr); err != nil {
		return nil, err
	}

	if config.Producer.MaxMessageBytes, err = conf.FieldInt(oskFieldMaxMsgBytes); err != nil {
		return nil, err
	}
	if config.Producer.Timeout, err = conf.FieldDuration(oskFieldTimeout); err != nil {
		return nil, err
	}

	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	var ackReplicas bool
	if ackReplicas, err = conf.FieldBool(oskFieldAckReplicas); err != nil {
		return nil, err
	}

	if config.Producer.Idempotent, err = conf.FieldBool(oskFieldIdempotentWrite); err != nil {
		return nil, err
	}

	if ackReplicas {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	if err := ApplySaramaSASLFromParsed(conf, k.mgr, config); err != nil {
		return nil, err
	}
	return config, nil
}

// Connect attempts to establish a connection to a Kafka broker.
func (k *kafkaWriter) Connect(ctx context.Context) error {
	k.connMut.Lock()
	defer k.connMut.Unlock()

	if k.producer != nil {
		return nil
	}

	var err error
	k.producer, err = sarama.NewSyncProducer(k.addresses, k.saramConf)
	return err
}

// WriteBatch will attempt to write a message to Kafka, wait for
// acknowledgement, and returns an error if applicable.
func (k *kafkaWriter) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	k.connMut.RLock()
	producer := k.producer
	k.connMut.RUnlock()

	if producer == nil {
		return service.ErrNotConnected
	}

	boff := k.backoffCtor()

	userDefinedHeaders := k.buildUserDefinedHeaders(k.staticHeaders)
	msgs := []*sarama.ProducerMessage{}

	for i := 0; i < len(msg); i++ {
		key, err := msg.TryInterpolatedBytes(i, k.key)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		topic, err := msg.TryInterpolatedString(i, k.topic)
		if err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}
		if k.customTopicCreation {
			if err := k.createTopic(topic); err != nil {
				return fmt.Errorf("failed to create topic '%v': %w", topic, err)
			}
		}

		msgBytes, err := msg[i].AsBytes()
		if err != nil {
			return err
		}
		nextMsg := &sarama.ProducerMessage{
			Topic:    topic,
			Value:    sarama.ByteEncoder(msgBytes),
			Headers:  append(k.buildSystemHeaders(msg[i]), userDefinedHeaders...),
			Metadata: i, // Store the original index for later reference.
		}
		if len(key) > 0 {
			nextMsg.Key = sarama.ByteEncoder(key)
		}

		// Only parse and set the partition if we are configured for manual
		// partitioner.  Although samara will (currently) ignore the partition
		// field when not using a manual partitioner, we should only set it when
		// we explicitly want that.
		if k.partition != nil {
			partitionString, err := msg.TryInterpolatedString(i, k.partition)
			if err != nil {
				return fmt.Errorf("partition interpolation error: %w", err)
			}
			if partitionString == "" {
				return errors.New("partition expression failed to produce a value")
			}

			partitionInt, err := strconv.Atoi(partitionString)
			if err != nil {
				return fmt.Errorf("failed to parse valid integer from partition expression: %w", err)
			}
			if partitionInt < 0 {
				return fmt.Errorf("invalid partition parsed from expression, must be >= 0, got %v", partitionInt)
			}
			// samara requires a 32-bit integer for the partition field
			nextMsg.Partition = int32(partitionInt)
		}
		msgs = append(msgs, nextMsg)
	}

	err := producer.SendMessages(msgs)
	for err != nil {
		if pErrs, ok := err.(sarama.ProducerErrors); !k.retryAsBatch && ok {
			if len(pErrs) == 0 {
				break
			}
			batchErr := service.NewBatchError(msg, pErrs[0].Err)
			msgs = nil
			for _, pErr := range pErrs {
				if mIndex, ok := pErr.Msg.Metadata.(int); ok {
					batchErr.Failed(mIndex, pErr.Err)
				}
				msgs = append(msgs, pErr.Msg)
			}
			if len(pErrs) == batchErr.IndexedErrors() {
				err = batchErr
			} else {
				// If these lengths don't match then somehow we failed to obtain
				// the indexes from metadata, which implies something is wrong
				// with our logic here.
				k.mgr.Logger().Warn("Unable to determine batch index of errors")
			}
			k.mgr.Logger().Errorf("Failed to send '%v' messages: %v\n", len(pErrs), err)
		} else {
			k.mgr.Logger().Errorf("Failed to send messages: %v\n", err)
		}

		tNext := boff.NextBackOff()
		if tNext == backoff.Stop {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(tNext):
		}

		// Recheck connection is alive
		k.connMut.RLock()
		producer = k.producer
		k.connMut.RUnlock()

		if producer == nil {
			return service.ErrNotConnected
		}
		err = producer.SendMessages(msgs)
	}

	return nil
}

// Close shuts down the Kafka writer and stops processing messages.
func (k *kafkaWriter) Close(context.Context) error {
	k.connMut.Lock()
	defer k.connMut.Unlock()

	var err error
	if k.producer != nil {
		err = k.producer.Close()
		k.producer = nil
	}

	return err
}

//------------------------------------------------------------------------------

type murmur2 struct {
	data   []byte
	cached *uint32
}

func newMurmur2Hash32() hash.Hash32 {
	return &murmur2{
		data: make([]byte, 0),
	}
}

// Write a slice of data to the hasher.
func (mur *murmur2) Write(p []byte) (n int, err error) {
	mur.data = append(mur.data, p...)
	mur.cached = nil
	return len(p), nil
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (mur *murmur2) Sum(b []byte) []byte {
	v := mur.Sum32()
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// Reset resets the Hash to its initial state.
func (mur *murmur2) Reset() {
	mur.data = mur.data[0:0]
	mur.cached = nil
}

// Size returns the number of bytes Sum will return.
func (mur *murmur2) Size() int {
	return 4
}

// BlockSize returns the hash's underlying block size.
// The Write method must be able to accept any amount
// of data, but it may operate more efficiently if all writes
// are a multiple of the block size.
func (mur *murmur2) BlockSize() int {
	return 4
}

const (
	seed uint32 = uint32(0x9747b28c)
	m    int32  = int32(0x5bd1e995)
	r    uint32 = uint32(24)
)

func (mur *murmur2) Sum32() uint32 {
	if mur.cached != nil {
		return *mur.cached
	}

	length := int32(len(mur.data))

	h := int32(seed ^ uint32(length))
	length4 := length / 4

	for i := int32(0); i < length4; i++ {
		i4 := i * 4
		k := int32(mur.data[i4+0]&0xff) +
			(int32(mur.data[i4+1]&0xff) << 8) +
			(int32(mur.data[i4+2]&0xff) << 16) +
			(int32(mur.data[i4+3]&0xff) << 24)
		k *= m
		k ^= int32(uint32(k) >> r)
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= int32(mur.data[(length & ^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= int32(mur.data[(length & ^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= int32(mur.data[length & ^3] & 0xff)
		h *= m
	}

	h ^= int32(uint32(h) >> 13)
	h *= m
	h ^= int32(uint32(h) >> 15)

	cached := uint32(h)
	mur.cached = &cached
	return cached
}

//------------------------------------------------------------------------------

// createTopic creates a topic in the Kafka cluster if it does not already
// exist.
//
// If k.conf.PartitionsPerNewTopic is set to a value greater than 0, then the
// topic will be created with that number of partitions.
func (k *kafkaWriter) createTopic(topic string) error {
	if exists, err := k.checkIfTopicExists(topic); err != nil {
		return err
	} else if exists {
		return nil
	}

	k.topicCache.Store(topic, false)
	topicDetail := sarama.TopicDetail{
		NumPartitions:     int32(k.customTopicParts),
		ReplicationFactor: int16(k.customTopicRepls),
	}
	return k.admin.CreateTopic(topic, &topicDetail, false)
}

// checkIfTopicExists checks if a topic exists in the Kafka cluster.
func (k *kafkaWriter) checkIfTopicExists(topic string) (exists bool, err error) {
	initialized, ok := k.topicCache.Load(topic)
	if ok && initialized.(bool) {
		return true, nil
	}

	var topics map[string]sarama.TopicDetail
	if topics, err = k.admin.ListTopics(); err != nil {
		return
	}

	_, exists = topics[topic]
	k.topicCache.Store(topic, exists)
	return
}
