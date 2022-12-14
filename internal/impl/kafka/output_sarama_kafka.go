package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"

	batchInternal "github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/kafka/sasl"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newKafkaOutput), docs.ComponentSpec{
		Name:    "kafka",
		Summary: `The kafka output type writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.`,
		Description: output.Description(true, true, `
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

Unfortunately this error message will appear for a wide range of connection problems even when the broker endpoint can be reached. Double check your authentication configuration and also ensure that you have [enabled TLS](#tlsenabled) if applicable.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("addresses", "A list of broker addresses to connect to. If an item of the list contains commas it will be expanded into multiple addresses.", []string{"localhost:9092"}, []string{"localhost:9041,localhost:9042"}, []string{"localhost:9041", "localhost:9042"}).Array(),
			btls.FieldSpec(),
			sasl.FieldSpec(),
			docs.FieldString("topic", "The topic to publish messages to.").IsInterpolated(),
			docs.FieldString("client_id", "An identifier for the client connection.").Advanced(),
			docs.FieldString("target_version", "The version of the Kafka protocol to use. This limits the capabilities used by the client and should ideally match the version of your brokers."),
			docs.FieldString("rack_id", "A rack identifier for this client.").Advanced(),
			docs.FieldString("key", "The key to publish messages with.").IsInterpolated(),
			docs.FieldString("partitioner", "The partitioning algorithm to use.").HasOptions("fnv1a_hash", "murmur2_hash", "random", "round_robin", "manual"),
			docs.FieldString("partition", "The manually-specified partition to publish messages to, relevant only when the field `partitioner` is set to `manual`. Must be able to parse as a 32-bit integer.").IsInterpolated().Advanced(),
			docs.FieldString("compression", "The compression algorithm to use.").HasOptions("none", "snappy", "lz4", "gzip", "zstd"),
			docs.FieldString("static_headers", "An optional map of static headers that should be added to messages in addition to metadata.", map[string]string{"first-static-header": "value-1", "second-static-header": "value-2"}).Map(),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are sent with messages as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			output.InjectTracingSpanMappingDocs,
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldBool("ack_replicas", "Ensure that messages have been copied across all replicas before acknowledging receipt.").Advanced(),
			docs.FieldInt("max_msg_bytes", "The maximum size in bytes of messages sent to the target topic.").Advanced(),
			docs.FieldString("timeout", "The maximum period of time to wait for message sends before abandoning the request and retrying.").Advanced(),
			docs.FieldBool("retry_as_batch", "When enabled forces an entire batch of messages to be retried if any individual message fails on a send, otherwise only the individual messages that failed are retried. Disabling this helps to reduce message duplicates during intermittent errors, but also makes it impossible to guarantee strict ordering of messages.").Advanced(),
			policy.FieldSpec(),
		).WithChildren(retries.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewKafkaConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newKafkaOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	k, err := NewKafkaWriter(conf.Kafka, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("kafka", conf.Kafka.MaxInFlight, k, mgr)
	if err != nil {
		return nil, err
	}

	if conf.Kafka.InjectTracingMap != "" {
		aw, ok := w.(*output.AsyncWriter)
		if !ok {
			return nil, fmt.Errorf("unable to set an inject_tracing_map due to wrong type: %T", w)
		}

		injectTracingMap, err := mgr.BloblEnvironment().NewMapping(conf.Kafka.InjectTracingMap)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize inject tracing map: %v", err)
		}
		aw.SetInjectTracingMap(injectTracingMap)
	}

	return batcher.NewFromConfig(conf.Kafka.Batching, w, mgr)
}

type kafkaWriter struct {
	log log.Modular
	mgr bundle.NewManagement

	backoffCtor func() backoff.BackOff

	tlsConf *tls.Config
	timeout time.Duration

	addresses []string
	version   sarama.KafkaVersion
	conf      output.KafkaConfig

	key       *field.Expression
	topic     *field.Expression
	partition *field.Expression

	producer    sarama.SyncProducer
	compression sarama.CompressionCodec
	partitioner sarama.PartitionerConstructor

	staticHeaders map[string]string
	metaFilter    *metadata.ExcludeFilter

	connMut sync.RWMutex
}

// NewKafkaWriter returns a kafka writer.
func NewKafkaWriter(conf output.KafkaConfig, mgr bundle.NewManagement) (output.AsyncSink, error) {
	compression, err := strToCompressionCodec(conf.Compression)
	if err != nil {
		return nil, err
	}

	if conf.Partition == "" && conf.Partitioner == "manual" {
		return nil, fmt.Errorf("partition field required for 'manual' partitioner")
	} else if len(conf.Partition) > 0 && conf.Partitioner != "manual" {
		return nil, fmt.Errorf("partition field can only be specified for 'manual' partitioner")
	}

	partitioner, err := strToPartitioner(conf.Partitioner)
	if err != nil {
		return nil, err
	}

	k := kafkaWriter{
		log: mgr.Logger(),
		mgr: mgr,

		conf:          conf,
		compression:   compression,
		partitioner:   partitioner,
		staticHeaders: conf.StaticHeaders,
	}

	if k.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	if k.key, err = mgr.BloblEnvironment().NewField(conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	if k.topic, err = mgr.BloblEnvironment().NewField(conf.Topic); err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}
	if k.partition, err = mgr.BloblEnvironment().NewField(conf.Partition); err != nil {
		return nil, fmt.Errorf("failed to parse parition expression: %v", err)
	}
	if k.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if k.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if conf.TLS.Enabled {
		var err error
		if k.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
			return nil, err
		}
	}

	if k.version, err = sarama.ParseKafkaVersion(conf.TargetVersion); err != nil {
		return nil, err
	}

	for _, addr := range conf.Addresses {
		for _, splitAddr := range strings.Split(addr, ",") {
			if trimmed := strings.TrimSpace(splitAddr); len(trimmed) > 0 {
				k.addresses = append(k.addresses, trimmed)
			}
		}
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

func (k *kafkaWriter) buildSystemHeaders(part *message.Part) []sarama.RecordHeader {
	if k.version.IsAtLeast(sarama.V0_11_0_0) {
		out := []sarama.RecordHeader{}
		_ = k.metaFilter.Iter(part, func(k string, v any) error {
			out = append(out, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(query.IToString(v)),
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
	if k.version.IsAtLeast(sarama.V0_11_0_0) {
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

// Connect attempts to establish a connection to a Kafka broker.
func (k *kafkaWriter) Connect(ctx context.Context) error {
	k.connMut.Lock()
	defer k.connMut.Unlock()

	if k.producer != nil {
		return nil
	}

	config := sarama.NewConfig()
	config.ClientID = k.conf.ClientID
	config.RackID = k.conf.RackID

	config.Version = k.version

	config.Producer.Compression = k.compression
	config.Producer.Partitioner = k.partitioner
	config.Producer.MaxMessageBytes = k.conf.MaxMsgBytes
	config.Producer.Timeout = k.timeout
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Net.TLS.Enable = k.conf.TLS.Enabled
	if k.conf.TLS.Enabled {
		config.Net.TLS.Config = k.tlsConf
	}
	if err := ApplySASLConfig(k.conf.SASL, k.mgr, config); err != nil {
		return err
	}

	if k.conf.AckReplicas {
		config.Producer.RequiredAcks = sarama.WaitForAll
	} else {
		config.Producer.RequiredAcks = sarama.WaitForLocal
	}

	var err error
	k.producer, err = sarama.NewSyncProducer(k.addresses, config)

	if err == nil {
		k.log.Infof("Sending Kafka messages to addresses: %s\n", k.addresses)
	}
	return err
}

// WriteBatch will attempt to write a message to Kafka, wait for
// acknowledgement, and returns an error if applicable.
func (k *kafkaWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	k.connMut.RLock()
	producer := k.producer
	k.connMut.RUnlock()

	if producer == nil {
		return component.ErrNotConnected
	}

	boff := k.backoffCtor()

	userDefinedHeaders := k.buildUserDefinedHeaders(k.staticHeaders)
	msgs := []*sarama.ProducerMessage{}

	if err := msg.Iter(func(i int, p *message.Part) error {
		key, err := k.key.Bytes(i, msg)
		if err != nil {
			return fmt.Errorf("key interpolation error: %w", err)
		}
		topic, err := k.topic.String(i, msg)
		if err != nil {
			return fmt.Errorf("topic interpolation error: %w", err)
		}
		nextMsg := &sarama.ProducerMessage{
			Topic:    topic,
			Value:    sarama.ByteEncoder(p.AsBytes()),
			Headers:  append(k.buildSystemHeaders(p), userDefinedHeaders...),
			Metadata: i, // Store the original index for later reference.
		}
		if len(key) > 0 {
			nextMsg.Key = sarama.ByteEncoder(key)
		}

		// Only parse and set the partition if we are configured for manual
		// partitioner.  Although samara will (currently) ignore the partition
		// field when not using a manual partitioner, we should only set it when
		// we explicitly want that.
		if k.conf.Partitioner == "manual" {
			partitionString, err := k.partition.String(i, msg)
			if err != nil {
				return fmt.Errorf("partition interpolation error: %w", err)
			}
			if partitionString == "" {
				return fmt.Errorf("partition expression failed to produce a value")
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
		return nil
	}); err != nil {
		return err
	}

	err := producer.SendMessages(msgs)
	for err != nil {
		if pErrs, ok := err.(sarama.ProducerErrors); !k.conf.RetryAsBatch && ok {
			if len(pErrs) == 0 {
				break
			}
			batchErr := batchInternal.NewError(msg, pErrs[0].Err)
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
				k.log.Warnln("Unable to determine batch index of errors")
			}
			k.log.Errorf("Failed to send '%v' messages: %v\n", len(pErrs), err)
		} else {
			k.log.Errorf("Failed to send messages: %v\n", err)
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
			return component.ErrNotConnected
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
