package writer

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	batchInternal "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/hash/murmur2"
	"github.com/Jeffail/benthos/v3/lib/util/kafka/sasl"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

// KafkaConfig contains configuration fields for the Kafka output type.
type KafkaConfig struct {
	Addresses        []string    `json:"addresses" yaml:"addresses"`
	ClientID         string      `json:"client_id" yaml:"client_id"`
	RackID           string      `json:"rack_id" yaml:"rack_id"`
	Key              string      `json:"key" yaml:"key"`
	Partitioner      string      `json:"partitioner" yaml:"partitioner"`
	Partition        string      `json:"partition" yaml:"partition"`
	Topic            string      `json:"topic" yaml:"topic"`
	Compression      string      `json:"compression" yaml:"compression"`
	MaxMsgBytes      int         `json:"max_msg_bytes" yaml:"max_msg_bytes"`
	Timeout          string      `json:"timeout" yaml:"timeout"`
	AckReplicas      bool        `json:"ack_replicas" yaml:"ack_replicas"`
	TargetVersion    string      `json:"target_version" yaml:"target_version"`
	TLS              btls.Config `json:"tls" yaml:"tls"`
	SASL             sasl.Config `json:"sasl" yaml:"sasl"`
	MaxInFlight      int         `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config   `json:",inline" yaml:",inline"`
	RetryAsBatch     bool                         `json:"retry_as_batch" yaml:"retry_as_batch"`
	Batching         batch.PolicyConfig           `json:"batching" yaml:"batching"`
	StaticHeaders    map[string]string            `json:"static_headers" yaml:"static_headers"`
	Metadata         metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	InjectTracingMap string                       `json:"inject_tracing_map" yaml:"inject_tracing_map"`
}

// NewKafkaConfig creates a new KafkaConfig with default values.
func NewKafkaConfig() KafkaConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "3s"
	rConf.Backoff.MaxInterval = "10s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return KafkaConfig{
		Addresses:     []string{"localhost:9092"},
		ClientID:      "benthos_kafka_output",
		RackID:        "",
		Key:           "",
		Partitioner:   "fnv1a_hash",
		Partition:     "",
		Topic:         "benthos_stream",
		Compression:   "none",
		MaxMsgBytes:   1000000,
		Timeout:       "5s",
		AckReplicas:   false,
		TargetVersion: sarama.V1_0_0_0.String(),
		StaticHeaders: map[string]string{},
		Metadata:      metadata.NewExcludeFilterConfig(),
		TLS:           btls.NewConfig(),
		SASL:          sasl.NewConfig(),
		MaxInFlight:   1,
		Config:        rConf,
		RetryAsBatch:  false,
		Batching:      batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// Kafka is a writer type that writes messages into kafka.
type Kafka struct {
	log   log.Modular
	mgr   types.Manager
	stats metrics.Type

	backoffCtor func() backoff.BackOff

	tlsConf *tls.Config
	timeout time.Duration

	addresses []string
	version   sarama.KafkaVersion
	conf      KafkaConfig

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

// NewKafka creates a new Kafka writer type.
func NewKafka(conf KafkaConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*Kafka, error) {
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

	k := Kafka{
		log:   log,
		mgr:   mgr,
		stats: stats,

		conf:          conf,
		compression:   compression,
		partitioner:   partitioner,
		staticHeaders: conf.StaticHeaders,
	}

	if k.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	if k.key, err = interop.NewBloblangField(mgr, conf.Key); err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}
	if k.topic, err = interop.NewBloblangField(mgr, conf.Topic); err != nil {
		return nil, fmt.Errorf("failed to parse topic expression: %v", err)
	}
	if k.partition, err = interop.NewBloblangField(mgr, conf.Partition); err != nil {
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
		if k.tlsConf, err = conf.TLS.Get(); err != nil {
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
			sarama.WithCustomHashFunction(murmur2.New32),
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

func (k *Kafka) buildSystemHeaders(part types.Part) []sarama.RecordHeader {
	if k.version.IsAtLeast(sarama.V0_11_0_0) {
		out := []sarama.RecordHeader{}
		k.metaFilter.Iter(part.Metadata(), func(k, v string) error {
			out = append(out, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
			return nil
		})
		return out
	}

	// no headers before version 0.11
	return nil
}

//------------------------------------------------------------------------------

func (k *Kafka) buildUserDefinedHeaders(staticHeaders map[string]string) []sarama.RecordHeader {
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

// ConnectWithContext attempts to establish a connection to a Kafka broker.
func (k *Kafka) ConnectWithContext(ctx context.Context) error {
	return k.Connect()
}

// Connect attempts to establish a connection to a Kafka broker.
func (k *Kafka) Connect() error {
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
	if err := k.conf.SASL.Apply(k.mgr, config); err != nil {
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

// Write will attempt to write a message to Kafka, wait for acknowledgement, and
// returns an error if applicable.
func (k *Kafka) Write(msg types.Message) error {
	return k.WriteWithContext(context.Background(), msg)
}

// WriteWithContext will attempt to write a message to Kafka, wait for
// acknowledgement, and returns an error if applicable.
func (k *Kafka) WriteWithContext(ctx context.Context, msg types.Message) error {
	k.connMut.RLock()
	producer := k.producer
	k.connMut.RUnlock()

	if producer == nil {
		return types.ErrNotConnected
	}

	boff := k.backoffCtor()

	userDefinedHeaders := k.buildUserDefinedHeaders(k.staticHeaders)
	msgs := []*sarama.ProducerMessage{}

	err := msg.Iter(func(i int, p types.Part) error {
		key := k.key.Bytes(i, msg)
		nextMsg := &sarama.ProducerMessage{
			Topic:    k.topic.String(i, msg),
			Value:    sarama.ByteEncoder(p.Get()),
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
			partitionString := k.partition.String(i, msg)
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
	})

	if err != nil {
		return err
	}

	err = producer.SendMessages(msgs)
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
			return types.ErrNotConnected
		}
		err = producer.SendMessages(msgs)
	}

	return nil
}

// CloseAsync shuts down the Kafka writer and stops processing messages.
func (k *Kafka) CloseAsync() {
	go func() {
		k.connMut.Lock()
		if k.producer != nil {
			k.producer.Close()
			k.producer = nil
		}
		k.connMut.Unlock()
	}()
}

// WaitForClose blocks until the Kafka writer has closed down.
func (k *Kafka) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
