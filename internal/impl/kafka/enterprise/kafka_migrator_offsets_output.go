// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

func kafkaMigratorOffsetsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.33.1").
		Summary("Kafka Migrator consumer group offsets output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		// TODO
		Description("This output can be used in combination with the `kafka_franz` input that is configured to read the `__consumer_offsets` topic.").
		Fields(KafkaMigratorOffsetsOutputConfigFields()...)
}

// KafkaMigratorOffsetsOutputConfigFields returns the full suite of config fields for a kafka_migrator_offsets output using the
// franz-go client library.
func KafkaMigratorOffsetsOutputConfigFields() []*service.ConfigField {
	return append(
		[]*service.ConfigField{
			service.NewStringListField("seed_brokers").
				Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
				Example([]string{"localhost:9092"}).
				Example([]string{"foo:9092", "bar:9092"}).
				Example([]string{"foo:9092,bar:9092"}),
			service.NewInterpolatedStringField("kafka_key").
				Description("Kafka key.").Default("${! @kafka_key }"),
			service.NewStringField("client_id").
				Description("An identifier for the client connection.").
				Default("benthos").
				Advanced(),
			service.NewIntField("max_in_flight").
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(1),
			service.NewDurationField("timeout").
				Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
				Default("10s").
				Advanced(),
			service.NewStringField("max_message_bytes").
				Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
				Advanced().
				Default("1MB").
				Example("100MB").
				Example("50mib"),
			service.NewStringField("broker_write_max_bytes").
				Description("The upper bound for the number of bytes written to a broker connection in a single write. This field corresponds to Kafka's `socket.request.max.bytes`.").
				Advanced().
				Default("100MB").
				Example("128MB").
				Example("50mib"),
			service.NewTLSToggledField("tls"),
			kafka.SASLFields(),
		},
		retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...,
	)
}

func init() {
	err := service.RegisterOutput("kafka_migrator_offsets", kafkaMigratorOffsetsOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.Output,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			output, err = NewKafkaMigratorOffsetsWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// KafkaMigratorOffsetsWriter implements a Kafka Migrator offsets writer using the franz-go library.
type KafkaMigratorOffsetsWriter struct {
	SeedBrokers         []string
	kafkaKey            *service.InterpolatedString
	clientID            string
	TLSConf             *tls.Config
	saslConfs           []sasl.Mechanism
	timeout             time.Duration
	produceMaxBytes     int32
	brokerWriteMaxBytes int32
	backoffCtor         func() backoff.BackOff

	connMut sync.Mutex
	client  *kadm.Client

	mgr *service.Resources
}

// NewKafkaMigratorOffsetsWriterFromConfig attempts to instantiate a KafkaMigratorOffsetsWriter from a parsed config.
func NewKafkaMigratorOffsetsWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*KafkaMigratorOffsetsWriter, error) {
	w := KafkaMigratorOffsetsWriter{
		mgr: mgr,
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		w.SeedBrokers = append(w.SeedBrokers, strings.Split(b, ",")...)
	}

	if w.kafkaKey, err = conf.FieldInterpolatedString("kafka_key"); err != nil {
		return nil, err
	}

	if w.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	maxMessageBytesStr, err := conf.FieldString("max_message_bytes")
	if err != nil {
		return nil, err
	}
	maxMessageBytes, err := humanize.ParseBytes(maxMessageBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_message_bytes: %w", err)
	}
	if maxMessageBytes > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("invalid max_message_bytes, must not exceed %v", math.MaxInt32)
	}
	w.produceMaxBytes = int32(maxMessageBytes)
	brokerWriteMaxBytesStr, err := conf.FieldString("broker_write_max_bytes")
	if err != nil {
		return nil, err
	}
	brokerWriteMaxBytes, err := humanize.ParseBytes(brokerWriteMaxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse broker_write_max_bytes: %w", err)
	}
	if brokerWriteMaxBytes > 1<<30 {
		return nil, fmt.Errorf("invalid broker_write_max_bytes, must not exceed %v", 1<<30)
	}
	w.brokerWriteMaxBytes = int32(brokerWriteMaxBytes)

	if w.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		w.TLSConf = tlsConf
	}
	if w.saslConfs, err = kafka.SASLMechanismsFromConfig(conf); err != nil {
		return nil, err
	}
	if w.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(conf); err != nil {
		return nil, err
	}

	return &w, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (w *KafkaMigratorOffsetsWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(w.SeedBrokers...),
		kgo.SASL(w.saslConfs...),
		kgo.ProducerBatchMaxBytes(w.produceMaxBytes),
		kgo.BrokerMaxWriteBytes(w.brokerWriteMaxBytes),
		kgo.ProduceRequestTimeout(w.timeout),
		kgo.ClientID(w.clientID),
		kgo.WithLogger(&kafka.KGoLogger{L: w.mgr.Logger()}),
	}
	if w.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(w.TLSConf))
	}

	var err error
	var client *kgo.Client
	if client, err = kgo.NewClient(clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	w.client = kadm.NewClient(client)

	return nil
}

// Write attempts to write a message to the output cluster.
func (w *KafkaMigratorOffsetsWriter) Write(ctx context.Context, msg *service.Message) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return service.ErrNotConnected
	}

	var kafkaKey []byte
	var err error
	// TODO: The `kafka_key` metadata field is cast from `[]byte` to string in the `kafka_franz` input, which is wrong.
	if kafkaKey, err = w.kafkaKey.TryBytes(msg); err != nil {
		return fmt.Errorf("failed to extract kafka key: %w", err)
	}

	key := kmsg.NewOffsetCommitKey()
	// Check the version to ensure that we process only offset commit keys
	if err := key.ReadFrom(kafkaKey); err != nil || (key.Version != 0 && key.Version != 1) {
		return nil
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("failed to get message bytes: %s", err)
	}

	val := kmsg.NewOffsetCommitValue()
	if err := val.ReadFrom(msgBytes); err != nil {
		return fmt.Errorf("failed to decode offset commit value: %s", err)
	}

	updateConsumerOffsets := func() error {
		listedOffsets, err := w.client.ListOffsetsAfterMilli(ctx, val.CommitTimestamp, key.Topic)
		if err != nil {
			return fmt.Errorf("failed to translate consumer offsets: %s", err)
		}

		if err := listedOffsets.Error(); err != nil {
			return fmt.Errorf("listed offsets returned and error: %s", err)
		}

		offsets := listedOffsets.Offsets()
		offsets.KeepFunc(func(o kadm.Offset) bool {
			return o.Partition == key.Partition
		})

		offsetResponses, err := w.client.CommitOffsets(ctx, key.Group, offsets)
		if err != nil {
			return fmt.Errorf("failed to commit consumer offsets: %s", err)
		}

		if err := offsetResponses.Error(); err != nil {
			return fmt.Errorf("committed consumer offsets returned an error: %s", err)
		}

		return nil
	}

	backOff := w.backoffCtor()
	for {
		err := updateConsumerOffsets()
		if err == nil {
			break
		}

		wait := backOff.NextBackOff()
		if wait == backoff.Stop {
			return fmt.Errorf("failed to update consumer offsets for topic %q and partition %d: %s", key.Topic, key.Partition, err)
		}

		time.Sleep(wait)
	}

	return nil
}

// Close underlying connections.
func (w *KafkaMigratorOffsetsWriter) Close(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return nil
	}

	w.client.Close()
	w.client = nil

	return nil
}
