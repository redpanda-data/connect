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
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	rmooFieldMaxInFlight = "max_in_flight"
	rmooFieldKafkaKey    = "kafka_key"
)

func redpandaMigratorOffsetsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.37.0").
		Summary("Redpanda Migrator consumer group offsets output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description("This output can be used in combination with the `kafka_franz` input that is configured to read the `__consumer_offsets` topic.").
		Fields(RedpandaMigratorOffsetsOutputConfigFields()...)
}

// RedpandaMigratorOffsetsOutputConfigFields returns the full suite of config fields for a redpanda_migrator_offsets output using the
// franz-go client library.
func RedpandaMigratorOffsetsOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		[]*service.ConfigField{
			service.NewInterpolatedStringField(rmooFieldKafkaKey).
				Description("Kafka key.").Default("${! @kafka_key }"),
			service.NewIntField(rmooFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(1),
		},
		kafka.FranzProducerLimitsFields(),
		retries.CommonRetryBackOffFields(0, "1s", "5s", "30s"),
	)
}

func init() {
	err := service.RegisterOutput("redpanda_migrator_offsets", redpandaMigratorOffsetsOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.Output,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt(rmooFieldMaxInFlight); err != nil {
				return
			}
			output, err = NewRedpandaMigratorOffsetsWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// RedpandaMigratorOffsetsWriter implements a Redpanda Migrator offsets writer using the franz-go library.
type RedpandaMigratorOffsetsWriter struct {
	clientDetails *kafka.FranzConnectionDetails
	clientOpts    []kgo.Opt
	kafkaKey      *service.InterpolatedString
	backoffCtor   func() backoff.BackOff

	connMut sync.Mutex
	client  *kadm.Client

	mgr *service.Resources
}

// NewRedpandaMigratorOffsetsWriterFromConfig attempts to instantiate a RedpandaMigratorOffsetsWriter from a parsed config.
func NewRedpandaMigratorOffsetsWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*RedpandaMigratorOffsetsWriter, error) {
	w := RedpandaMigratorOffsetsWriter{
		mgr: mgr,
	}

	var err error
	if w.clientDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
		return nil, err
	}

	if w.kafkaKey, err = conf.FieldInterpolatedString(rmooFieldKafkaKey); err != nil {
		return nil, err
	}

	if w.clientOpts, err = kafka.FranzProducerLimitsOptsFromConfig(conf); err != nil {
		return nil, err
	}

	if w.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(conf); err != nil {
		return nil, err
	}

	return &w, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (w *RedpandaMigratorOffsetsWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client != nil {
		return nil
	}

	clientOpts := slices.Concat(
		w.clientOpts,
		[]kgo.Opt{
			kgo.SeedBrokers(w.clientDetails.SeedBrokers...),
			kgo.SASL(w.clientDetails.SASL...),
			kgo.ClientID(w.clientDetails.ClientID),
			kgo.WithLogger(&kafka.KGoLogger{L: w.mgr.Logger()}),
		})
	if w.clientDetails.TLSConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(w.clientDetails.TLSConf))
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
func (w *RedpandaMigratorOffsetsWriter) Write(ctx context.Context, msg *service.Message) error {
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

		// TODO: Add metadata to offsets!
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
func (w *RedpandaMigratorOffsetsWriter) Close(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return nil
	}

	w.client.Close()
	w.client = nil

	return nil
}
