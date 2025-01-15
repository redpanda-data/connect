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
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	rmooFieldOffsetTopic           = "offset_topic"
	rmooFieldOffsetGroup           = "offset_group"
	rmooFieldOffsetPartition       = "offset_partition"
	rmooFieldOffsetCommitTimestamp = "offset_commit_timestamp"
	rmooFieldOffsetMetadata        = "offset_metadata"

	// Deprecated fields
	rmooFieldKafkaKey    = "kafka_key"
	rmooFieldMaxInFlight = "max_in_flight"
)

func redpandaMigratorOffsetsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.37.0").
		Summary("Redpanda Migrator consumer group offsets output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description("This output can be used in combination with the `kafka_franz` input that is configured to read the `__consumer_offsets` topic.").
		Fields(redpandaMigratorOffsetsOutputConfigFields()...)
}

// redpandaMigratorOffsetsOutputConfigFields returns the full suite of config fields for a redpanda_migrator_offsets output using the
// franz-go client library.
func redpandaMigratorOffsetsOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		[]*service.ConfigField{
			service.NewInterpolatedStringField(rmooFieldOffsetTopic).
				Description("Kafka offset topic.").Default("${! @kafka_offset_topic }"),
			service.NewInterpolatedStringField(rmooFieldOffsetGroup).
				Description("Kafka offset group.").Default("${! @kafka_offset_group }"),
			service.NewInterpolatedStringField(rmooFieldOffsetPartition).
				Description("Kafka offset partition.").Default("${! @kafka_offset_partition }"),
			service.NewInterpolatedStringField(rmooFieldOffsetCommitTimestamp).
				Description("Kafka offset commit timestamp.").Default("${! @kafka_offset_commit_timestamp }"),
			service.NewInterpolatedStringField(rmooFieldOffsetMetadata).
				Description("Kafka offset metadata value.").Default(`${! @kafka_offset_metadata }`),

			// Deprecated fields
			service.NewInterpolatedStringField(rmooFieldKafkaKey).
				Description("Kafka key.").Default("${! @kafka_key }").Deprecated(),
			service.NewIntField(rmooFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(1).Deprecated(),
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
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			maxInFlight = 1

			output, err = newRedpandaMigratorOffsetsWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// redpandaMigratorOffsetsWriter implements a Redpanda Migrator offsets writer using the franz-go library.
type redpandaMigratorOffsetsWriter struct {
	clientDetails         *kafka.FranzConnectionDetails
	clientOpts            []kgo.Opt
	offsetTopic           *service.InterpolatedString
	offsetGroup           *service.InterpolatedString
	offsetPartition       *service.InterpolatedString
	offsetCommitTimestamp *service.InterpolatedString
	offsetMetadata        *service.InterpolatedString
	backoffCtor           func() backoff.BackOff

	connMut sync.Mutex
	client  *kadm.Client

	mgr *service.Resources
}

// newRedpandaMigratorOffsetsWriterFromConfig attempts to instantiate a redpandaMigratorOffsetsWriter from a parsed config.
func newRedpandaMigratorOffsetsWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*redpandaMigratorOffsetsWriter, error) {
	w := redpandaMigratorOffsetsWriter{
		mgr: mgr,
	}

	var err error
	if w.clientDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
		return nil, err
	}

	if w.offsetTopic, err = conf.FieldInterpolatedString(rmooFieldOffsetTopic); err != nil {
		return nil, err
	}

	if w.offsetGroup, err = conf.FieldInterpolatedString(rmooFieldOffsetGroup); err != nil {
		return nil, err
	}

	if w.offsetPartition, err = conf.FieldInterpolatedString(rmooFieldOffsetPartition); err != nil {
		return nil, err
	}

	if w.offsetCommitTimestamp, err = conf.FieldInterpolatedString(rmooFieldOffsetCommitTimestamp); err != nil {
		return nil, err
	}

	if w.offsetMetadata, err = conf.FieldInterpolatedString(rmooFieldOffsetMetadata); err != nil {
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
func (w *redpandaMigratorOffsetsWriter) Connect(ctx context.Context) error {
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
func (w *redpandaMigratorOffsetsWriter) Write(ctx context.Context, msg *service.Message) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return service.ErrNotConnected
	}

	var topic string
	var err error
	if topic, err = w.offsetTopic.TryString(msg); err != nil {
		return fmt.Errorf("failed to extract offset topic: %s", err)
	}

	var group string
	if group, err = w.offsetGroup.TryString(msg); err != nil {
		return fmt.Errorf("failed to extract offset group: %s", err)
	}

	var offsetPartition int32
	if p, err := w.offsetPartition.TryString(msg); err != nil {
		return fmt.Errorf("failed to extract offset partition: %s", err)
	} else {
		i, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("failed to parse offset partition: %s", err)
		}
		offsetPartition = int32(i)
	}

	var offsetCommitTimestamp int64
	if t, err := w.offsetCommitTimestamp.TryString(msg); err != nil {
		return fmt.Errorf("failed to extract offset commit timestamp: %s", err)
	} else {
		offsetCommitTimestamp, err = strconv.ParseInt(t, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse offset partition: %s", err)
		}
	}

	var offsetMetadata string
	if w.offsetMetadata != nil {
		if offsetMetadata, err = w.offsetMetadata.TryString(msg); err != nil {
			return fmt.Errorf("failed to extract offset metadata: %w", err)
		}
	}

	updateConsumerOffsets := func() error {
		listedOffsets, err := w.client.ListOffsetsAfterMilli(ctx, offsetCommitTimestamp, topic)
		if err != nil {
			return fmt.Errorf("failed to translate consumer offsets: %s", err)
		}

		if err := listedOffsets.Error(); err != nil {
			return fmt.Errorf("listed offsets error: %s", err)
		}

		offsets := listedOffsets.Offsets()
		// Logic extracted from offsets.KeepFunc() and adjusted to set the metadata.
		for topic, partitionOffsets := range offsets {
			for partition, offset := range partitionOffsets {
				if offset.Partition != offsetPartition {
					delete(partitionOffsets, partition)
				}
				offset.Metadata = offsetMetadata
				partitionOffsets[partition] = offset
			}
			if len(partitionOffsets) == 0 {
				delete(offsets, topic)
			}
		}

		offsetResponses, err := w.client.CommitOffsets(ctx, group, offsets)
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
		// TODO: Use `dispatch.TriggerSignal()` to consume new messages while `updateConsumerOffsets()` is running.
		err := updateConsumerOffsets()
		if err == nil {
			break
		}

		wait := backOff.NextBackOff()
		if wait == backoff.Stop {
			return fmt.Errorf("failed to update consumer offsets for topic %q and partition %d: %s", topic, offsetPartition, err)
		}

		time.Sleep(wait)
	}

	return nil
}

// Close underlying connections.
func (w *redpandaMigratorOffsetsWriter) Close(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return nil
	}

	w.client.Close()
	w.client = nil

	return nil
}
