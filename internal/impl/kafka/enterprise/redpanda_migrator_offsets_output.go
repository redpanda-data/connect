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
	rmooFieldIsEndOffset           = "is_end_offset"

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
			service.NewInterpolatedStringField(rmooFieldIsEndOffset).
				Description("Indicates if the update represents the end offset of the Kafka topic partition.").Default(`${! @kafka_is_end_offset }`),

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
	clientOpts            []kgo.Opt
	offsetTopic           *service.InterpolatedString
	offsetGroup           *service.InterpolatedString
	offsetPartition       *service.InterpolatedString
	offsetCommitTimestamp *service.InterpolatedString
	offsetMetadata        *service.InterpolatedString
	isEndOffset           *service.InterpolatedString
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

	clientDetails, err := kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger())
	if err != nil {
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

	if w.isEndOffset, err = conf.FieldInterpolatedString(rmooFieldIsEndOffset); err != nil {
		return nil, err
	}

	var clientOpts []kgo.Opt
	if clientOpts, err = kafka.FranzProducerLimitsOptsFromConfig(conf); err != nil {
		return nil, err
	}

	w.clientOpts = slices.Concat(
		clientOpts,
		[]kgo.Opt{
			kgo.SeedBrokers(clientDetails.SeedBrokers...),
			kgo.SASL(clientDetails.SASL...),
			kgo.ClientID(clientDetails.ClientID),
			kgo.WithLogger(&kafka.KGoLogger{L: w.mgr.Logger()}),
		})
	if clientDetails.TLSConf != nil {
		w.clientOpts = append(w.clientOpts, kgo.DialTLSConfig(clientDetails.TLSConf))
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

	var err error
	var client *kgo.Client
	if client, err = kgo.NewClient(w.clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	// The default kadm client timeout is 15s. Do we need to make this configurable?
	w.client = kadm.NewClient(client)

	return nil
}

// getLastRecordOffset returns the last record offset for the given topic and partition.
func (w *redpandaMigratorOffsetsWriter) getLastRecordOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	client, err := kgo.NewClient(w.clientOpts...)
	if err != nil {
		return 0, fmt.Errorf("failed to create Kafka client: %s", err)
	}
	defer client.Close()

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {
			partition: kgo.NewOffset().Relative(-1),
		},
	})

	fetches := client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return 0, fmt.Errorf("failed to read last record for topic %q partition %q: client closed", topic, partition)
	}

	if err := fetches.Err(); err != nil {
		return 0, fmt.Errorf("failed to read last record for topic %q partition %q: %s", topic, partition, err)
	}

	it := fetches.RecordIter()
	if it.Done() {
		return 0, fmt.Errorf("couldn't find the last record for topic %q partition %q: %s", topic, partition, err)
	}

	return it.Next().Offset, nil
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

	isEndOffset := false
	if w.isEndOffset != nil {
		data, err := w.isEndOffset.TryString(msg)
		if err != nil {
			return fmt.Errorf("failed to extract is_end_offset: %w", err)
		}
		isEndOffset, err = strconv.ParseBool(data)
		if err != nil {
			return fmt.Errorf("failed to parse is_end_offset: %w", err)
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

				// As an optimisation to try and avoid unnecessary duplicates in the common case, we check if the
				// received offset update was triggered when a consumer read the last record of the source topic. In
				// this special case, we check if the matching offset in the destination topic (returned by
				// `ListOffsetsAfterMilli`) also points to the end record. If it does, then we can fetch the current end
				// offset of the destination topic and set the destination consumer offset to that value.
				// Note: We have to be conservative here and assume there might be duplicates in the destination topic,
				// so we can't just update the offset to the end offset without first checking if
				// `ListOffsetsAfterMilli` actually returned the offset of the end record.
				if isEndOffset {
					lastOffset, err := w.getLastRecordOffset(ctx, topic, partition)
					if err != nil {
						return err
					}

					if offset.At == lastOffset {
						offsets, err := w.client.ListEndOffsets(ctx, topic)
						if err != nil {
							return fmt.Errorf("failed to read the end offset for topic %q and partition %q: %s", topic, partition, err)
						}

						endOffset, ok := offsets.Lookup(topic, partition)
						if !ok {
							return fmt.Errorf("failed to find the end offset for topic %q and partition %q: %s", topic, partition, err)
						}

						offset.At = endOffset.Offset
					}
				}

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
		// TODO: Maybe use `dispatch.TriggerSignal()` to consume new messages while `updateConsumerOffsets()` is running
		// if this proves to be too slow.
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
