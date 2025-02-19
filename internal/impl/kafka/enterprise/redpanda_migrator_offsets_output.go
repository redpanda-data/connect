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

	var partition int32
	if p, err := w.offsetPartition.TryString(msg); err != nil {
		return fmt.Errorf("failed to extract offset partition: %s", err)
	} else {
		i, err := strconv.Atoi(p)
		if err != nil {
			return fmt.Errorf("failed to parse offset partition: %s", err)
		}
		partition = int32(i)
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
		// ListOffsetsAfterMilli returns the topic's end offset if the supplied timestamp is greater than the timestamps
		// of all the records in the topic. It also sets the timestamp of the returned offset to -1 in this case.
		listedOffsets, err := w.client.ListOffsetsAfterMilli(ctx, offsetCommitTimestamp, topic)
		if err != nil {
			return fmt.Errorf("failed to translate consumer offsets: %s", err)
		}

		if err := listedOffsets.Error(); err != nil {
			return fmt.Errorf("listed offsets error: %s", err)
		}

		offset, ok := listedOffsets.Lookup(topic, partition)
		if !ok {
			// This should never happen, but we check just in case.
			return fmt.Errorf("no offsets returned for topic %q", topic)
		}

		if !isEndOffset && offset.Timestamp == -1 {
			// This can happen if we received an offset update, but the record which was read from the source cluster to
			// trigger it has not been replicated to the destination cluster yet. In this case, we raise an error so the
			// operation is retried.
			return fmt.Errorf("no offsets returned for topic %q", topic)
		}

		// This is an optimisation to try and avoid unnecessary duplicates in the common case when the
		// received offset update points to the end offset of the source topic. In
		// this special case, we check if the matching offset in the destination topic (returned by
		// `ListOffsetsAfterMilli`) also points to the end offset. If it does, then we can fetch the current end
		// offset of the destination topic and set the destination consumer offset to that value.
		// Note: Even for compacted topics, the last record of the topic cannot be compacted, so it's safe to assume its
		// offset will be one less than the end offset.
		if isEndOffset && offset.Timestamp != -1 {
			endOffsets, err := w.client.ListEndOffsets(ctx, topic)
			if err != nil {
				return fmt.Errorf("failed to read the end offset for topic %q and partition %q: %s", topic, partition, err)
			}

			endOffset, ok := endOffsets.Lookup(topic, partition)
			if !ok {
				return fmt.Errorf("failed to find the end offset for topic %q and partition %q: %s", topic, partition, err)
			}
			if endOffset.Offset == offset.Offset+1 {
				offset.Offset = endOffset.Offset
			}
		}

		var offsets kadm.Offsets
		offsets.Add(kadm.Offset{
			Topic:       offset.Topic,
			Partition:   offset.Partition,
			At:          offset.Offset,
			LeaderEpoch: offset.LeaderEpoch,
			Metadata:    offsetMetadata,
		})

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

		w.mgr.Logger().Debug(err.Error())

		wait := backOff.NextBackOff()
		if wait == backoff.Stop {
			return fmt.Errorf("failed to update consumer offsets for topic %q and partition %d: %s", topic, partition, err)
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
