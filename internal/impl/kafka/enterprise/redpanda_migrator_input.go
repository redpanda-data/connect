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
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	// Deprecated fields
	rmiFieldMultiHeader               = "multi_header"
	rmiFieldBatchSize                 = "batch_size"
	rmiFieldOutputResource            = "output_resource"
	rmiFieldReplicationFactorOverride = "replication_factor_override"
	rmiFieldReplicationFactor         = "replication_factor"

	rmiResourceDefaultLabel = "redpanda_migrator_input"
)

func redpandaMigratorInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.37.0").
		Summary(`A Redpanda Migrator input using the https://github.com/twmb/franz-go[Franz Kafka client library^].`).
		Description(`
Reads a batch of messages from a Kafka broker and waits for the output to acknowledge the writes before updating the Kafka consumer group offset.

This input should be used in combination with a ` + "`redpanda_migrator`" + ` output.

When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

It provides the same delivery guarantees and ordering semantics as the ` + "`redpanda`" + ` input.

== Metrics

Emits a ` + "`input_redpanda_migrator_lag`" + ` metric with ` + "`topic`" + ` and ` + "`partition`" + ` labels for each consumed topic.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_ms
- kafka_timestamp_unix
- kafka_tombstone_message
- All record headers
` + "```" + `
`).
		Fields(redpandaMigratorInputConfigFields()...).
		LintRule(`
let has_topic_partitions = this.topics.any(t -> t.contains(":"))
root = if $has_topic_partitions {
  if this.consumer_group.or("") != "" {
    "this input does not support both a consumer group and explicit topic partitions"
  } else if this.regexp_topics {
    "this input does not support both regular expression topics and explicit topic partitions"
  }
} else {
  if this.consumer_group.or("") == "" {
    "a consumer group is mandatory when not using explicit topic partitions"
  }
}
`)
}

func redpandaMigratorInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		kafka.FranzConsumerFields(),
		kafka.FranzReaderOrderedConfigFields(),
		[]*service.ConfigField{
			service.NewAutoRetryNacksToggleField(),

			// Deprecated fields
			service.NewStringField(rmiFieldOutputResource).
				Description("The label of the redpanda_migrator output in which the currently selected topics need to be created before attempting to read messages.").
				Default(rmoResourceDefaultLabel).
				Advanced().
				Deprecated(),
			service.NewBoolField(rmiFieldReplicationFactorOverride).
				Description("Use the specified replication factor when creating topics.").
				Default(true).
				Advanced().
				Deprecated(),
			service.NewIntField(rmiFieldReplicationFactor).
				Description("Replication factor for created topics. This is only used when `replication_factor_override` is set to `true`.").
				Default(3).
				Advanced().
				Deprecated(),
			service.NewBoolField(rmiFieldMultiHeader).
				Description("Decode headers into lists to allow handling of multiple values with the same key").
				Default(false).
				Advanced().
				Deprecated(),
			service.NewIntField(rmiFieldBatchSize).
				Description("The maximum number of messages that should be accumulated into each batch.").
				Default(1024).
				Advanced().
				Deprecated(),
		},
	)
}

func init() {
	err := service.RegisterBatchInput("redpanda_migrator", redpandaMigratorInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			if err := license.CheckRunningEnterprise(mgr); err != nil {
				return nil, err
			}

			tmpOpts, err := kafka.FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			clientOpts := append([]kgo.Opt{}, tmpOpts...)

			if tmpOpts, err = kafka.FranzConsumerOptsFromConfig(conf); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, tmpOpts...)

			clientLabel := mgr.Label()
			if clientLabel == "" {
				clientLabel = rmiResourceDefaultLabel
			}

			rdr, err := kafka.NewFranzReaderOrderedFromConfig(conf, mgr,
				func() ([]kgo.Opt, error) {
					return clientOpts, nil
				})
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, &redpandaMigratorInput{
				FranzReaderOrdered: rdr,
				clientLabel:        clientLabel,
				mgr:                mgr,
			})
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type redpandaMigratorInput struct {
	*kafka.FranzReaderOrdered

	clientLabel string

	mgr *service.Resources
}

func (rmi *redpandaMigratorInput) Connect(ctx context.Context) error {
	if err := rmi.FranzReaderOrdered.Connect(ctx); err != nil {
		return err
	}

	if err := kafka.FranzSharedClientSet(rmi.clientLabel, &kafka.FranzSharedClientInfo{
		Client: rmi.FranzReaderOrdered.Client,
	}, rmi.mgr); err != nil {
		rmi.mgr.Logger().Warnf("Failed to store client connection for sharing: %s", err)
	}

	return nil
}

func (rmi *redpandaMigratorInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		batch, ack, err := rmi.FranzReaderOrdered.ReadBatch(ctx)
		if err != nil {
			return batch, ack, err
		}

		batch = slices.DeleteFunc(batch, func(msg *service.Message) bool {
			b, err := msg.AsBytes()

			if b == nil {
				rmi.mgr.Logger().Debugf("Skipping tombstone message")
				return true
			}

			return err != nil
		})

		if len(batch) == 0 {
			_ = ack(ctx, nil) // TODO: Log this error?
			continue
		}

		return batch, ack, nil
	}
}

func (rmi *redpandaMigratorInput) Close(ctx context.Context) error {
	_, _ = kafka.FranzSharedClientPop(rmi.clientLabel, rmi.mgr)

	return rmi.FranzReaderOrdered.Close(ctx)
}
