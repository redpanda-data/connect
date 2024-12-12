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
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	rmiFieldOutputResource            = "output_resource"
	rmiFieldReplicationFactorOverride = "replication_factor_override"
	rmiFieldReplicationFactor         = "replication_factor"

	// Deprecated
	rmiFieldMultiHeader = "multi_header"
	rmiFieldBatchSize   = "batch_size"

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

This input should be used in combination with a ` + "`redpanda_migrator`" + ` output which it can query for existing topics.

When a consumer group is specified this input consumes one or more topics where partitions will automatically balance across any other connected clients with the same consumer group. When a consumer group is not specified topics can either be consumed in their entirety or with explicit partitions.

It attempts to create all selected topics along with their associated ACLs in the broker that the ` + "`redpanda_migrator`" + ` output points to identified by the label specified in ` + "`output_resource`" + `.

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
			service.NewStringField(rmiFieldOutputResource).
				Description("The label of the redpanda_migrator output in which the currently selected topics need to be created before attempting to read messages.").
				Default(rmoResourceDefaultLabel).
				Advanced(),
			service.NewBoolField(rmiFieldReplicationFactorOverride).
				Description("Use the specified replication factor when creating topics.").
				Default(true).
				Advanced(),
			service.NewIntField(rmiFieldReplicationFactor).
				Description("Replication factor for created topics. This is only used when `replication_factor_override` is set to `true`.").
				Default(3).
				Advanced(),

			// Deprecated
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

			replicationFactorOverride, err := conf.FieldBool(rmiFieldReplicationFactorOverride)
			if err != nil {
				return nil, err
			}

			replicationFactor, err := conf.FieldInt(rmiFieldReplicationFactor)
			if err != nil {
				return nil, err
			}

			outputResource, err := conf.FieldString(rmiFieldOutputResource)
			if err != nil {
				return nil, err
			}

			clientLabel := mgr.Label()
			if clientLabel == "" {
				clientLabel = rmiResourceDefaultLabel
			}

			rdr, err := kafka.NewFranzReaderOrderedFromConfig(conf, mgr,
				func() ([]kgo.Opt, error) {
					return clientOpts, nil
				},
				nil,
				func(ctx context.Context, res *service.Resources, client *kgo.Client) {
					if err = kafka.FranzSharedClientSet(clientLabel, &kafka.FranzSharedClientInfo{
						Client: client,
					}, res); err != nil {
						res.Logger().With("error", err).Warn("Failed to store client connection for sharing")
					}

					topics := client.GetConsumeTopics()
					if len(topics) > 0 {
						mgr.Logger().Debugf("Consuming from topics: %s", topics)
					} else {
						mgr.Logger().Warn("No matching topics found")
						return
					}

					// Make multiple attempts until the output connects in the background.
					// TODO: It would be nicer to somehow get notified when the output is ready.
				loop:
					for {
						if err = kafka.FranzSharedClientUse(outputResource, res, func(details *kafka.FranzSharedClientInfo) error {
							for _, topic := range topics {
								if err := createTopic(ctx, topic, replicationFactorOverride, replicationFactor, client, details.Client); err != nil && err != errTopicAlreadyExists {
									// We could end up attempting to create a topic which doesn't have any messages in it, so if that
									// fails, we can just log an error and carry on. If it does contain messages, the output will
									// attempt to create it again anyway and will trigger and error if it can't.
									// The output `topicCache` could be populated here to avoid the redundant call to create topics, but
									// it's not worth the complexity.
									mgr.Logger().Errorf("Failed to create topic %q and ACLs: %s", topic, err)
								} else {
									if err == errTopicAlreadyExists {
										mgr.Logger().Debugf("Topic %q already exists", topic)
									} else {
										mgr.Logger().Infof("Created topic %q in output cluster", topic)
									}
									if err := createACLs(ctx, topic, client, details.Client); err != nil {
										mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
									}
								}
							}
							return nil
						}); err == nil {
							break
						}

						select {
						case <-time.After(100 * time.Millisecond):
						case <-ctx.Done():
							break loop
						}
					}
				},
				func(res *service.Resources) {
					_, _ = kafka.FranzSharedClientPop(clientLabel, res)
				})
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------
