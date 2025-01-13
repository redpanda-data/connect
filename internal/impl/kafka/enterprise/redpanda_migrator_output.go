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

	"github.com/twmb/franz-go/pkg/kgo"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
)

const (
	rmoFieldMaxInFlight                  = "max_in_flight"
	rmoFieldBatching                     = "batching"
	rmoFieldInputResource                = "input_resource"
	rmoFieldRepFactorOverride            = "replication_factor_override"
	rmoFieldRepFactor                    = "replication_factor"
	rmoFieldTranslateSchemaIDs           = "translate_schema_ids"
	rmoFieldSchemaRegistryOutputResource = "schema_registry_output_resource"

	// Deprecated
	rmoFieldRackID = "rack_id"

	rmoResourceDefaultLabel = "redpanda_migrator_output"
)

func redpandaMigratorOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.37.0").
		Summary("A Redpanda Migrator output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description(`
Writes a batch of messages to a Kafka broker and waits for acknowledgement before propagating it back to the input.

This output should be used in combination with a `+"`redpanda_migrator`"+` input identified by the label specified in
`+"`input_resource`"+` which it can query for topic and ACL configurations. Once connected, the output will attempt to
create all topics which the input consumes from along with their ACLs.

If the configured broker does not contain the current message topic, this output attempts to create it along with its
ACLs.

ACL migration adheres to the following principles:

- `+"`ALLOW WRITE`"+` ACLs for topics are not migrated
- `+"`ALLOW ALL`"+` ACLs for topics are downgraded to `+"`ALLOW READ`"+`
- Only topic ACLs are migrated, group ACLs are not migrated
`).
		Fields(redpandaMigratorOutputConfigFields()...).
		LintRule(kafka.FranzWriterConfigLints()).
		Example("Transfer data", "Writes messages to the configured broker and creates topics and topic ACLs if they don't exist. It also ensures that the message order is preserved.", `
output:
  redpanda_migrator:
    seed_brokers: [ "127.0.0.1:9093" ]
    topic: ${! metadata("kafka_topic").or(throw("missing kafka_topic metadata")) }
    key: ${! metadata("kafka_key") }
    partitioner: manual
    partition: ${! metadata("kafka_partition").or(throw("missing kafka_partition metadata")) }
    timestamp_ms: ${! metadata("kafka_timestamp_ms").or(timestamp_unix_milli()) }
    input_resource: redpanda_migrator_input
    max_in_flight: 1
`)
}

func redpandaMigratorOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		kafka.FranzWriterConfigFields(),
		[]*service.ConfigField{
			service.NewIntField(rmoFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(256),
			service.NewStringField(rmoFieldInputResource).
				Description("The label of the redpanda_migrator input from which to read the configurations for topics and ACLs which need to be created.").
				Default(rmiResourceDefaultLabel).
				Advanced(),
			service.NewBoolField(rmoFieldRepFactorOverride).
				Description("Use the specified replication factor when creating topics.").
				Default(true).
				Advanced(),
			service.NewIntField(rmoFieldRepFactor).
				Description("Replication factor for created topics. This is only used when `replication_factor_override` is set to `true`.").
				Default(3).
				Advanced(),
			service.NewBoolField(rmoFieldTranslateSchemaIDs).Description("Translate schema IDs.").Default(true).Advanced(),
			service.NewStringField(rmoFieldSchemaRegistryOutputResource).
				Description("The label of the schema_registry output to use for fetching schema IDs.").
				Default(sroResourceDefaultLabel).
				Advanced(),

			// Deprecated
			service.NewStringField(rmoFieldRackID).Deprecated(),
			service.NewBatchPolicyField(rmoFieldBatching).Deprecated(),
		},
		kafka.FranzProducerFields(),
	)
}

func init() {
	err := service.RegisterBatchOutput("redpanda_migrator", redpandaMigratorOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			if maxInFlight, err = conf.FieldInt(rmoFieldMaxInFlight); err != nil {
				return
			}

			var inputResource string
			if inputResource, err = conf.FieldString(rmoFieldInputResource); err != nil {
				return
			}

			var replicationFactorOverride bool
			if replicationFactorOverride, err = conf.FieldBool(rmoFieldRepFactorOverride); err != nil {
				return
			}

			var replicationFactor int
			if replicationFactor, err = conf.FieldInt(rmoFieldRepFactor); err != nil {
				return
			}

			var translateSchemaIDs bool
			if translateSchemaIDs, err = conf.FieldBool(rmoFieldTranslateSchemaIDs); err != nil {
				return
			}

			var schemaRegistryOutputResource srResourceKey
			if translateSchemaIDs {
				var res string
				if res, err = conf.FieldString(rmoFieldSchemaRegistryOutputResource); err != nil {
					return
				}
				schemaRegistryOutputResource = srResourceKey(res)
			}

			var tmpOpts, clientOpts []kgo.Opt

			var connDetails *kafka.FranzConnectionDetails
			if connDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
				return
			}
			clientOpts = append(clientOpts, connDetails.FranzOpts()...)

			if tmpOpts, err = kafka.FranzProducerOptsFromConfig(conf); err != nil {
				return
			}
			clientOpts = append(clientOpts, tmpOpts...)

			clientOpts = append(clientOpts, kgo.AllowAutoTopicCreation()) // TODO: Configure this?

			var client *kgo.Client
			var clientMut sync.Mutex
			// Stores the source to destination SchemaID mapping.
			var schemaIDCache sync.Map
			var topicCache sync.Map
			createdTopicsOnConnect := false
			output, err = kafka.NewFranzWriterFromConfig(conf,
				func(ctx context.Context, fn kafka.FranzSharedClientUseFn) error {
					clientMut.Lock()
					defer clientMut.Unlock()

					if client == nil {
						var err error
						if client, err = kgo.NewClient(clientOpts...); err != nil {
							return err
						}
					}

					if err := fn(&kafka.FranzSharedClientInfo{Client: client, ConnDetails: connDetails}); err != nil {
						return err
					}

					if createdTopicsOnConnect {
						return nil
					}

					// Make multiple attempts until the input connects in the background.
					// TODO: It would be nicer to somehow get notified when the input is ready.
				loop:
					for {
						if err = kafka.FranzSharedClientUse(inputResource, mgr, func(details *kafka.FranzSharedClientInfo) error {
							inputClient := details.Client
							outputClient := client
							topics := inputClient.GetConsumeTopics()

							for _, topic := range topics {
								if err := createTopic(ctx, topic, replicationFactorOverride, replicationFactor, inputClient, outputClient); err != nil {
									if err == errTopicAlreadyExists {
										topicCache.Store(topic, struct{}{})
										mgr.Logger().Debugf("Topic %q already exists", topic)
									} else {
										mgr.Logger().Errorf("Failed to create topic %q and ACLs: %s", topic, err)
									}

									// This may be a topic which doesn't have any messages in it, so if we can't create
									// it here, we just log an error and carry on. If it does contain messages, we'll
									// attempt to create it again anyway during WriteBatch and we'll raise another error
									// there if we can't.
									continue
								}

								mgr.Logger().Infof("Created topic %q", topic)

								if err := createACLs(ctx, topic, inputClient, outputClient); err != nil {
									mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
								}

								topicCache.Store(topic, struct{}{})
							}

							createdTopicsOnConnect = true
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

					return nil
				},
				func(context.Context) error {
					clientMut.Lock()
					defer clientMut.Unlock()

					if client == nil {
						return nil
					}

					client.Close()
					client = nil
					return nil
				},
				func(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
					if translateSchemaIDs {
						if res, ok := mgr.GetGeneric(schemaRegistryOutputResource); ok {
							srOutput := res.(*schemaRegistryOutput)

							var ch franz_sr.ConfluentHeader
							for recordIdx, record := range records {
								schemaID, _, err := ch.DecodeID(record.Value)
								if err != nil {
									mgr.Logger().Warnf("Failed to extract schema ID from message index %d on topic %q: %s", recordIdx, record.Topic, err)
									continue
								}

								var destSchemaID int
								if cachedID, ok := schemaIDCache.Load(schemaID); !ok {
									destSchemaID, err = srOutput.GetDestinationSchemaID(ctx, schemaID)
									if err != nil {
										mgr.Logger().Warnf("Failed to fetch destination schema ID from message index %d on topic %q: %s", recordIdx, record.Topic, err)
										continue
									}
									schemaIDCache.Store(schemaID, destSchemaID)
								} else {
									destSchemaID = cachedID.(int)
								}

								err = sr.UpdateID(record.Value, destSchemaID)
								if err != nil {
									mgr.Logger().Warnf("Failed to update schema ID in message index %d on topic %s: %q", recordIdx, record.Topic, err)
									continue
								}
							}
						} else {
							mgr.Logger().Warnf("schema_registry output resource %q not found; skipping schema ID translation", schemaRegistryOutputResource)
							return nil
						}

					}

					// Once we get here, the input should already be initialised and its pre-flight hook should have
					// been called already. Thus, we don't need to loop until the input is ready.
					if err := kafka.FranzSharedClientUse(inputResource, mgr, func(details *kafka.FranzSharedClientInfo) error {
						for _, record := range records {
							if _, ok := topicCache.Load(record.Topic); !ok {
								if err := createTopic(ctx, record.Topic, replicationFactorOverride, replicationFactor, details.Client, client); err != nil {
									if err == errTopicAlreadyExists {
										mgr.Logger().Debugf("Topic %q already exists", record.Topic)
									} else {
										return fmt.Errorf("failed to create topic %q and ACLs: %s", record.Topic, err)
									}
								}

								mgr.Logger().Infof("Created topic %q", record.Topic)

								if err := createACLs(ctx, record.Topic, details.Client, client); err != nil {
									mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", record.Topic, err)
								}

								topicCache.Store(record.Topic, struct{}{})
							}
						}
						return nil
					}); err != nil {
						mgr.Logger().With("error", err, "resource", inputResource).Warn("Failed to access shared client for given resource identifier")
					}

					return nil
				})
			return
		})
	if err != nil {
		panic(err)
	}
}
