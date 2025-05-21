// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	rmoFieldTopic                        = "topic"
	rmoFieldTopicPrefix                  = "topic_prefix"
	rmoFieldMaxInFlight                  = "max_in_flight"
	rmoFieldBatching                     = "batching"
	rmoFieldInputResource                = "input_resource"
	rmoFieldRepFactorOverride            = "replication_factor_override"
	rmoFieldRepFactor                    = "replication_factor"
	rmoFieldTranslateSchemaIDs           = "translate_schema_ids"
	rmoFieldIsServerless                 = "is_serverless"
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
		LintRule(FranzWriterConfigLints()).
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
		FranzConnectionFields(),
		FranzWriterConfigFields(),
		[]*service.ConfigField{
			service.NewInterpolatedStringField(rmoFieldTopicPrefix).
				Description("The topic prefix.").Default("").Advanced(),
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
			service.NewBoolField(rmoFieldTranslateSchemaIDs).Description("Translate schema IDs.").Default(false).Advanced(),
			service.NewBoolField(rmoFieldIsServerless).Description("Set this to `true` when using Serverless clusters in Redpanda Cloud.").Default(false).Advanced(),
			service.NewStringField(rmoFieldSchemaRegistryOutputResource).
				Description("The label of the schema_registry output to use for fetching schema IDs.").
				Default(sroResourceDefaultLabel).
				Advanced(),

			// Deprecated
			service.NewStringField(rmoFieldRackID).Deprecated(),
			service.NewBatchPolicyField(rmoFieldBatching).Deprecated(),
		},
		FranzProducerFields(),
	)
}

func init() {
	service.MustRegisterBatchOutput("redpanda_migrator", redpandaMigratorOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			topicPrefix, err := conf.FieldString(rmoFieldTopicPrefix)
			if err != nil {
				return
			}
			var destTopicResolver *service.InterpolatedString
			if topicPrefix != "" {
				if destTopicResolver, err = conf.FieldInterpolatedString(rmoFieldTopic); err != nil {
					return
				}
			}

			if maxInFlight, err = conf.FieldInt(rmoFieldMaxInFlight); err != nil {
				return
			}

			var inputResource string
			if inputResource, err = conf.FieldString(rmoFieldInputResource); err != nil {
				return
			}

			createTopicCfg := createTopicConfig{}

			if createTopicCfg.replicationFactorOverride, err = conf.FieldBool(rmoFieldRepFactorOverride); err != nil {
				return
			}

			if createTopicCfg.replicationFactor, err = conf.FieldInt(rmoFieldRepFactor); err != nil {
				return
			}

			var translateSchemaIDs bool
			if translateSchemaIDs, err = conf.FieldBool(rmoFieldTranslateSchemaIDs); err != nil {
				return
			}

			if createTopicCfg.isServerlessBroker, err = conf.FieldBool(rmoFieldIsServerless); err != nil {
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

			var connDetails *FranzConnectionDetails
			if connDetails, err = FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
				return
			}
			clientOpts = append(clientOpts, connDetails.FranzOpts()...)

			if tmpOpts, err = FranzProducerOptsFromConfig(conf); err != nil {
				return
			}
			clientOpts = append(clientOpts, tmpOpts...)

			clientOpts = append(clientOpts, kgo.AllowAutoTopicCreation()) // TODO: Configure this?

			var client *kgo.Client
			var clientMut sync.Mutex
			// Stores the source to destination SchemaID mapping.
			var schemaIDCache sync.Map
			var topicCache sync.Map
			var runOnce sync.Once
			output, err = NewFranzWriterFromConfig(
				conf,
				NewFranzWriterHooks(
					func(ctx context.Context, fn FranzSharedClientUseFn) error {
						clientMut.Lock()
						defer clientMut.Unlock()

						if client == nil {
							var err error
							if client, err = kgo.NewClient(clientOpts...); err != nil {
								return err
							}
						}

						return fn(&FranzSharedClientInfo{Client: client, ConnDetails: connDetails})
					}).WithYieldClientFn(
					func(context.Context) error {
						clientMut.Lock()
						defer clientMut.Unlock()

						if client == nil {
							return nil
						}

						client.Close()
						client = nil
						return nil
					}).WithWriteHookFn(
					func(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
						// Try to create all topics which the input `redpanda_migrator` resource is configured to read
						// from when we receive the first message.
						runOnce.Do(func() {
							err := FranzSharedClientUse(inputResource, mgr, func(details *FranzSharedClientInfo) error {
								inputClient := details.Client
								outputClient := client
								topics := inputClient.GetConsumeTopics()

								for _, topic := range topics {
									destTopic := topic
									// Check if the user specified a topic alias and resolve it.
									if topicPrefix != "" {
										// Hack: The current message corresponds to a specific topic, but we want to
										// create all topics, so we assume users will only use the `kafka_topic`
										// metadata when specifying the `topic`.
										tmpMsg := service.NewMessage(nil)
										tmpMsg.MetaSetMut("kafka_topic", topic)
										if destTopic, err = destTopicResolver.TryString(tmpMsg); err != nil {
											return fmt.Errorf("failed to parse destination topic: %s", err)
										} else if destTopic == "" {
											return errors.New("failed to parse destination topic: empty string")
										}

										destTopic = topicPrefix + destTopic
									}

									cfg := createTopicCfg
									cfg.srcTopic = topic
									cfg.destTopic = destTopic
									if err := createTopic(ctx, mgr.Logger(), inputClient, outputClient, cfg); err != nil {
										if errors.Is(err, kerr.TopicAlreadyExists) {
											topicCache.Store(topic, struct{}{})
											mgr.Logger().Debugf("Topic %q already exists", topic)
										} else {
											// This may be a topic which doesn't have any messages in it, so if we
											// failed to create it now, we log an error and continue. If it does contain
											// messages, we'll attempt to create it again anyway when receiving a
											// message from it.
											mgr.Logger().Errorf("Failed to create topic %q and ACLs: %s", topic, err)
										}
									} else {
										mgr.Logger().Infof("Created topic %q", destTopic)

										if err := createACLs(ctx, topic, destTopic, inputClient, outputClient); err != nil {
											mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", topic, err)
										}

										topicCache.Store(topic, struct{}{})
									}
								}

								return nil
							})
							if err != nil {
								mgr.Logger().Errorf("Failed to fetch topics from input %q: %s", inputResource, err)
							}
						})

						var srOutput *schemaRegistryOutput
						if translateSchemaIDs {
							if res, ok := mgr.GetGeneric(schemaRegistryOutputResource); ok {
								srOutput = res.(*schemaRegistryOutput)
							} else {
								mgr.Logger().Warnf("schema_registry output resource %q not found; skipping schema ID translation", schemaRegistryOutputResource)
							}
						}
						translateSchemaID := func(record *kgo.Record) error {
							if srOutput == nil {
								return nil
							}

							var ch franz_sr.ConfluentHeader
							schemaID, _, err := ch.DecodeID(record.Value)
							if err != nil {
								return fmt.Errorf("failed to extract schema ID: %s", err)
							}

							var destSchemaID int
							if cachedID, ok := schemaIDCache.Load(schemaID); !ok {
								if destSchemaID, err = srOutput.GetDestinationSchemaID(ctx, schemaID); err != nil {
									return fmt.Errorf("failed to fetch destination schema ID: %s", err)
								}
								schemaIDCache.Store(schemaID, destSchemaID)
							} else {
								destSchemaID = cachedID.(int)
							}

							if err = sr.UpdateID(record.Value, destSchemaID); err != nil {
								return fmt.Errorf("failed to update schema ID: %s", err)
							}

							return nil
						}

						// The current record may be coming from a topic which was created later during runtime, so we
						// need to try and create it if we haven't done so already.
						if err := FranzSharedClientUse(inputResource, mgr, func(details *FranzSharedClientInfo) error {
							for i, record := range records {
								if err := translateSchemaID(record); err != nil {
									mgr.Logger().Warnf("Failed to update schema ID in record index %d on topic %s: %q", i, record.Topic, err)
								}

								srcTopic := record.Topic
								record.Topic = topicPrefix + record.Topic

								if _, ok := topicCache.Load(srcTopic); !ok {
									cfg := createTopicCfg
									cfg.srcTopic = srcTopic
									cfg.destTopic = record.Topic
									if err := createTopic(ctx, mgr.Logger(), details.Client, client, cfg); err != nil {
										if errors.Is(err, kerr.TopicAlreadyExists) {
											mgr.Logger().Debugf("Topic %q already exists", record.Topic)
										} else {
											return fmt.Errorf("failed to create topic %q and ACLs: %s", record.Topic, err)
										}
									} else {
										mgr.Logger().Infof("Created topic %q", record.Topic)
									}

									if err := createACLs(ctx, srcTopic, record.Topic, details.Client, client); err != nil {
										mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", record.Topic, err)
									}

									topicCache.Store(srcTopic, struct{}{})
								}
							}
							return nil
						}); err != nil {
							mgr.Logger().With("error", err, "resource", inputResource).Warn("Failed to access shared client for given resource identifier")
						}

						return nil
					}))
			return
		})
}
