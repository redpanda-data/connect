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

func redpandaMigratorOutputSpec() *service.ConfigSpec {
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
	service.MustRegisterBatchOutput("redpanda_migrator", redpandaMigratorOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if output, err = redpandaMigratorOutputFromParsed(conf, mgr); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt(rmoFieldMaxInFlight); err != nil {
				return
			}

			return
		})
}

type redpandaMigratorOutput struct {
	*FranzWriter
	topicPrefix                  string
	destTopicResolver            *service.InterpolatedString
	inputResource                string
	createTopicCfg               createTopicConfig
	translateSchemaIDs           bool
	schemaRegistryOutputResource srResourceKey

	// Shared client resources
	client      *kgo.Client
	clientMu    sync.Mutex // TODO(mmt): is this needed?
	clientOpts  []kgo.Opt
	connDetails *FranzConnectionDetails

	// Caches
	schemaIDCache sync.Map
	topicCache    sync.Map
	once          sync.Once

	// Resources
	logger *service.Logger
	mgr    *service.Resources
}

func redpandaMigratorOutputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*redpandaMigratorOutput, error) {
	o := &redpandaMigratorOutput{
		logger: mgr.Logger(),
		mgr:    mgr,
	}

	var err error
	o.FranzWriter, err = NewFranzWriterFromConfig(conf, NewFranzWriterHooks(
		func(ctx context.Context, fn FranzSharedClientUseFn) error {
			o.clientMu.Lock()
			defer o.clientMu.Unlock()

			if o.client == nil {
				var err error
				if o.client, err = NewFranzClient(ctx, o.clientOpts...); err != nil {
					return err
				}
			}

			return fn(&FranzSharedClientInfo{Client: o.client, ConnDetails: o.connDetails})
		}).WithYieldClientFn(
		func(context.Context) error {
			o.clientMu.Lock()
			defer o.clientMu.Unlock()

			if o.client == nil {
				return nil
			}

			o.client.Close()
			o.client = nil
			return nil
		}))
	if err != nil {
		return nil, err
	}
	o.OnWrite = o.onWrite

	if o.topicPrefix, err = conf.FieldString(rmoFieldTopicPrefix); err != nil {
		return nil, err
	}

	if o.topicPrefix != "" {
		if o.destTopicResolver, err = conf.FieldInterpolatedString(rmoFieldTopic); err != nil {
			return nil, err
		}
	}

	if o.inputResource, err = conf.FieldString(rmoFieldInputResource); err != nil {
		return nil, err
	}

	if o.createTopicCfg.replicationFactorOverride, err = conf.FieldBool(rmoFieldRepFactorOverride); err != nil {
		return nil, err
	}

	if o.createTopicCfg.replicationFactor, err = conf.FieldInt(rmoFieldRepFactor); err != nil {
		return nil, err
	}

	if o.translateSchemaIDs, err = conf.FieldBool(rmoFieldTranslateSchemaIDs); err != nil {
		return nil, err
	}

	if o.createTopicCfg.isServerlessBroker, err = conf.FieldBool(rmoFieldIsServerless); err != nil {
		return nil, err
	}

	if o.translateSchemaIDs {
		var res string
		if res, err = conf.FieldString(rmoFieldSchemaRegistryOutputResource); err != nil {
			return nil, err
		}
		o.schemaRegistryOutputResource = srResourceKey(res)
	}

	if o.connDetails, err = FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
		return nil, err
	}
	o.clientOpts = append(o.clientOpts, o.connDetails.FranzOpts()...)
	var opts []kgo.Opt
	if opts, err = FranzProducerOptsFromConfig(conf); err != nil {
		return nil, err
	}
	o.clientOpts = append(o.clientOpts, opts...)

	return o, nil
}

func (o *redpandaMigratorOutput) onWrite(ctx context.Context, _ *kgo.Client, records []*kgo.Record) error {
	return FranzSharedClientUse(o.inputResource, o.mgr, func(details *FranzSharedClientInfo) error {
		o.once.Do(func() {
			o.logger.Infof("Creating topics for %s", o.inputResource)
			count := o.tryCreateAllTopics(ctx, details)
			o.logger.Infof("Created %d topics for %s", count, o.inputResource)
		})

		if err := o.updateTopicsInRecords(ctx, details.Client, records); err != nil {
			return err
		}
		if o.translateSchemaIDs {
			if err := o.updateSchemaIDsInRecords(ctx, records); err != nil {
				return err
			}
		}
		return nil
	})
}

func (o *redpandaMigratorOutput) tryCreateAllTopics(ctx context.Context, details *FranzSharedClientInfo) int {
	inputClient := details.Client

	count := 0
	for _, topic := range inputClient.GetConsumeTopics() {
		if _, err := o.createTopicIfNeeded(ctx, inputClient, topic); err != nil {
			// Continue on error, we will attempt to create the topic again when
			// we receive a message.
			o.logger.Errorf("Failed to create topic %q: %s", topic, err)
		} else {
			count++
		}
	}

	return count
}

func (o *redpandaMigratorOutput) updateTopicsInRecords(ctx context.Context, inputClient *kgo.Client, records []*kgo.Record) error {
	for _, record := range records {
		destTopic, err := o.createTopicIfNeeded(ctx, inputClient, record.Topic)
		if err != nil {
			return err
		}
		record.Topic = destTopic
	}
	return nil
}

func (o *redpandaMigratorOutput) createTopicIfNeeded(ctx context.Context, inputClient *kgo.Client, topic string) (string, error) {
	if cachedTopic, ok := o.topicCache.Load(topic); ok {
		return cachedTopic.(string), nil
	}

	destTopic, err := o.resolveTopic(topic)
	if err != nil {
		return "", fmt.Errorf("resolve topic %q: %w", topic, err)
	}

	cfg := o.createTopicCfg // copy
	cfg.srcTopic = topic
	cfg.destTopic = destTopic
	if err := createTopic(ctx, o.logger, inputClient, o.client, cfg); err != nil {
		if !errors.Is(err, kerr.TopicAlreadyExists) {
			return "", fmt.Errorf("create topic %q: %w", topic, err)
		}
	}
	if err := createACLs(ctx, topic, destTopic, inputClient, o.client); err != nil {
		return "", fmt.Errorf("create ACLs for topic %q: %w", topic, err)
	}
	o.logger.Infof("Created topic and ACLs %s -> %s", topic, destTopic)

	o.topicCache.Store(topic, destTopic)
	return destTopic, nil
}

func (o *redpandaMigratorOutput) resolveTopic(topic string) (string, error) {
	if o.topicPrefix == "" {
		return topic, nil
	}

	// Hack: The current message corresponds to a specific topic, but we want to
	// create all topics, so we assume users will only use the `kafka_topic`
	// metadata when specifying the `topic`.
	msg := service.NewMessage(nil)
	msg.MetaSetMut("kafka_topic", topic)
	destTopic, err := o.destTopicResolver.TryString(msg)
	if err != nil {
		return "", fmt.Errorf("failed to parse destination topic: %s", err)
	}
	if destTopic == "" {
		return "", errors.New("failed to parse destination topic: empty string")
	}
	destTopic = o.topicPrefix + destTopic

	return destTopic, nil
}

func (o *redpandaMigratorOutput) updateSchemaIDsInRecords(ctx context.Context, records []*kgo.Record) error {
	res, ok := o.mgr.GetGeneric(o.schemaRegistryOutputResource)
	if !ok {
		return fmt.Errorf("schema_registry output resource %q not found", o.schemaRegistryOutputResource)
	}
	srOutput := res.(*schemaRegistryOutput)

	for _, record := range records {
		if err := o.updateRecordSchemaID(ctx, srOutput, record); err != nil {
			return fmt.Errorf("update schema ID in record offset %d on topic %s: %w", record.Offset, record.Topic, err)
		}
	}

	return nil
}

func (o *redpandaMigratorOutput) updateRecordSchemaID(ctx context.Context, srOutput *schemaRegistryOutput, record *kgo.Record) error {
	// Do not attempt to translate schema IDs for a tombstone
	if record.Value == nil {
		return nil
	}

	var ch franz_sr.ConfluentHeader
	schemaID, _, err := ch.DecodeID(record.Value)
	if err != nil {
		return fmt.Errorf("decode schema ID: %w", err)
	}

	var destSchemaID int
	if cachedID, ok := o.schemaIDCache.Load(schemaID); !ok {
		var err error
		destSchemaID, err = srOutput.GetDestinationSchemaID(ctx, schemaID)
		if err != nil {
			return fmt.Errorf("translate schema ID: %w", err)
		}
		o.schemaIDCache.Store(schemaID, destSchemaID)
	} else {
		destSchemaID = cachedID.(int)
	}

	if err = sr.UpdateID(record.Value, destSchemaID); err != nil {
		return err
	}

	return nil
}
