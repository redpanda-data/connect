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

This output should be used in combination with a `+"`redpanda_migrator`"+` input which it can query for topic and ACL configurations.

If the configured broker does not contain the current message `+"topic"+`, it attempts to create it along with the topic
ACLs which are read automatically from the `+"`redpanda_migrator`"+` input identified by the label specified in
`+"`input_resource`"+`.

ACL migration adheres to the following principles:

- `+"`ALLOW WRITE`"+` ACLs for topics are not migrated
- `+"`ALLOW ALL`"+` ACLs for topics are downgraded to `+"`ALLOW READ`"+`
- Only topic ACLs are migrated, group ACLs are not migrated
`).
		Fields(RedpandaMigratorOutputConfigFields()...).
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

// RedpandaMigratorOutputConfigFields returns the full suite of config fields for a `redpanda_migrator` output using
// the franz-go client library.
func RedpandaMigratorOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		kafka.FranzWriterConfigFields(),
		[]*service.ConfigField{
			service.NewIntField(rmoFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(10),
			service.NewBatchPolicyField(rmoFieldBatching),
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
			if batchPolicy, err = conf.FieldBatchPolicy(rmoFieldBatching); err != nil {
				return
			}
			output, err = NewRedpandaMigratorWriterFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// RedpandaMigratorWriter implements a Kafka writer using the franz-go library.
type RedpandaMigratorWriter struct {
	recordConverter              *kafka.FranzWriter
	replicationFactorOverride    bool
	replicationFactor            int
	translateSchemaIDs           bool
	inputResource                string
	schemaRegistryOutputResource srResourceKey

	clientDetails *kafka.FranzConnectionDetails
	clientOpts    []kgo.Opt
	connMut       sync.Mutex
	client        *kgo.Client
	topicCache    sync.Map
	// Stores the source to destination SchemaID mapping.
	schemaIDCache        sync.Map
	schemaRegistryOutput *schemaRegistryOutput

	clientLabel string

	mgr *service.Resources
}

// NewRedpandaMigratorWriterFromConfig attempts to instantiate a RedpandaMigratorWriter from a parsed config.
func NewRedpandaMigratorWriterFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*RedpandaMigratorWriter, error) {
	w := RedpandaMigratorWriter{
		mgr: mgr,
	}

	var err error

	// NOTE: We do not provide closures for client access and yielding because
	// this writer is only used for its BatchToRecords method. If we ever expand
	// in order to use this as a full writer then we need to provide a full
	// suite of arguments here.
	if w.recordConverter, err = kafka.NewFranzWriterFromConfig(conf, nil, nil); err != nil {
		return nil, err
	}

	if w.clientDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
		return nil, err
	}
	w.clientOpts = w.clientDetails.FranzOpts()

	var tmpOpts []kgo.Opt
	if tmpOpts, err = kafka.FranzProducerOptsFromConfig(conf); err != nil {
		return nil, err
	}
	w.clientOpts = append(w.clientOpts, tmpOpts...)

	if w.inputResource, err = conf.FieldString(rmoFieldInputResource); err != nil {
		return nil, err
	}

	if w.replicationFactorOverride, err = conf.FieldBool(rmoFieldRepFactorOverride); err != nil {
		return nil, err
	}

	if w.replicationFactor, err = conf.FieldInt(rmoFieldRepFactor); err != nil {
		return nil, err
	}

	if w.translateSchemaIDs, err = conf.FieldBool(rmoFieldTranslateSchemaIDs); err != nil {
		return nil, err
	}

	if w.translateSchemaIDs {
		var res string
		if res, err = conf.FieldString(rmoFieldSchemaRegistryOutputResource); err != nil {
			return nil, err
		}
		w.schemaRegistryOutputResource = srResourceKey(res)
	}

	if w.clientLabel = mgr.Label(); w.clientLabel == "" {
		w.clientLabel = rmoResourceDefaultLabel
	}

	return &w, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (w *RedpandaMigratorWriter) Connect(ctx context.Context) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client != nil {
		return nil
	}

	var err error
	if w.client, err = kgo.NewClient(w.clientOpts...); err != nil {
		return err
	}

	// Check connectivity to cluster
	if err := w.client.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to cluster: %s", err)
	}

	if err = kafka.FranzSharedClientSet(w.clientLabel, &kafka.FranzSharedClientInfo{
		Client:      w.client,
		ConnDetails: w.clientDetails,
	}, w.mgr); err != nil {
		w.mgr.Logger().With("error", err).Warn("Failed to store client connection for sharing")
	}

	if w.translateSchemaIDs {
		if res, ok := w.mgr.GetGeneric(w.schemaRegistryOutputResource); ok {
			w.schemaRegistryOutput = res.(*schemaRegistryOutput)
		} else {
			w.mgr.Logger().Warnf("schema_registry output resource %q not found; skipping schema ID translation", w.schemaRegistryOutputResource)
		}
	}

	return nil
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (w *RedpandaMigratorWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	w.connMut.Lock()
	defer w.connMut.Unlock()

	if w.client == nil {
		return service.ErrNotConnected
	}

	records, err := w.recordConverter.BatchToRecords(ctx, b)
	if err != nil {
		return err
	}

	var ch franz_sr.ConfluentHeader
	if w.translateSchemaIDs && w.schemaRegistryOutput != nil {
		for recordIdx, record := range records {
			schemaID, _, err := ch.DecodeID(record.Value)
			if err != nil {
				w.mgr.Logger().Warnf("Failed to extract schema ID from message index %d on topic %q: %s", recordIdx, record.Topic, err)
				continue
			}

			var destSchemaID int
			if cachedID, ok := w.schemaIDCache.Load(schemaID); !ok {
				destSchemaID, err = w.schemaRegistryOutput.GetDestinationSchemaID(ctx, schemaID)
				if err != nil {
					w.mgr.Logger().Warnf("Failed to fetch destination schema ID from message index %d on topic %q: %s", recordIdx, record.Topic, err)
					continue
				}
				w.schemaIDCache.Store(schemaID, destSchemaID)
			} else {
				destSchemaID = cachedID.(int)
			}

			err = sr.UpdateID(record.Value, destSchemaID)
			if err != nil {
				w.mgr.Logger().Warnf("Failed to update schema ID in message index %d on topic %s: %q", recordIdx, record.Topic, err)
				continue
			}
		}
	}

	if err := kafka.FranzSharedClientUse(w.inputResource, w.mgr, func(details *kafka.FranzSharedClientInfo) error {
		for _, record := range records {
			if _, ok := w.topicCache.Load(record.Topic); !ok {
				if err := createTopic(ctx, record.Topic, w.replicationFactorOverride, w.replicationFactor, details.Client, w.client); err != nil && err != errTopicAlreadyExists {
					return fmt.Errorf("failed to create topic %q: %s", record.Topic, err)
				} else {
					if err == errTopicAlreadyExists {
						w.mgr.Logger().Debugf("Topic %q already exists", record.Topic)
					} else {
						w.mgr.Logger().Infof("Created topic %q", record.Topic)
					}
					if err := createACLs(ctx, record.Topic, details.Client, w.client); err != nil {
						w.mgr.Logger().Errorf("Failed to create ACLs for topic %q: %s", record.Topic, err)
					}

					w.topicCache.Store(record.Topic, struct{}{})
				}
			}
		}
		return nil
	}); err != nil {
		w.mgr.Logger().With("error", err, "resource", w.inputResource).Warn("Failed to access shared client for given resource identifier")
	}

	return w.client.ProduceSync(ctx, records...).FirstErr()
}

func (w *RedpandaMigratorWriter) disconnect() {
	if w.client == nil {
		return
	}
	_, _ = kafka.FranzSharedClientPop(w.clientLabel, w.mgr)
	w.client.Close()
	w.client = nil
}

// Close underlying connections.
func (w *RedpandaMigratorWriter) Close(ctx context.Context) error {
	w.disconnect()
	return nil
}
