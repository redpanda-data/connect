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
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
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

Emits a ` + "`redpanda_lag`" + ` metric with ` + "`topic`" + ` and ` + "`partition`" + ` labels for each consumed topic.

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
		LintRule(FranzConsumerFieldLintRules)
}

func redpandaMigratorInputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionFields(),
		FranzConsumerFields(),
		FranzReaderOrderedConfigFields(),
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
	service.MustRegisterBatchInput("redpanda_migrator", redpandaMigratorInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			tmpOpts, err := FranzConnectionOptsFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			clientOpts := append([]kgo.Opt{}, tmpOpts...)

			if tmpOpts, err = FranzConsumerOptsFromConfig(conf); err != nil {
				return nil, err
			}
			clientOpts = append(clientOpts, tmpOpts...)

			clientLabel := mgr.Label()
			if clientLabel == "" {
				clientLabel = rmiResourceDefaultLabel
			}

			rdr, err := NewFranzReaderOrderedFromConfig(conf, mgr,
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
}

//------------------------------------------------------------------------------

type redpandaMigratorInput struct {
	*FranzReaderOrdered

	clientLabel string

	mgr *service.Resources
}

func (rmi *redpandaMigratorInput) Connect(ctx context.Context) error {
	if err := rmi.FranzReaderOrdered.Connect(ctx); err != nil {
		return err
	}

	if err := FranzSharedClientSet(rmi.clientLabel, &FranzSharedClientInfo{
		Client: rmi.Client,
	}, rmi.mgr); err != nil {
		rmi.mgr.Logger().Warnf("Failed to store client connection for sharing: %s", err)
	}

	return nil
}

func (rmi *redpandaMigratorInput) Close(ctx context.Context) error {
	_, _ = FranzSharedClientPop(rmi.clientLabel, rmi.mgr)

	return rmi.FranzReaderOrdered.Close(ctx)
}
