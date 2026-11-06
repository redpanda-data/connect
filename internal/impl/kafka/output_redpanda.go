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
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	roFieldMaxInFlight = "max_in_flight"
	roFieldBatching    = "batching"
)

func redpandaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("4.39.0").
		Categories("Services").
		Summary("A Kafka output using the https://github.com/twmb/franz-go[Franz Kafka client library^].").
		Description(`
Writes a batch of messages to Kafka brokers and waits for acknowledgement before propagating it back to the input.
`).
		Fields(redpandaOutputConfigFields()...).
		LintRule(FranzWriterConfigLints()).
		Example("Simple Common Output", "Data is generated and written to a topic `bar`, targeting the cluster configured within the `redpanda` block at the bottom. This lets you configure TLS and SASL once and share them across multiple inputs and outputs.", `
input:
  generate:
    interval: 1s
    mapping: 'root.name = fake("name")'

pipeline:
  processors:
    - mutation: |
        root.id = uuid_v4()
        root.loud_name = this.name.uppercase()

output:
  redpanda:
    topic: bar
    key: ${! @id }

redpanda:
  seed_brokers: [ "127.0.0.1:9092" ]
  tls:
    enabled: true
  sasl:
    - mechanism: SCRAM-SHA-512
      password: bar
      username: foo
`).
		Example("Deletes From CDC", "Changes from a PostgreSQL table are written to a compacted topic keyed by the primary key. The `postgres_cdc` input sets the `operation` metadata field to `delete` for deleted rows, so `tombstone` writes those as records with a null value and compaction removes the key. Other operations are written with their row as the value.", `
input:
  postgres_cdc:
    dsn: postgres://user:pass@localhost:5432/db?sslmode=disable
    slot_name: users_slot
    schema: public
    tables: [ users ]
    stream_snapshot: true

output:
  redpanda:
    seed_brokers: [ "127.0.0.1:9092" ]
    topic: users
    key: ${! this.id }
    tombstone: ${! @operation == "delete" }
`).
		Example("Topic Mirror", "Records are copied from one topic to another. The `redpanda` input sets the `kafka_tombstone_message` metadata field to `true` for tombstones and `false` otherwise, so `tombstone` keeps tombstones as records with a null value instead of writing them as empty records.", `
input:
  redpanda:
    seed_brokers: [ "127.0.0.1:9092" ]
    topics: [ source ]
    consumer_group: mirror

output:
  redpanda:
    seed_brokers: [ "127.0.0.1:9092" ]
    topic: destination
    key: ${! @kafka_key }
    tombstone: ${! @kafka_tombstone_message }
`)
}

func redpandaOutputConfigFields() []*service.ConfigField {
	return slices.Concat(
		FranzConnectionOptionalFields(),
		FranzWriterConfigFields(),
		[]*service.ConfigField{
			service.NewIntField(roFieldMaxInFlight).
				Description(FranzMaxInFlightDescription + " Increase this number to improve throughput until performance plateaus.").
				Default(256),
			service.NewBatchPolicyField(roFieldBatching).
				Description("Optional explicit batching policy for the output. Note that when batches are formed at the input level they can be expanded by this policy, but not contracted. When consuming data from a Redpanda input it is recommended to tune batches from the input config via the `max_yield_batch_bytes` field, or the `unordered_processing.batching` field if appropriate.").Version("4.75.0").
				ShortDescription("Optional explicit batching policy for the output. Input-level batches can be expanded by this policy but not contracted."),
			service.NewInjectTracingSpanMappingField().Version("4.85.0"),
		},
		FranzProducerFields(),
	)
}

func init() {
	service.MustRegisterBatchOutput("redpanda", redpandaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt(roFieldMaxInFlight); err != nil {
				return
			}

			var connDetails *FranzConnectionDetails
			if connDetails, err = FranzConnectionDetailsFromConfig(conf, mgr.Logger()); err != nil {
				return
			}

			var producerOpts []kgo.Opt
			if producerOpts, err = FranzProducerOptsFromConfig(conf); err != nil {
				return
			}

			if batchPolicy, err = conf.FieldBatchPolicy(roFieldBatching); err != nil {
				return
			}

			if connDetails.IsConfigured() {
				if output, err = NewFranzWriterFromConfig(
					conf,
					newRedpandaOutputClient(connDetails, producerOpts).hooks(),
				); err != nil {
					mgr.Logger().Errorf("Failed creating writer from config: %v", err)
					return
				}
			} else {
				mgr.Logger().Info("Connection fields omitted, falling back to common redpanda config.")

				// We're using a common redpanda block to determine the connection.
				if output, err = NewFranzWriterFromConfig(
					conf,
					NewFranzWriterHooks(
						func(_ context.Context, fn FranzSharedClientUseFn) error {
							return FranzSharedClientUse(SharedGlobalRedpandaClientKey, mgr, fn)
						},
					).WithYieldClientFn(
						func(context.Context) error { return nil },
					),
				); err != nil {
					mgr.Logger().Errorf("Failed creating writer from config: %v", err)
					return
				}
			}

			if output, err = conf.WrapBatchOutputExtractTracingSpanMapping("redpanda", output); err != nil {
				return
			}

			return
		})
}

// redpandaOutputClient is the client of a redpanda output with its own
// connection fields, created on first use. Writes hold the read lock for as
// long as they use the client, including the wait for their acks, so that up
// to max_in_flight batches are produced concurrently. Creating and closing the
// client take the write lock, so the client is never closed under a write that
// is still waiting on its acks.
type redpandaOutputClient struct {
	connDetails  *FranzConnectionDetails
	producerOpts []kgo.Opt

	mu     sync.RWMutex
	client *kgo.Client
}

func newRedpandaOutputClient(connDetails *FranzConnectionDetails, producerOpts []kgo.Opt) *redpandaOutputClient {
	return &redpandaOutputClient{
		connDetails:  connDetails,
		producerOpts: producerOpts,
	}
}

func (c *redpandaOutputClient) hooks() franzWriterHooks {
	return NewFranzWriterHooks(c.use).WithYieldClientFn(c.close)
}

func (c *redpandaOutputClient) use(ctx context.Context, fn FranzSharedClientUseFn) error {
	client, err := c.rlockClient(ctx)
	if err != nil {
		return err
	}
	defer c.mu.RUnlock()

	return fn(&FranzSharedClientInfo{
		Client:      client,
		ConnDetails: c.connDetails,
	})
}

// rlockClient returns the client with the read lock held, creating the client
// first if there is none. A read lock cannot be upgraded, so the client is
// created under the write lock and checked again once the read lock is back.
func (c *redpandaOutputClient) rlockClient(ctx context.Context) (*kgo.Client, error) {
	c.mu.RLock()
	for c.client == nil {
		c.mu.RUnlock()
		if err := c.connect(ctx); err != nil {
			return nil, err
		}
		c.mu.RLock()
	}
	return c.client, nil
}

func (c *redpandaOutputClient) connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return nil
	}
	client, err := NewFranzClient(ctx, append(c.connDetails.FranzOpts(), c.producerOpts...)...)
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *redpandaOutputClient) close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client == nil {
		return nil
	}
	c.client.Close()
	c.client = nil
	return nil
}
