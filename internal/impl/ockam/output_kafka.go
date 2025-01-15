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

package ockam

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

// this function is, almost, an exact copy of the init() function in ../kafka/output_kafka_franz.go
func init() {
	err := service.RegisterBatchOutput("ockam_kafka", ockamKafkaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("kafka", "max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("kafka", "batching"); err != nil {
				return
			}
			output, err = newOckamKafkaOutput(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

func ockamKafkaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Ockam").
		Categories("Services").
		Field(service.NewObjectField("kafka", slices.Concat(
			[]*service.ConfigField{
				service.NewStringListField("seed_brokers").Optional().
					Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
					Example([]string{"localhost:9092"}).
					Example([]string{"foo:9092", "bar:9092"}).
					Example([]string{"foo:9092,bar:9092"}),
				service.NewTLSToggledField("tls"),
				service.NewIntField("max_in_flight").
					Description("The maximum number of batches to be sending in parallel at any given time.").
					Default(10),
				service.NewBatchPolicyField("batching"),
			},
			kafka.FranzProducerFields(),
			kafka.FranzWriterConfigFields(),
		)...)).
		Field(service.NewBoolField("disable_content_encryption").Default(false)).
		Field(service.NewStringField("enrollment_ticket").Optional()).
		Field(service.NewStringField("identity_name").Optional()).
		Field(service.NewStringField("allow").Default("self").Optional()).
		Field(service.NewStringField("route_to_kafka_outlet").Default("self")).
		Field(service.NewStringField("allow_consumer").Default("self")).
		Field(service.NewStringField("route_to_consumer").Default("/ip4/127.0.0.1/tcp/6262")).
		Field(service.NewStringListField("encrypted_fields").
			Description("The fields to encrypt in the kafka messages, assuming the record is a valid JSON map. By default, the whole record is encrypted.").
			Default([]string{}))
}

//------------------------------------------------------------------------------

type ockamKafkaOutput struct {
	kafkaWriter *kafka.FranzWriter
	node        node
}

func newOckamKafkaOutput(conf *service.ParsedConfig, log *service.Logger) (*ockamKafkaOutput, error) {
	_, err := setupCommand()
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Node ----

	var ticket string
	if conf.Contains("enrollment_ticket") {
		ticket, err = conf.FieldString("enrollment_ticket")
		if err != nil {
			return nil, err
		}
	}

	var identityName string
	if conf.Contains("identity_name") {
		identityName, err = conf.FieldString("identity_name")
		if err != nil {
			return nil, err
		}
	}

	address, err := findAvailableLocalTCPAddress()
	if err != nil {
		return nil, err
	}

	n, err := newNode(identityName, address, ticket, "")
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Kafka Inlet ----

	routeToConsumer, err := conf.FieldString("route_to_consumer")
	if err != nil {
		return nil, err
	}

	allowConsumer, err := conf.FieldString("allow_consumer")
	if err != nil {
		return nil, err
	}

	kafkaInletAddress, err := findAvailableLocalTCPAddress()
	if err != nil {
		return nil, err
	}

	var routeToKafkaOutlet string
	routeToKafkaOutlet, err = conf.FieldString("route_to_kafka_outlet")
	if err != nil {
		return nil, err
	}

	var allowOutlet string
	allowOutlet, err = conf.FieldString("allow")
	if err != nil {
		return nil, err
	}

	var disableContentEncryption bool
	disableContentEncryption, err = conf.FieldBool("disable_content_encryption")
	if err != nil {
		return nil, err
	}

	var encryptedFields []string
	encryptedFields, err = conf.FieldStringList("encrypted_fields")
	if err != nil {
		return nil, err
	}

	err = n.createKafkaInlet("redpanda-connect-kafka-inlet", kafkaInletAddress, routeToKafkaOutlet, true, routeToConsumer, allowOutlet, "", allowConsumer, disableContentEncryption, encryptedFields)
	if err != nil {
		return nil, err
	}

	// ---- Create Ockam Kafka Outlet ----

	if routeToKafkaOutlet == "self" {
		// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
		seedBrokers, err := conf.FieldStringList("kafka", "seed_brokers")
		if err != nil {
			return nil, err
		}
		if len(seedBrokers) != 1 {
			log.Warn("ockam_kafka output only supports one seed broker")
		}
		bootstrapServer := strings.Split(seedBrokers[0], ",")[0]
		// TODO: Handle more that one seed brokers

		_, tls, err := conf.FieldTLSToggled("kafka", "tls")
		if err != nil {
			tls = false
		}

		kafkaOutletName := "redpanda-connect-kafka-outlet"
		err = n.createKafkaOutlet(kafkaOutletName, bootstrapServer, tls, "self")
		if err != nil {
			return nil, err
		}
	}

	clientOpts, err := kafka.FranzProducerOptsFromConfig(conf.Namespace("kafka"))
	if err != nil {
		return nil, err
	}
	clientOpts = append(clientOpts,
		kgo.SeedBrokers(kafkaInletAddress),
		kgo.AllowAutoTopicCreation(),
	)

	var client *kgo.Client
	kafkaWriter, err := kafka.NewFranzWriterFromConfig(
		conf.Namespace("kafka"),
		kafka.NewFranzWriterHooks(func(_ context.Context, fn kafka.FranzSharedClientUseFn) error {
			if client == nil {
				var err error
				if client, err = kgo.NewClient(clientOpts...); err != nil {
					return err
				}
			}
			return fn(&kafka.FranzSharedClientInfo{
				Client: client,
			})
		}).WithYieldClientFn(func(context.Context) error {
			if client == nil {
				return nil
			}
			client.Close()
			client = nil
			return nil
		}))
	if err != nil {
		return nil, err
	}

	return &ockamKafkaOutput{kafkaWriter, *n}, nil
}

func (o *ockamKafkaOutput) Connect(ctx context.Context) error {
	return o.kafkaWriter.Connect(ctx)
}

func (o *ockamKafkaOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return o.kafkaWriter.WriteBatch(ctx, batch)
}

func (o *ockamKafkaOutput) Close(ctx context.Context) error {
	return errors.Join(o.kafkaWriter.Close(ctx), o.node.delete())
}
