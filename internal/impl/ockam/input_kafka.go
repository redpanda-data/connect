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

// this function is, almost, an exact copy of the init() function in ../kafka/input_kafka_franz.go
func init() {
	service.MustRegisterBatchInput("ockam_kafka", ockamKafkaInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newOckamKafkaInput(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf.Namespace("kafka"), i)
		})

}

func ockamKafkaInputConfig() *service.ConfigSpec {
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
			},
			kafka.FranzConsumerFields(),
			kafka.FranzReaderUnorderedConfigFields(),
		)...).LintRule(kafka.FranzConsumerFieldLintRules)).
		Field(service.NewBoolField("disable_content_encryption").Default(false)).
		Field(service.NewStringField("enrollment_ticket").Optional()).
		Field(service.NewStringField("identity_name").Optional()).
		Field(service.NewStringField("allow").Default("self")).
		Field(service.NewStringField("route_to_kafka_outlet").Default("self")).
		Field(service.NewStringField("allow_producer").Default("self")).
		Field(service.NewStringField("relay").Optional()).
		Field(service.NewStringField("node_address").Default("127.0.0.1:6262")).
		Field(service.NewStringListField("encrypted_fields").
			Description("The fields to encrypt in the kafka messages, assuming the record is a valid JSON map. By default, the whole record is encrypted.").
			Default([]string{}))
}

//------------------------------------------------------------------------------

type ockamKafkaInput struct {
	node        node
	kafkaReader *kafka.FranzReaderUnordered
}

func newOckamKafkaInput(conf *service.ParsedConfig, mgr *service.Resources) (*ockamKafkaInput, error) {
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

	var relay string
	if conf.Contains("relay") {
		relay, err = conf.FieldString("relay")
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

	address, err := conf.FieldString("node_address")
	if err != nil {
		return nil, err
	}
	if localTCPAddressIsTaken(address) {
		return nil, errors.New("node_address '" + address + "' is already in use")
	}

	n, err := newNode(identityName, address, ticket, relay)
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Kafka Inlet ----

	allowProducer, err := conf.FieldString("allow_producer")
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

	err = n.createKafkaInlet("redpanda-connect-kafka-inlet", kafkaInletAddress, routeToKafkaOutlet, true, "self", allowOutlet, allowProducer, "", disableContentEncryption, encryptedFields)
	if err != nil {
		return nil, err
	}

	if routeToKafkaOutlet == "self" {
		// TODO: Handle other tls fields in kafka franz
		_, tls, err := conf.FieldTLSToggled("kafka", "tls")
		if err != nil {
			tls = false
		}
		// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
		seedBrokers, err := conf.FieldStringList("kafka", "seed_brokers")
		if err != nil {
			return nil, err
		}
		if len(seedBrokers) != 1 {
			mgr.Logger().Warn("ockam_kafka input only supports one seed broker")
		}
		bootstrapServer := strings.Split(seedBrokers[0], ",")[0]
		// TODO: Handle more that one seed brokers

		kafkaOutletName := "redpanda-connect-kafka-outlet"
		err = n.createKafkaOutlet(kafkaOutletName, bootstrapServer, tls, "self")
		if err != nil {
			return nil, err
		}
	}

	// ---- Create Ockam Kafka Outlet if necessary ----
	clientOpts, err := kafka.FranzConsumerOptsFromConfig(conf.Namespace("kafka"))
	if err != nil {
		return nil, err
	}
	clientOpts = append(clientOpts,
		kgo.SeedBrokers(kafkaInletAddress),
	)

	kafkaReader, err := kafka.NewFranzReaderUnorderedFromConfig(conf.Namespace("kafka"), mgr, clientOpts...)
	if err != nil {
		return nil, err
	}

	return &ockamKafkaInput{*n, kafkaReader}, nil
}

func (o *ockamKafkaInput) Connect(ctx context.Context) error {
	return o.kafkaReader.Connect(ctx)
}

func (o *ockamKafkaInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	return o.kafkaReader.ReadBatch(ctx)
}

func (o *ockamKafkaInput) Close(ctx context.Context) error {
	return errors.Join(o.kafkaReader.Close(ctx), o.node.delete())
}
