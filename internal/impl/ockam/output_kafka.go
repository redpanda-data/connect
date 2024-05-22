package ockam

import (
	"context"
	"errors"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
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
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
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
	return kafka.FranzKafkaOutputConfig().
		Summary("Ockam").
		Field(service.NewStringListField("seed_brokers").Optional().
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"})).
		Field(service.NewStringField("ockam_enrollment_ticket").Optional()).
		Field(service.NewStringField("ockam_identity_name").Optional()).
		Field(service.NewStringField("ockam_allow_consumer").Default("self").Optional()).
		Field(service.NewStringField("ockam_route_to_consumer").Default("/ip4/127.0.0.1/tcp/6262")).
		Field(service.NewStringField("ockam_allow").Default("self").Optional()).
		Field(service.NewStringField("ockam_route_to_kafka_outlet").Default("self").Optional())
}

//------------------------------------------------------------------------------

type ockamKafkaOutput struct {
	kafkaWriter *kafka.FranzKafkaWriter
	node        node
}

func newOckamKafkaOutput(conf *service.ParsedConfig, log *service.Logger) (*ockamKafkaOutput, error) {
	_, err := setupCommand()
	if err != nil {
		return nil, err
	}

	// --- Create Ockam Node ----

	var ticket string
	if conf.Contains("ockam_enrollment_ticket") {
		ticket, err = conf.FieldString("ockam_enrollment_ticket")
		if err != nil {
			return nil, err
		}
	}

	var identityName string
	if conf.Contains("ockam_identity_name") {
		identityName, err = conf.FieldString("ockam_identity_name")
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

	routeToConsumer, err := conf.FieldString("ockam_route_to_consumer")
	if err != nil {
		return nil, err
	}

	allowConsumer, err := conf.FieldString("ockam_allow_consumer")
	if err != nil {
		return nil, err
	}

	kafkaInletAddress, err := findAvailableLocalTCPAddress()
	if err != nil {
		return nil, err
	}

	var routeToKafkaOutlet string
	routeToKafkaOutlet, err = conf.FieldString("ockam_route_to_kafka_outlet")
	if err != nil {
		return nil, err
	}

	var allowOutlet string
	allowOutlet, err = conf.FieldString("ockam_allow")
	if err != nil {
		return nil, err
	}

	err = n.createKafkaInlet("redpanda-connect-kafka-inlet", kafkaInletAddress, routeToKafkaOutlet, true, routeToConsumer, allowOutlet, "", allowConsumer)
	if err != nil {
		return nil, err
	}

	// ---- Create Ockam Kafka Outlet ----

	kafkaWriter, err := kafka.NewFranzKafkaWriterFromConfig(conf, log)
	if err != nil {
		return nil, err
	}

	if routeToKafkaOutlet == "self" {
		// Use the first "seed_brokers" field item as the bootstrapServer argument for Ockam.
		seedBrokers, err := conf.FieldStringList("seed_brokers")
		if err != nil {
			return nil, err
		}
		if len(seedBrokers) > 1 {
			log.Warn("ockam_kafka output only supports one seed broker")
		}
		bootstrapServer := strings.Split(seedBrokers[0], ",")[0]
		// TODO: Handle more that one seed brokers

		_, tls, err := conf.FieldTLSToggled("tls")
		if err != nil {
			tls = false
		}

		kafkaOutletName := "redpanda-connect-kafka-outlet"
		err = n.createKafkaOutlet(kafkaOutletName, bootstrapServer, tls, "self")
		if err != nil {
			return nil, err
		}
	}

	// Override the list of SeedBrokers that would be used by kafka_franz, set it to the address of the kafka inlet
	kafkaWriter.SeedBrokers = []string{kafkaInletAddress}
	// Disable TLS, kafka_franz writer will communicate in plaintext with the ockam kafka inlet
	kafkaWriter.TLSConf = nil

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
