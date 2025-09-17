// Copyright 2025 Redpanda Data, Inc.
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

package migrator_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

const migratorTestTopic = "test_topic"

// EmbeddedRedpandaCluster represents a Redpanda cluster with client and admin access.
type EmbeddedRedpandaCluster struct {
	redpandatest.RedpandaEndpoints
	Client *kgo.Client
	Admin  *kadm.Client
	t      *testing.T
}

// startRedpandaSourceAndDestination starts two containers for Redpanda and
// returns the EmbeddedRedpandaCluster for each container.
func startRedpandaSourceAndDestination(t *testing.T) (src, dst EmbeddedRedpandaCluster) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	src = EmbeddedRedpandaCluster{t: t}
	dst = EmbeddedRedpandaCluster{t: t}

	src.RedpandaEndpoints, err = redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	dst.RedpandaEndpoints, err = redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	src.Client, err = kgo.NewClient(
		kgo.SeedBrokers(src.BrokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(func() { src.Client.Close() })

	dst.Client, err = kgo.NewClient(
		kgo.SeedBrokers(dst.BrokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ConsumeTopics(migratorTestTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { dst.Client.Close() })

	src.Admin = kadm.NewClient(src.Client)
	dst.Admin = kadm.NewClient(dst.Client)

	src.CreateTopic(migratorTestTopic)

	return src, dst
}

const (
	redpandaTestOpTimeout   = time.Second
	redpandaTestWaitTimeout = 5 * time.Second
)

// CreateTopic creates a topic if it doesn't exist
func (e *EmbeddedRedpandaCluster) CreateTopic(topic string) {
	e.t.Helper()
	e.CreateTopicWithConfigs(topic, nil)
}

func (e *EmbeddedRedpandaCluster) CreateTopicWithConfigs(topic string, configs map[string]*string) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	_, err := e.Admin.CreateTopic(ctx, 2, 1, configs, topic)
	if err != nil {
		e.t.Errorf("Failed to create topic %s: %v", topic, err)
	}
}

// CreateACLAllow creates an ALLOW ACL for a principal and operation on a topic.
func (e *EmbeddedRedpandaCluster) CreateACLAllow(topic, principal string, op kmsg.ACLOperation) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	b := kadm.NewACLs().
		Topics(topic).
		ResourcePatternType(kadm.ACLPatternLiteral).
		Operations(op).
		Allow(principal)
	_, err := e.Admin.CreateACLs(ctx, b)
	require.NoError(e.t, err)
}

// DescribeTopicACLs returns ACLs for a topic.
func (e *EmbeddedRedpandaCluster) DescribeTopicACLs(topic string) ([]kadm.DescribedACL, error) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	return migrator.DescribeACLs(ctx, e.Admin, topic)
}

// TopicConfig returns the value of the configuration entry with key `key` for
// topic `topic`, or nil if the key is not found.
func (e *EmbeddedRedpandaCluster) TopicConfig(topic, key string) *string {
	e.t.Helper()
	_, rc, err := migrator.TopicDetailsWithClient(e.t.Context(), e.Admin, topic)
	if err != nil {
		e.t.Errorf("Failed to get topic configs for topic %s: %v", topic, err)
	}
	for _, cfg := range rc.Configs {
		if cfg.Key == key {
			return cfg.Value
		}
	}
	return nil
}

// Produce sends a message with the given value to the specified topic
func (e *EmbeddedRedpandaCluster) Produce(topic string, value []byte, opts ...func(*kgo.Record)) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	record := &kgo.Record{
		Topic: topic,
		Value: value,
	}
	for _, opt := range opts {
		opt(record)
	}
	require.NoError(e.t, e.Client.ProduceSync(ctx, record).FirstErr())
}

func ProduceToTopicOpt(topic string) func(*kgo.Record) {
	return func(r *kgo.Record) {
		r.Topic = topic
	}
}

func ProduceToPartitionOpt(partition int) func(*kgo.Record) {
	return func(r *kgo.Record) {
		r.Partition = int32(partition)
	}
}

func ProduceWithSchemaIDOpt(schemaID int) func(*kgo.Record) {
	return func(r *kgo.Record) {
		hdr := make([]byte, 5)
		hdr[0] = 0
		binary.BigEndian.PutUint32(hdr[1:], uint32(schemaID))
		r.Value = append(hdr, r.Value...)
	}
}

func (e *EmbeddedRedpandaCluster) CommitOffset(group, topic string, part, at int) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	var offs kadm.Offsets
	offs.Add(kadm.Offset{
		Topic:     topic,
		Partition: int32(part),
		At:        int64(at),
	})
	_, err := e.Admin.CommitOffsets(ctx, group, offs)
	require.NoError(e.t, err)
}

// writeToTopic produces num messages to a topic.
func writeToTopic(cluster EmbeddedRedpandaCluster, numMessages int, opts ...func(*kgo.Record)) {
	for i := range numMessages {
		cluster.Produce(migratorTestTopic, []byte(strconv.Itoa(i)), opts...)
	}
	cluster.t.Logf("Successfully wrote %d messages to topic %s", numMessages, migratorTestTopic)
}

// readTopicContent reads specified number of messages from a topic.
func readTopicContent(cluster EmbeddedRedpandaCluster, numMessages int) []*kgo.Record {
	ctx := cluster.t.Context()
	t := cluster.t
	client := cluster.Client

	records := make([]*kgo.Record, 0, numMessages)
	for len(records) < numMessages {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoError(t, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})

		select {
		case <-ctx.Done():
			require.Fail(t, "Timed out waiting for messages")
			return nil
		default:
			if len(records) < numMessages {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return records
}

type EmbeddedConfluentCluster EmbeddedRedpandaCluster

// startConfluent starts a Confluent CP cluster using Docker. Adapted from
// https://github.com/confluentinc/cp-all-in-one/.
func startConfluent(t *testing.T) EmbeddedConfluentCluster {
	t.Helper()

	const containerExpireSeconds = 900

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	// Get free ports for Kafka and Schema Registry
	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)
	schemaRegistryPort, err := integration.GetFreePort()
	require.NoError(t, err)

	// Start Kafka container (Confluent CP Server)
	kafkaOptions := &dockertest.RunOptions{
		Repository: "confluentinc/cp-server",
		Tag:        "latest",
		Hostname:   "broker",
		Env: []string{
			"KAFKA_NODE_ID=1",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
			fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:%d", kafkaPort),
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
			"KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_CONFLUENT_BALANCER_TOPIC_REPLICATION_FACTOR=1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
			"KAFKA_DEFAULT_REPLICATION_FACTOR=1",
			"KAFKA_MIN_INSYNC_REPLICAS=1",
			"KAFKA_PROCESS_ROLES=broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS=1@broker:29093",
			"KAFKA_LISTENERS=PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092",
			"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
			"KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
			"KAFKA_LOG_DIRS=/tmp/kraft-combined-logs",
			"CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
			"CONFLUENT_METRICS_ENABLE=false",
			"CONFLUENT_SUPPORT_CUSTOMER_ID=anonymous",
		},
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostPort: fmt.Sprintf("%d", kafkaPort)}},
		},
	}

	kafkaResource, err := pool.RunWithOptions(kafkaOptions)
	require.NoError(t, err)
	require.NoError(t, kafkaResource.Expire(containerExpireSeconds))

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(kafkaResource))
	})

	// Wait for Kafka to be healthy
	brokerAddr := fmt.Sprintf("localhost:%d", kafkaPort)
	require.NoError(t, pool.Retry(func() error {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(brokerAddr),
			kgo.ClientID("health-check"),
		)
		if err != nil {
			return err
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		return client.Ping(ctx)
	}))
	t.Log("Kafka container is healthy")

	// Start Schema Registry container (Confluent CP Schema Registry)
	schemaRegistryOptions := &dockertest.RunOptions{
		Repository: "confluentinc/cp-schema-registry",
		Tag:        "latest",
		Hostname:   "schema-registry",
		Env: []string{
			"SCHEMA_REGISTRY_HOST_NAME=schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=broker:29092",
			"SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
		},
		ExposedPorts: []string{"8081/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8081/tcp": {{HostPort: fmt.Sprintf("%d", schemaRegistryPort)}},
		},
		Links: []string{fmt.Sprintf("%s:broker", kafkaResource.Container.Name)},
	}

	schemaRegistryResource, err := pool.RunWithOptions(schemaRegistryOptions)
	require.NoError(t, err)
	require.NoError(t, schemaRegistryResource.Expire(containerExpireSeconds))

	t.Cleanup(func() {
		require.NoError(t, pool.Purge(schemaRegistryResource))
	})

	schemaRegistryURL := fmt.Sprintf("http://localhost:%d", schemaRegistryPort)

	// Wait for Schema Registry to be healthy
	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, schemaRegistryURL+"/subjects", nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("schema registry not ready, status: %d", resp.StatusCode)
		}
		return nil
	}))
	t.Log("Kafka container is healthy")

	// Create Kafka client and admin
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	admin := kadm.NewClient(client)

	return EmbeddedConfluentCluster{
		RedpandaEndpoints: redpandatest.RedpandaEndpoints{
			BrokerAddr:        brokerAddr,
			SchemaRegistryURL: schemaRegistryURL,
		},
		Client: client,
		Admin:  admin,
	}
}
