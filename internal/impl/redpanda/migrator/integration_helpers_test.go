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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
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
	redpandaTestWaitTimeout = 10 * time.Second
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

// CreateClusterACLAllow creates an ALLOW ACL for a principal and operation on the cluster resource.
func (e *EmbeddedRedpandaCluster) CreateClusterACLAllow(principal string, op kmsg.ACLOperation) {
	e.t.Helper()

	ctx, cancel := context.WithTimeout(e.t.Context(), redpandaTestOpTimeout)
	defer cancel()

	b := kadm.NewACLs().
		Clusters().
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
		Key:   value,
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
	return readTopicContentContext(cluster.t.Context(), cluster, numMessages)
}

// readTopicContentContext reads specified number of messages from a topic.
func readTopicContentContext(ctx context.Context, cluster EmbeddedRedpandaCluster, numMessages int) []*kgo.Record {
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
				t.Logf("Waiting for more messages... %d/%d", len(records), numMessages)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return records
}

func consume(cluster EmbeddedRedpandaCluster, topic, group string, numMessages int, opts ...kgo.Opt) []kgo.Record {
	ctx := cluster.t.Context()
	t := cluster.t

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(cluster.BrokerAddr),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
	}
	clientOpts = append(clientOpts, opts...)

	client, err := kgo.NewClient(clientOpts...)
	require.NoError(t, err)
	defer client.Close()

	records := make([]kgo.Record, 0, numMessages)
	for len(records) < numMessages {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoError(t, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, *r)
		})

		if len(records) < numMessages {
			select {
			case <-ctx.Done():
				require.Fail(t, "timed out consuming messages")
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	require.NoError(t, client.CommitUncommittedOffsets(ctx))

	return records
}

// ListTopics lists all topics.
func (e *EmbeddedRedpandaCluster) ListTopics() []string {
	metadata, err := e.Admin.Metadata(e.t.Context())
	require.NoError(e.t, err)

	topics := make([]string, 0, len(metadata.Topics))
	for name := range metadata.Topics {
		if strings.HasPrefix(name, "_") {
			continue
		}
		topics = append(topics, name)
	}

	return topics
}

// DescribeTopic describes a topic with partition details.
func (e *EmbeddedRedpandaCluster) DescribeTopic(topic string) kadm.TopicDetail {
	details, err := e.Admin.ListTopics(e.t.Context(), topic)
	require.NoError(e.t, err)
	require.Contains(e.t, details, topic)
	return details[topic]
}

// ListGroups lists all consumer groups and logs the output.
func (e *EmbeddedRedpandaCluster) ListGroups() []string {
	groups, err := e.Admin.ListGroups(e.t.Context())
	require.NoError(e.t, err)

	groupNames := make([]string, 0, len(groups))
	for _, g := range groups {
		groupNames = append(groupNames, g.Group)
	}
	return groupNames
}

// DescribeGroup describes a consumer group.
func (e *EmbeddedRedpandaCluster) DescribeGroup(group string) kadm.DescribedGroup {
	groups, err := e.Admin.DescribeGroups(e.t.Context(), group)
	require.NoError(e.t, err)
	require.Len(e.t, groups, 1)

	return groups[group]
}

type EmbeddedConfluentCluster struct {
	EmbeddedRedpandaCluster
	ConnectURL string
}

// startConfluent starts a Confluent CP cluster using Docker. Adapted from
// https://github.com/confluentinc/cp-all-in-one/.
func startConfluent(t *testing.T) EmbeddedConfluentCluster {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute
	return startConfluentInPool(t, pool, false)
}

const containerExpireSeconds = 3600

// startConfluent starts a Confluent CP cluster using Docker. Adapted from
// https://github.com/confluentinc/cp-all-in-one/.
func startConfluentInPool(t *testing.T, pool *dockertest.Pool, connect bool) EmbeddedConfluentCluster {
	t.Helper()

	// Get free ports for Kafka and Schema Registry
	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)
	schemaRegistryPort, err := integration.GetFreePort()
	require.NoError(t, err)

	// Start Kafka container (Confluent CP Server)
	kafkaOptions := &dockertest.RunOptions{
		Repository: "confluentinc/cp-server",
		Tag:        "8.0.0",
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
			// Prevent log cleanup during testing
			"KAFKA_LOG_RETENTION_MS=-1",
			"KAFKA_LOG_RETENTION_BYTES=-1",
			"KAFKA_LOG_SEGMENT_BYTES=1073741824",
			"KAFKA_LOG_CLEANUP_POLICY=delete",
			"KAFKA_LOG_CLEANER_ENABLE=false",
		},
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostPort: fmt.Sprintf("%d", kafkaPort)}},
		},
	}

	kafkaResource, err := pool.RunWithOptions(kafkaOptions, autoRemove)
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
		Tag:        "8.0.0",
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

	schemaRegistryResource, err := pool.RunWithOptions(schemaRegistryOptions, autoRemove)
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
	t.Log("Schema Registry container is healthy")

	// Start datagen connect
	var connectURL string
	if connect {
		connectPort, err := integration.GetFreePort()
		require.NoError(t, err)

		connectOptions := &dockertest.RunOptions{
			Repository: "cnfldemos/cp-server-connect-datagen",
			Tag:        "0.6.4-7.6.0",
			Hostname:   "connect",
			Env: []string{
				"CONNECT_BOOTSTRAP_SERVERS=broker:29092",
				"CONNECT_REST_ADVERTISED_HOST_NAME=connect",
				"CONNECT_GROUP_ID=compose-connect-group",
				"CONNECT_CONFIG_STORAGE_TOPIC=docker-connect-configs",
				"CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1",
				"CONNECT_OFFSET_FLUSH_INTERVAL_MS=10000",
				"CONNECT_OFFSET_STORAGE_TOPIC=docker-connect-offsets",
				"CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1",
				"CONNECT_STATUS_STORAGE_TOPIC=docker-connect-status",
				"CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1",
				"CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter",
				"CONNECT_VALUE_CONVERTER=io.confluent.connect.avro.AvroConverter",
				"CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=http://schema-registry:8081",
				"CLASSPATH=/usr/share/java/monitoring-interceptors/monitoring-interceptors-8.0.0.jar",
				"CONNECT_PRODUCER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor",
				"CONNECT_CONSUMER_INTERCEPTOR_CLASSES=io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor",
				"CONNECT_PLUGIN_PATH=/usr/share/java,/usr/share/confluent-hub-components",
			},
			ExposedPorts: []string{"8083/tcp"},
			PortBindings: map[docker.Port][]docker.PortBinding{
				"8083/tcp": {{HostPort: fmt.Sprintf("%d", connectPort)}},
			},
			Links: []string{
				fmt.Sprintf("%s:broker", kafkaResource.Container.Name),
				fmt.Sprintf("%s:schema-registry", schemaRegistryResource.Container.Name),
			},
		}

		connectResource, err := pool.RunWithOptions(connectOptions, autoRemove)
		require.NoError(t, err)
		require.NoError(t, connectResource.Expire(containerExpireSeconds))

		t.Cleanup(func() {
			require.NoError(t, pool.Purge(connectResource))
		})

		connectURL = fmt.Sprintf("http://localhost:%d", connectPort)

		// Wait for Kafka Connect to be healthy
		require.NoError(t, pool.Retry(func() error {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectURL, nil)
			if err != nil {
				return err
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("kafka connect not ready, status: %d", resp.StatusCode)
			}
			return nil
		}))
		t.Log("Kafka Connect container is healthy")
	}

	// Create Kafka client and admin
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerAddr),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	admin := kadm.NewClient(client)

	return EmbeddedConfluentCluster{
		EmbeddedRedpandaCluster: EmbeddedRedpandaCluster{
			RedpandaEndpoints: redpandatest.RedpandaEndpoints{
				BrokerAddr:        brokerAddr,
				SchemaRegistryURL: schemaRegistryURL,
			},
			Client: client,
			Admin:  admin,
			t:      t,
		},
		ConnectURL: connectURL,
	}
}

// createConnector creates a Kafka Connect connector via REST API.
func createConnector(ctx context.Context, connectURL, name string, config map[string]any) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	url := fmt.Sprintf("%s/connectors/%s/config", connectURL, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(configJSON))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create connector failed, status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func autoRemove(hc *docker.HostConfig) {
	hc.AutoRemove = true
}
