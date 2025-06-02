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

package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	franz_sr "github.com/twmb/franz-go/pkg/sr"
)

func createSchema(t *testing.T, url, subject, schema string, references []franz_sr.SchemaReference) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	_, err = client.CreateSchema(t.Context(), subject, franz_sr.Schema{Schema: schema, References: references})
	require.NoError(t, err)
}

func deleteSubject(t *testing.T, url, subject string, hardDelete bool) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	deleteMode := franz_sr.SoftDelete
	if hardDelete {
		deleteMode = franz_sr.HardDelete
	}

	_, err = client.DeleteSubject(t.Context(), subject, deleteMode)
	require.NoError(t, err)
}

func createKafkaTopic(ctx context.Context, address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	cl, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		return err
	}
	defer cl.Close()

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	topicReq := kmsg.NewCreateTopicsRequestTopic()
	topicReq.NumPartitions = partitions
	topicReq.Topic = topicName
	topicReq.ReplicationFactor = 1
	createTopicsReq.Topics = append(createTopicsReq.Topics, topicReq)

	res, err := createTopicsReq.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	if len(res.Topics) != 1 {
		return fmt.Errorf("expected one topic in response, saw %d", len(res.Topics))
	}
	return kerr.ErrorForCode(res.Topics[0].ErrorCode)
}

func createKafkaTopicSasl(address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.SASL(
			scram.Sha256(func(context.Context) (scram.Auth, error) {
				return scram.Auth{User: "admin", Pass: "foobar"}, nil
			}),
		),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	topicReq := kmsg.NewCreateTopicsRequestTopic()
	topicReq.NumPartitions = partitions
	topicReq.Topic = topicName
	topicReq.ReplicationFactor = 1
	createTopicsReq.Topics = append(createTopicsReq.Topics, topicReq)

	res, err := createTopicsReq.RequestWith(context.Background(), cl)
	if err != nil {
		return err
	}
	if len(res.Topics) != 1 {
		return fmt.Errorf("expected one topic in response, saw %d", len(res.Topics))
	}
	t := res.Topics[0]

	if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
		return fmt.Errorf("topic creation failure: %w", err)
	}
	return nil
}

func TestIntegrationRedpanda(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    commit_period: "1s"
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)

	t.Run("only one partition", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})

	t.Run("explicit partitions", func(t *testing.T) {
		t.Parallel()
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				topicName := "topic-" + vars.ID
				vars.General["VAR1"] = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptSleepAfterInput(time.Second*3),
			integration.StreamTestOptVarSet("VAR4", ""),
		)

		t.Run("range of partitions", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
					require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 4))
				}),
				integration.StreamTestOptPort(kafkaPortStr),
				integration.StreamTestOptSleepAfterInput(time.Second*3),
				integration.StreamTestOptVarSet("VAR1", ":0-3"),
				integration.StreamTestOptVarSet("VAR4", ""),
			)
		})
	})

	manualPartitionTemplate := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    partitioner: manual
    partition: "0"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    commit_period: "1s"
`
	t.Run("manual_partitioner", func(t *testing.T) {
		suite.Run(
			t, manualPartitionTemplate,
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})
}

func TestRedpandaRecordOrder(t *testing.T) {
	// This test checks for out-of-order records being transferred between two Redpanda containers using the `redpanda`
	// input and output with default settings. It used to fail occasionally before this fix was put in place:
	// https://github.com/redpanda-data/connect/pull/3386.
	//
	// Normally, you'll want to let it run multiple times in a loop over night:
	// ```shell
	// $ nohup go test -timeout 0 -v -count 10000 -run ^TestRedpandaRecordOrder$ ./internal/impl/kafka/enterprise > test.log 2>&1 &`
	// ```
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	destination, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	t.Logf("Source broker: %s", source.BrokerAddr)
	t.Logf("Destination broker: %s", destination.BrokerAddr)

	// Create the topic
	dummyTopic := "foobar"
	dummyRetentionTime := strconv.Itoa(int((1 * time.Hour).Milliseconds()))
	createTopicWithACLs(t, source.BrokerAddr, dummyTopic, dummyRetentionTime, "User:redpanda", kmsg.ACLOperationAll)
	createTopicWithACLs(t, destination.BrokerAddr, dummyTopic, dummyRetentionTime, "User:redpanda", kmsg.ACLOperationAll)

	dummyMessage := `{"test":"foo"}`
	go func() {
		t.Log("Producing messages...")

		produceMessages(t, source, dummyTopic, dummyMessage, 0, 50, false, 50*time.Millisecond)

		t.Log("Finished producing messages")
	}()

	runRedpandaPipeline := func(t *testing.T, source, destination redpandatest.RedpandaEndpoints, topic string, suppressLogs bool) {
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: migrator_cg
    start_from_oldest: true

output:
  redpanda:
    seed_brokers: [ %s ]
    topic: ${! @kafka_topic }
    key: ${! @kafka_key }
    timestamp_ms: ${! @kafka_timestamp_ms }
    compression: none
`, source.BrokerAddr, topic, destination.BrokerAddr)))
		if suppressLogs {
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))
		}

		stream, err := streamBuilder.Build()
		require.NoError(t, err)

		// Run stream in the background and shut it down when the test is finished
		closeChan := make(chan struct{})
		go func() {
			//nolint:usetesting // context.Background() could be replaced by t.Context()
			err = stream.Run(context.Background())
			require.NoError(t, err)

			t.Log("Migrator pipeline shut down")

			close(closeChan)
		}()
		t.Cleanup(func() {
			require.NoError(t, stream.StopWithin(1*time.Second))

			<-closeChan
		})
	}

	// Run the Redpanda pipeline
	runRedpandaPipeline(t, source, destination, dummyTopic, true)
	t.Log("Pipeline started")

	// Wait for a few records to be produced...
	time.Sleep(1 * time.Second)

	dummyConsumerGroup := "foobar_cg"
	var prevSrcKeys []int
	require.Eventually(t, func() bool {
		srcKeys := fetchRecordKeys(t, source.BrokerAddr, dummyTopic, dummyConsumerGroup, 10)

		time.Sleep(1 * time.Second)

		destKeys := fetchRecordKeys(t, destination.BrokerAddr, dummyTopic, dummyConsumerGroup, 10)
		if destKeys == nil {
			// Stop the tests if the producer finished and the destination consumer group reached the high water mark
			if srcKeys == nil {
				return true
			}

			// Try again if the destination topic still needs to receive data
			return false
		}

		if srcKeys == nil {
			srcKeys = prevSrcKeys
		}

		assert.True(t, slices.IsSorted(srcKeys))
		assert.True(t, slices.IsSorted(destKeys))

		t.Logf("Source keys: %v", srcKeys)
		t.Logf("Destination keys: %v", destKeys)

		// Cache the previous source key so we can compare the current destination key with it after the producer
		// finished, but Migrator still needs to copy some records over
		prevSrcKeys = srcKeys

		return false
	}, 30*time.Second, 1*time.Nanosecond)
}

func TestIntegrationRedpandaSasl(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			"--set redpanda.enable_sasl=true",
			`--set redpanda.superusers=["admin"]`,
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	adminCreated := false

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if !adminCreated {
			var stdErr bytes.Buffer
			_, aerr := resource.Exec([]string{
				"rpk", "acl", "user", "create", "admin",
				"--password", "foobar",
				"--api-urls", "localhost:9644",
			}, dockertest.ExecOptions{
				StdErr: &stdErr,
			})
			if aerr != nil {
				return aerr
			}
			if stdErr.String() != "" {
				return errors.New(stdErr.String())
			}
			adminCreated = true
		}
		return createKafkaTopicSasl("localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      include_patterns: [ .* ]
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: admin
        password: foobar
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestMetadata(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallel(1000),
		// integration.StreamTestStreamParallelLossy(1000),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopicSasl("localhost:"+kafkaPortStr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)
}

func TestIntegrationRedpandaOutputFixedTimestamp(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(t.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    timestamp_ms: 1000000000000

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: "blobfish"
  processors:
    - mapping: |
        root = if metadata("kafka_timestamp_ms") != 1000000000000 { "error: invalid timestamp" }
`

	suite := integration.StreamTests(
		integration.StreamTestOpenCloseIsolated(),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
	)
}

func BenchmarkIntegrationRedpanda(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(b, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr + "/tcp"}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(b, pool.Retry(func() error {
		return createKafkaTopic(b.Context(), "localhost:"+kafkaPortStr, "testingconnection", 1)
	}))

	// Ordered (new) client
	b.Run("ordered", func(b *testing.B) {
		template := `
output:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topic: topic-$ID
    max_in_flight: 128
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ localhost:$PORT ]
    topics: [ topic-$ID ]
    consumer_group: "$VAR3"
    commit_period: "1s"
`
		suite := integration.StreamBenchs(
			integration.StreamBenchSend(20, 1),
			integration.StreamBenchSend(10, 1),
			integration.StreamBenchSend(1, 1),
			// integration.StreamBenchReadSaturated(),
		)
		suite.Run(
			b, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR3"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, "localhost:"+kafkaPortStr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})
}

func TestSchemaRegistryIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	tests := []struct {
		name                       string
		schema                     string
		includeSoftDeletedSubjects bool
		extraSubject               string
		subjectFilter              string
		schemaWithReference        string
	}{
		{
			name:   "roundtrip",
			schema: `{"name":"foo", "type": "string"}`,
		},
		{
			name:                       "roundtrip with deleted subject",
			schema:                     `{"name":"foo", "type": "string"}`,
			includeSoftDeletedSubjects: true,
		},
		{
			name:          "roundtrip with subject filter",
			schema:        `{"name":"foo", "type": "string"}`,
			extraSubject:  "foobar",
			subjectFilter: `^\w+-\w+-\w+-\w+-\w+$`,
		},
		{
			name:   "roundtrip with schema references",
			schema: `{"name":"foo", "type": "string"}`,
			// A UUID which always gets picked first when querying the `/subjects` endpoint.
			extraSubject:        "ffffffff-ffff-ffff-ffff-ffffffffffff",
			schemaWithReference: `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`,
		},
	}

	source, err := redpandatest.StartRedpanda(t, pool, false, true)
	require.NoError(t, err)
	destination, err := redpandatest.StartRedpanda(t, pool, false, true)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			subject := u4.String()

			defer func() {
				// Clean up the extraSubject first since it may contain schemas with references.
				if test.extraSubject != "" {
					deleteSubject(t, source.SchemaRegistryURL, test.extraSubject, false)
					deleteSubject(t, source.SchemaRegistryURL, test.extraSubject, true)
					if test.subjectFilter == "" {
						deleteSubject(t, destination.SchemaRegistryURL, test.extraSubject, false)
						deleteSubject(t, destination.SchemaRegistryURL, test.extraSubject, true)
					}
				}

				if !test.includeSoftDeletedSubjects {
					deleteSubject(t, source.SchemaRegistryURL, subject, false)
				}
				deleteSubject(t, source.SchemaRegistryURL, subject, true)

				deleteSubject(t, destination.SchemaRegistryURL, subject, false)
				deleteSubject(t, destination.SchemaRegistryURL, subject, true)
			}()

			createSchema(t, source.SchemaRegistryURL, subject, test.schema, nil)

			if test.subjectFilter != "" {
				createSchema(t, source.SchemaRegistryURL, test.extraSubject, test.schema, nil)
			}

			if test.includeSoftDeletedSubjects {
				deleteSubject(t, source.SchemaRegistryURL, subject, false)
			}

			if test.schemaWithReference != "" {
				createSchema(t, source.SchemaRegistryURL, test.extraSubject, test.schemaWithReference, []franz_sr.SchemaReference{{Name: "foo", Subject: subject, Version: 1}})
			}

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
    include_deleted: %t
    subject_filter: %s
    fetch_in_order: %t
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order.
        max_in_flight: 1
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, source.SchemaRegistryURL, test.includeSoftDeletedSubjects, test.subjectFilter, test.schemaWithReference != "", destination.SchemaRegistryURL)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
			defer done()

			err = stream.Run(ctx)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects", destination.SchemaRegistryURL))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			if test.subjectFilter != "" {
				assert.Contains(t, string(body), subject)
				assert.NotContains(t, string(body), test.extraSubject)
			}

			resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", destination.SchemaRegistryURL, subject))
			require.NoError(t, err)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			assert.Equal(t, subject, sd.Subject)
			assert.Equal(t, 1, sd.Version)
			assert.JSONEq(t, test.schema, sd.Schema.Schema)

			if test.schemaWithReference != "" {
				resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", destination.SchemaRegistryURL, test.extraSubject))
				require.NoError(t, err)
				body, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)

				var sd franz_sr.SubjectSchema
				require.NoError(t, json.Unmarshal(body, &sd))
				assert.Equal(t, test.extraSubject, sd.Subject)
				assert.Equal(t, 1, sd.Version)
				assert.JSONEq(t, test.schemaWithReference, sd.Schema.Schema)
			}
		})
	}
}

func TestSchemaRegistryIDTranslationIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := redpandatest.StartRedpanda(t, pool, false, true)
	require.NoError(t, err)
	destination, err := redpandatest.StartRedpanda(t, pool, false, true)
	require.NoError(t, err)

	// Create two schemas under subject `foo`.
	createSchema(t, source.SchemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`, nil)
	createSchema(t, source.SchemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}, {"name":"num", "type": "int", "default": 42}]}`, nil)

	// Create a schema under subject `bar` which references the second schema under `foo`.
	createSchema(t, source.SchemaRegistryURL, "bar", `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`,
		[]franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
	)

	// Create a schema at the destination which will have ID 1 so we can check that the ID translation works
	// correctly.
	createSchema(t, destination.SchemaRegistryURL, "baz", `{"name":"baz", "type": "record", "fields":[{"name":"num", "type": "int"}]}`, nil)

	// Use a Stream with a mapping filter to send only the schema with the reference to the destination in order
	// to force the output to backfill the rest of the schemas.
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
  processors:
    - mapping: |
        if this.id != 3 { root = deleted() }
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order
        max_in_flight: 1
        translate_ids: true
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, source.SchemaRegistryURL, destination.SchemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
	defer done()

	err = stream.Run(ctx)
	require.NoError(t, err)

	// Check that the schemas were backfilled correctly.
	tests := []struct {
		subject            string
		version            int
		expectedID         int
		expectedReferences []franz_sr.SchemaReference
	}{
		{
			subject:    "foo",
			version:    1,
			expectedID: 2,
		},
		{
			subject:    "foo",
			version:    2,
			expectedID: 3,
		},
		{
			subject:            "bar",
			version:            1,
			expectedID:         4,
			expectedReferences: []franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", destination.SchemaRegistryURL, test.subject, test.version))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, test.expectedID, sd.ID)
			assert.Equal(t, test.expectedReferences, sd.References)
		})
	}
}

func createTopicWithACLs(t *testing.T, brokerAddr, topic, retentionTime, principal string, operation kadm.ACLOperation) {
	client, err := kgo.NewClient(kgo.SeedBrokers([]string{brokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	configs := map[string]*string{"retention.ms": &retentionTime}
	_, err = adm.CreateTopic(t.Context(), 1, -1, configs, topic)
	require.NoError(t, err)

	updateTopicACL(t, adm, topic, principal, operation)
}

func updateTopicACL(t *testing.T, client *kadm.Client, topic, principal string, operation kadm.ACLOperation) {
	builder := kadm.NewACLs().Allow(principal).AllowHosts("*").Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral).Operations(operation)
	res, err := client.CreateACLs(t.Context(), builder)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.NoError(t, res[0].Err)
}

func checkTopic(t *testing.T, brokerAddr, topic, retentionTime, principal string, operation kadm.ACLOperation) {
	client, err := kgo.NewClient(kgo.SeedBrokers([]string{brokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	topicConfigs, err := adm.DescribeTopicConfigs(t.Context(), topic)
	require.NoError(t, err)

	rc, err := topicConfigs.On(topic, nil)
	require.NoError(t, err)
	assert.Condition(t, func() bool {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && *c.Value == retentionTime {
				return true
			}
		}
		return false
	})

	builder := kadm.NewACLs().Topics(topic).
		ResourcePatternType(kadm.ACLPatternLiteral).Operations(operation).Allow().Deny().AllowHosts().DenyHosts()

	aclResults, err := adm.DescribeACLs(t.Context(), builder)
	require.NoError(t, err)
	require.Len(t, aclResults, 1)
	require.NoError(t, aclResults[0].Err)
	require.Len(t, aclResults[0].Described, 1)

	for _, acl := range aclResults[0].Described {
		assert.Equal(t, principal, acl.Principal)
		assert.Equal(t, "*", acl.Host)
		assert.Equal(t, topic, acl.Name)
		assert.Equal(t, operation.String(), acl.Operation.String())
	}
}

// produceMessages produces `count` messages to the given `topic` with the given `message` content. The
// `timestampOffset` indicates an offset which gets added to the `counter()` Bloblang function which is used to generate
// the message timestamps sequentially, the first one being `1 + timestampOffset`.
func produceMessages(t *testing.T, rpe redpandatest.RedpandaEndpoints, topic, message string, timestampOffset, count int, encode bool, delay time.Duration) {
	streamBuilder := service.NewStreamBuilder()
	config := ""
	if encode {
		config = fmt.Sprintf(`
pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s
        avro_raw_json: true
`, rpe.SchemaRegistryURL, topic)
	}
	config += fmt.Sprintf(`
output:
  kafka_franz:
    seed_brokers: [ %s ]
    topic: %s
    key: ${! counter() }
    timestamp_ms: ${! counter() + %d}
    max_in_flight: 1
`, rpe.BrokerAddr, topic, timestampOffset)
	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	inFunc, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	go func() {
		err = stream.Run(t.Context())
		require.NoError(t, err)
	}()

	for range count {
		ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
		require.NoError(t, inFunc(ctx, service.NewMessage([]byte(message))))
		done()

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	require.NoError(t, stream.StopWithin(1*time.Second))
}

// readMessagesWithCG reads `count` messages from the given `topic` with the given `consumerGroup`.
// TODO: Since `read_until` can't guarantee that we will read `count` messages, `topic` needs to have exactly `count`
// messages in it. We should add some mechanism to the Kafka inputs to allow us to read a range of offsets if possible
// (or up to a certain offset).
func readMessagesWithCG(t *testing.T, rpe redpandatest.RedpandaEndpoints, topic, consumerGroup, message string, count int, decode bool) {
	streamBuilder := service.NewStreamBuilder()
	config := fmt.Sprintf(`
input:
  kafka_franz:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: %s
    start_from_oldest: true
`, rpe.BrokerAddr, topic, consumerGroup)
	if decode {
		config += fmt.Sprintf(`
  processors:
    - schema_registry_decode:
        url: %s
        avro_raw_json: true
`, rpe.SchemaRegistryURL)
	}
	config += `
output:
  # Need to use drop explicitly with SetYAML(). Otherwise, the output will be inproc
  # (or stdout if we import github.com/redpanda-data/benthos/v4/public/components/io)
  drop: {}
`
	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	recvMsgWG := sync.WaitGroup{}
	recvMsgWG.Add(count)
	err := streamBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		defer recvMsgWG.Done()
		b, err := m.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, message, string(b))

		return nil
	})
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()

	go func() {
		require.NoError(t, stream.Run(ctx))
	}()

	recvMsgWG.Wait()

	require.NoError(t, stream.StopWithin(3*time.Second))
}

func runMigratorBundle(t *testing.T, source, destination redpandatest.RedpandaEndpoints, topic, topicPrefix string, suppressLogs bool, callback func(*service.Message)) {
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_bundle:
    redpanda_migrator:
      seed_brokers: [ %s ]
      topics: [ %s ]
      consumer_group: migrator_cg
      start_from_oldest: true
    schema_registry:
      url: %s
    consumer_group_offsets_poll_interval: 2s
  processors:
    - switch:
        - check: '@input_label == "redpanda_migrator_offsets_input"'
          processors:
            - log:
                level: INFO
                message: Migrating Kafka offset
                fields:
                  kafka_offset_topic:            ${! @kafka_offset_topic }
                  kafka_offset_group:            ${! @kafka_offset_group }
                  kafka_offset_partition:        ${! @kafka_offset_partition }
                  kafka_offset_commit_timestamp: ${! @kafka_offset_commit_timestamp }
                  kafka_offset_metadata:         ${! @kafka_offset_metadata }
                  kafka_is_high_watermark:       ${! @kafka_is_high_watermark }
        - check: '@input_label == "redpanda_migrator_input"'
          processors:
            - branch:
                processors:
                  - schema_registry_decode:
                      url: %s
                      avro_raw_json: true
                  - log:
                      level: INFO
                      message: 'Migrating Kafka message: ${! content() } with key ${! @kafka_key } and timestamp ${! @kafka_timestamp_ms }'
        - check: '@input_label == "schema_registry_input"'
          processors:
            - branch:
                processors:
                  - log:
                      message: 'Migrating Schema Registry schema: ${! content() }'

output:
  redpanda_migrator_bundle:
    redpanda_migrator:
      seed_brokers: [ %s ]
      topic_prefix: "%s"
      replication_factor_override: true
      replication_factor: -1
      # TODO: Remove this compression setting once https://github.com/redpanda-data/redpanda/issues/25769 is fixed
      compression: none
    schema_registry:
      url: %s
`, source.BrokerAddr, topic, source.SchemaRegistryURL, source.SchemaRegistryURL, destination.BrokerAddr, topicPrefix, destination.SchemaRegistryURL)))
	if suppressLogs {
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))
	}

	if callback != nil {
		require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			callback(m)
			return nil
		}))

		// Ensure the callback function is called after the output wrote the message
		streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)
	}

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	// Run stream in the background and shut it down when the test is finished
	closeChan := make(chan struct{})
	go func() {
		//nolint:usetesting // context.Background() could be replaced by t.Context()
		err = stream.Run(context.Background())
		require.NoError(t, err)

		t.Log("Migrator pipeline shut down")

		close(closeChan)
	}()
	t.Cleanup(func() {
		require.NoError(t, stream.StopWithin(1*time.Second))

		<-closeChan
	})
}

func TestRedpandaMigratorIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := redpandatest.StartRedpanda(t, pool, true, true)
	require.NoError(t, err)
	destination, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	t.Logf("Source broker: %s", source.BrokerAddr)
	t.Logf("Destination broker: %s", destination.BrokerAddr)

	dummyTopic := "test"

	// Create a schema associated with the test topic
	createSchema(t, source.SchemaRegistryURL, dummyTopic, fmt.Sprintf(`{"name":"%s", "type": "record", "fields":[{"name":"test", "type": "string"}]}`, dummyTopic), nil)

	// Produce one message
	dummyMessage := `{"test":"foo"}`
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true, 0)
	t.Log("Finished producing first message in source")

	// Run the Redpanda Migrator bundle
	msgChan := make(chan *service.Message)
	checkMigrated := func(label string, validate func(string, map[string]string)) {
	loop:
		for {
			select {
			case m := <-msgChan:
				l, ok := m.MetaGet("input_label")
				require.True(t, ok)
				if l != label {
					goto loop
				}

				b, err := m.AsBytes()
				require.NoError(t, err)

				meta := map[string]string{}
				require.NoError(t, m.MetaWalk(func(k, v string) error {
					meta[k] = v
					return nil
				}))

				validate(string(b), meta)
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for migrator transfer")
			}

			break loop
		}
	}

	destTopicPrefix := "dest."
	runMigratorBundle(t, source, destination, dummyTopic, destTopicPrefix, false, func(m *service.Message) {
		msgChan <- m
	})

	checkMigrated("redpanda_migrator_input", func(msg string, _ map[string]string) {
		assert.Equal(t, "\x00\x00\x00\x00\x01\x06foo", msg)
	})
	t.Log("Migrator started")

	dummyCG := "foobar_cg"
	// Read the message from source using a consumer group
	readMessagesWithCG(t, source, dummyTopic, dummyCG, dummyMessage, 1, true)
	checkMigrated("redpanda_migrator_offsets_input", func(_ string, meta map[string]string) {
		assert.Equal(t, dummyTopic, meta["kafka_offset_topic"])
	})
	t.Logf("Finished reading first message from source with consumer group %q", dummyCG)

	// Produce one more message in the source
	secondDummyMessage := `{"test":"bar"}`
	produceMessages(t, source, dummyTopic, secondDummyMessage, 0, 1, true, 0)
	checkMigrated("redpanda_migrator_input", func(msg string, _ map[string]string) {
		assert.Equal(t, "\x00\x00\x00\x00\x01\x06bar", msg)
	})
	t.Log("Finished producing second message in source")

	// Read the new message from the destination using a consumer group
	readMessagesWithCG(t, destination, destTopicPrefix+dummyTopic, dummyCG, secondDummyMessage, 1, true)
	checkMigrated("redpanda_migrator_offsets_input", func(_ string, meta map[string]string) {
		assert.Equal(t, dummyTopic, meta["kafka_offset_topic"])
	})
	t.Logf("Finished reading second message from destination with consumer group %q", dummyCG)
}

func TestRedpandaMigratorOffsetsIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	tests := []struct {
		name          string
		cgAtEndOffset bool
		extraCGUpdate bool
	}{
		{
			name:          "source consumer group points to the topic end offset",
			cgAtEndOffset: true,
		},
		{
			name:          "source consumer group points to an older offset inside the topic",
			cgAtEndOffset: false,
		},
		{
			name:          "subsequent consumer group updates are processed correctly",
			cgAtEndOffset: true,
			extraCGUpdate: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			pool.MaxWait = time.Minute

			source, err := redpandatest.StartRedpanda(t, pool, true, true)
			require.NoError(t, err)
			destination, err := redpandatest.StartRedpanda(t, pool, true, true)
			require.NoError(t, err)

			t.Logf("Source broker: %s", source.BrokerAddr)
			t.Logf("Destination broker: %s", destination.BrokerAddr)

			dummyTopic := "test"
			dummyMessage := `{"test":"foo"}`
			dummyConsumerGroup := "test_cg"
			messageCount := 5

			// Produce messages in the source cluster.
			// The message timestamps are produced in ascending order, starting from 1 all the way to messageCount.
			produceMessages(t, source, dummyTopic, dummyMessage, 0, messageCount, false, 0)

			// Produce the exact same messages in the destination cluster.
			produceMessages(t, destination, dummyTopic, dummyMessage, 0, messageCount, false, 0)

			// Read the messages from the source cluster using a consumer group.
			readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, 5, false)

			if test.extraCGUpdate || !test.cgAtEndOffset {
				// Make sure both source and destination have extra messages after the current consumer group offset.
				// The next messages need to have more recent timestamps than the existing messages, so we use
				// `messageCount` as an offset for their timestamps.
				produceMessages(t, source, dummyTopic, dummyMessage, messageCount, messageCount, false, 0)
				produceMessages(t, destination, dummyTopic, dummyMessage, messageCount, messageCount, false, 0)
			}

			t.Log("Finished setting up messages in the source and destination clusters")

			// Migrate the consumer group offsets.
			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
    topics: [ %s ]

output:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
`, source.BrokerAddr, dummyTopic, destination.BrokerAddr)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

			migratorUpdateWG := sync.WaitGroup{}
			migratorUpdateWG.Add(1)
			require.NoError(t, streamBuilder.AddConsumerFunc(func(context.Context, *service.Message) error {
				defer migratorUpdateWG.Done()
				return nil
			}))

			// Ensure the callback function is called after the output wrote the message.
			streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			// Run stream in the background.
			migratorCloseChan := make(chan struct{})
			go func() {
				err = stream.Run(t.Context())
				require.NoError(t, err)

				t.Log("redpanda_migrator_offsets pipeline shut down")

				close(migratorCloseChan)
			}()

			defer func() {
				require.NoError(t, stream.StopWithin(3*time.Second))

				<-migratorCloseChan
			}()

			migratorUpdateWG.Wait()

			if test.extraCGUpdate {
				// Trigger another consumer group update to get it to point to the end of the topic.
				migratorUpdateWG.Add(1)
				readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)
				migratorUpdateWG.Wait()
			}

			client, err := kgo.NewClient(kgo.SeedBrokers([]string{destination.BrokerAddr}...))
			require.NoError(t, err)
			defer client.Close()

			adm := kadm.NewClient(client)
			offsets, err := adm.FetchOffsets(t.Context(), dummyConsumerGroup)
			currentCGOffset, ok := offsets.Lookup(dummyTopic, 0)
			require.True(t, ok)

			endOffset := int64(messageCount)
			if test.cgAtEndOffset {
				offsets, err := adm.ListEndOffsets(t.Context(), dummyTopic)
				require.NoError(t, err)
				o, ok := offsets.Lookup(dummyTopic, 0)
				require.True(t, ok)
				endOffset = o.Offset
			}
			assert.Equal(t, endOffset, currentCGOffset.At)
			assert.Equal(t, dummyTopic, currentCGOffset.Topic)
		})
	}
}

func TestRedpandaMigratorTopicConfigAndACLsIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := redpandatest.StartRedpanda(t, pool, true, true)
	require.NoError(t, err)

	destination, err := redpandatest.StartRedpanda(t, pool, true, true)
	require.NoError(t, err)

	t.Logf("Source broker: %s", source.BrokerAddr)
	t.Logf("Destination broker: %s", destination.BrokerAddr)

	dummyTopic := "test"

	runMigrator := func() {
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: migrator_cg

output:
  redpanda_migrator:
    seed_brokers: [ %s ]
    topic: ${! @kafka_topic }
    key: ${! @kafka_key }
    partition: ${! @kafka_partition }
    partitioner: manual
    timestamp_ms: ${! @kafka_timestamp_ms }
    translate_schema_ids: false
    replication_factor_override: true
    replication_factor: -1
`, source.BrokerAddr, dummyTopic, destination.BrokerAddr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		migratorUpdateWG := sync.WaitGroup{}
		migratorUpdateWG.Add(1)
		require.NoError(t, streamBuilder.AddConsumerFunc(func(context.Context, *service.Message) error {
			defer migratorUpdateWG.Done()
			return nil
		}))

		// Ensure the callback function is called after the output wrote the message.
		streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)

		stream, err := streamBuilder.Build()
		require.NoError(t, err)

		// Run stream in the background.
		go func() {
			err = stream.Run(t.Context())
			require.NoError(t, err)

			t.Log("redpanda_migrator_offsets pipeline shut down")
		}()

		migratorUpdateWG.Wait()

		require.NoError(t, stream.StopWithin(3*time.Second))
	}

	// Create a topic with a custom retention time
	dummyRetentionTime := strconv.Itoa(int((48 * time.Hour).Milliseconds()))
	dummyPrincipal := "User:redpanda"
	dummyACLOperation := kmsg.ACLOperationRead
	createTopicWithACLs(t, source.BrokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)

	// Produce one message
	dummyMessage := `{"test":"foo"}`
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true, 0)

	// Run the Redpanda Migrator
	runMigrator()

	// Ensure that the topic and ACL were migrated correctly
	checkTopic(t, destination.BrokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)

	client, err := kgo.NewClient(kgo.SeedBrokers([]string{source.BrokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	// Update ACL in the source topic and ensure that it's reflected in the destination
	dummyACLOperation = kmsg.ACLOperationDescribe
	updateTopicACL(t, adm, dummyTopic, dummyPrincipal, dummyACLOperation)

	// Produce one more message so the consumerFunc will get triggered to indicate that Migrator ran successfully
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true, 0)

	// Run the Redpanda Migrator again
	runMigrator()

	// Ensure that the ACL was updated correctly
	checkTopic(t, destination.BrokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)
}

// fetchRecordKeys calls franz-go directly because we don't have any means to read a range of records using the
// kafka_franz input
func fetchRecordKeys(t *testing.T, brokerAddress, topic, consumerGroup string, count int) []int {
	client, err := kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{brokerAddress}...),
		kgo.ConsumeTopics([]string{topic}...),
		kgo.ConsumerGroup(consumerGroup),
	}...)
	require.NoError(t, err)

	defer func() {
		// We need to manually trigger a commit before closing the client because the default is to autocommit every 5s
		err := client.CommitUncommittedOffsets(t.Context())
		require.NoError(t, err)
		client.Close()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	fetches := client.PollRecords(ctx, count)
	require.False(t, fetches.IsClientClosed())
	err = fetches.Err()
	if err != nil {
		// If the context was cancelled, the producer finished so we won't get any more messages
		if err != context.DeadlineExceeded {
			require.NoError(t, err)
		}
		return nil
	}

	it := fetches.RecordIter()

	if it.Done() {
		return nil
	}

	var keys []int
	for !it.Done() {
		rec := it.Next()
		key, err := strconv.Atoi(string(rec.Key))
		require.NoError(t, err)
		keys = append(keys, key)
	}
	return keys
}

// TestRedpandaMigratorConsumerGroupConsistencyIntegration checks that the consumer group updates are propagated
// correctly when a consumer is switched from source to destination such that the destination consumer doesn't miss any
// messages (even if it receives duplicates).
func TestRedpandaMigratorConsumerGroupConsistencyIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	destination, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	t.Logf("Source broker: %s", source.BrokerAddr)
	t.Logf("Destination broker: %s", destination.BrokerAddr)

	// Create the topic
	dummyTopic := "foobar"
	dummyRetentionTime := strconv.Itoa(int((1 * time.Hour).Milliseconds()))
	createTopicWithACLs(t, source.BrokerAddr, dummyTopic, dummyRetentionTime, "User:redpanda", kmsg.ACLOperationAll)

	// Create a schema associated with the test topic
	createSchema(t, source.SchemaRegistryURL, dummyTopic, fmt.Sprintf(`{"name":"%s", "type": "record", "fields":[{"name":"test", "type": "string"}]}`, dummyTopic), nil)

	dummyMessage := `{"test":"foo"}`
	go func() {
		t.Log("Producing messages...")

		produceMessages(t, source, dummyTopic, dummyMessage, 0, 100, true, 50*time.Millisecond)

		t.Log("Finished producing messages")
	}()

	// Run the Redpanda Migrator bundle
	runMigratorBundle(t, source, destination, dummyTopic, "", true, nil)
	t.Log("Migrator started")

	// Wait for a few records to be produced...
	time.Sleep(1 * time.Second)

	// Fetch the first few record keys from the source to create the consumer group
	dummyConsumerGroup := "foobar_cg"
	initialFetchCount := 5
	keys := fetchRecordKeys(t, source.BrokerAddr, dummyTopic, dummyConsumerGroup, initialFetchCount)
	require.Len(t, keys, 5)
	require.Equal(t, 1, keys[0])
	require.Equal(t, 5, keys[4])

	// Wait for the topic and consumer group to be replicated in the destination first
	require.Eventually(t, func() bool {
		client, err := kgo.NewClient([]kgo.Opt{
			kgo.SeedBrokers([]string{destination.BrokerAddr}...),
		}...)
		require.NoError(t, err)
		defer client.Close()

		adm := kadm.NewClient(client)

		topics, err := adm.ListTopics(t.Context(), []string{dummyTopic}...)
		require.NoError(t, err)
		if !topics.Has(dummyTopic) {
			return false
		}

		groups, err := adm.DescribeGroups(t.Context(), []string{dummyConsumerGroup}...)
		require.NoError(t, err)
		if groups.Error() != nil || !slices.Contains(groups.Names(), dummyConsumerGroup) {
			t.Logf("Consumer group %q doesn't exist yet...", dummyConsumerGroup)
			return false
		}

		groupLag, err := adm.Lag(t.Context(), dummyConsumerGroup)
		require.NoError(t, err)
		require.NoError(t, groupLag.Error())

		var lag kadm.GroupMemberLag
		var ok bool
		groupLag.Each(func(l kadm.DescribedGroupLag) {
			lag, ok = l.Lag.Lookup(dummyTopic, 0)
		})
		if !ok {
			return false
		}

		// Ensure the migrated consumer group points to the offset of the last record from the initial fetch
		require.Equal(t, int64(initialFetchCount), lag.Commit.At)

		return true
	}, 30*time.Second, 1*time.Second)

	var prevSrcKeys []int
	require.Eventually(t, func() bool {
		srcKeys := fetchRecordKeys(t, source.BrokerAddr, dummyTopic, dummyConsumerGroup, 10)

		// Allow some time for the consumer group update to be migrated before flipping the consumer to the destination.
		// In practice, we'll have to figure out what is a safe window of time to wait or design a tool which can diff
		// the source and destination consumer groups after the source consumers are stopped.
		// TODO: Maybe do this reliably by using AddConsumerFunc in the Migrator stream.
		time.Sleep(1 * time.Second)

		destKeys := fetchRecordKeys(t, destination.BrokerAddr, dummyTopic, dummyConsumerGroup, 10)
		if destKeys == nil {
			// Stop the tests if the producer finished and the destination consumer group reached the high water mark
			if srcKeys == nil {
				return true
			}

			// Try again if the destination topic still needs to receive data
			return false
		}

		if srcKeys == nil {
			srcKeys = prevSrcKeys
		}

		// Ensure the destination keys come after the source keys which means the consumer group update was migrated
		assert.LessOrEqual(t, destKeys[0], srcKeys[len(srcKeys)-1]+1)

		t.Logf("Source keys: %v", srcKeys)
		t.Logf("Destination keys: %v", destKeys)

		// Cache the previous source key so we can compare the current destination key with it after the producer
		// finished, but Migrator still needs to copy some records over
		prevSrcKeys = srcKeys

		return false
	}, 30*time.Second, 1*time.Nanosecond)
}
