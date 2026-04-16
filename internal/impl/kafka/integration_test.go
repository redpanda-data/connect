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
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// createTopicSem serializes topic creation to avoid overwhelming Redpanda's
// admin API with concurrent requests, which can cause INVALID_PARTITIONS errors.
var createTopicSem = make(chan struct{}, 1)

func createKafkaTopic(ctx context.Context, address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	// Retry with a fresh client on each attempt. Redpanda's testcontainers
	// setup can return INVALID_PARTITIONS under concurrent topic creation
	// load; a new connection often resolves it.
	var lastErr error
	for range 10 {
		if err := doCreateKafkaTopic(ctx, address, topicName, partitions); err != nil {
			if errors.Is(err, kerr.InvalidPartitions) {
				lastErr = err
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
				}
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func doCreateKafkaTopic(ctx context.Context, address, topicName string, partitions int32) error {
	select {
	case createTopicSem <- struct{}{}:
		defer func() { <-createTopicSem }()
	case <-ctx.Done():
		return ctx.Err()
	}

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

func TestRedpandaIntegration(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	template := `
output:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
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
		integration.StreamTestSendBatchCount(10),
	)

	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
			vars.General["VAR4"] = "group" + vars.ID
			require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
		integration.StreamTestOptVarSet("VAR1", ""),
	)

	t.Run("only one partition", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})

	t.Run("explicit partitions", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				topicName := "topic-" + vars.ID
				vars.General["VAR1"] = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptSleepAfterInput(time.Second*3),
			integration.StreamTestOptVarSet("VAR4", ""),
		)
	})

	t.Run("range of partitions", func(t *testing.T) {
		suite.Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 4))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptSleepAfterInput(time.Second*3),
			integration.StreamTestOptVarSet("VAR1", ":0-3"),
			integration.StreamTestOptVarSet("VAR4", ""),
		)
	})

	manualPartitionTemplate := `
output:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    timeout: "5s"
    partitioner: manual
    partition: "0"
    metadata:
      include_patterns: [ .* ]
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: "$VAR4"
    commit_period: "1s"
`
	t.Run("manual_partitioner", func(t *testing.T) {
		suite.Run(
			t, manualPartitionTemplate,
			integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
				vars.General["VAR4"] = "group" + vars.ID
				require.NoError(t, createKafkaTopic(t.Context(), brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
			integration.StreamTestOptVarSet("VAR1", ""),
		)
	})
}

func TestRedpandaRecordOrderIntegration(t *testing.T) {
	// This test checks for out-of-order records being transferred between two Redpanda containers using the `redpanda`
	// input and output with default settings. It used to fail occasionally before this fix was put in place:
	// https://github.com/redpanda-data/connect/pull/3386.
	//
	// Normally, you'll want to let it run multiple times in a loop over night:
	// ```shell
	// $ nohup go test -timeout 0 -v -count 10000 -run ^TestRedpandaRecordOrder$ ./internal/impl/kafka/enterprise > test.log 2>&1 &`
	// ```
	integration.CheckSkip(t)

	source, err := redpandatest.StartRedpanda(t, false)
	require.NoError(t, err)

	destination, err := redpandatest.StartRedpanda(t, false)
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

	runRedpandaPipeline := func(t *testing.T, source, destination redpandatest.Endpoints, topic string, suppressLogs bool) {
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

		// Run stream in the background and shut it down when the test is finished.
		// Use a dedicated context so we control when the stream stops, rather than
		// relying on t.Context() which is canceled when the test function returns
		// (before cleanup runs).
		ctx, cancel := context.WithCancel(t.Context())
		closeChan := make(chan struct{})
		go func() {
			if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}

			t.Log("Migrator pipeline shut down")

			close(closeChan)
		}()
		t.Cleanup(func() {
			cancel()
			require.NoError(t, stream.StopWithin(5*time.Second))

			select {
			case <-closeChan:
			case <-time.After(10 * time.Second):
				t.Log("cleanup: timed out waiting for pipeline goroutine to exit")
			}
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

func TestRedpandaSaslIntegration(t *testing.T) {
	integration.CheckSkip(t)

	brokerAddr, kafkaPortStr := startRedpanda(t)

	require.Eventually(t, func() bool {
		return createKafkaTopic(t.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	template := `
output:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topic: topic-$ID
    timestamp_ms: 1000000000000

input:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
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
			require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 1))
		}),
		integration.StreamTestOptPort(kafkaPortStr),
	)
}

func BenchmarkRedpandaIntegration(b *testing.B) {
	integration.CheckSkip(b)

	brokerAddr, kafkaPortStr := startRedpanda(b)

	require.Eventually(b, func() bool {
		return createKafkaTopic(b.Context(), brokerAddr, "testingconnection", 1) == nil
	}, time.Minute, time.Second)

	// Ordered (new) client
	b.Run("ordered", func(b *testing.B) {
		template := `
output:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
    topic: topic-$ID
    max_in_flight: 128
    timeout: "5s"
    metadata:
      include_patterns: [ .* ]

input:
  redpanda:
    seed_brokers: [ 127.0.0.1:$PORT ]
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
				require.NoError(t, createKafkaTopic(ctx, brokerAddr, vars.ID, 1))
			}),
			integration.StreamTestOptPort(kafkaPortStr),
		)
	})
}

// fetchRecordKeys calls franz-go directly because we don't have any means to
// read a range of records using the kafka_franz input.
func fetchRecordKeys(t *testing.T, brokerAddress, topic, consumerGroup string, count int) []int {
	client, err := kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{brokerAddress}...),
		kgo.ConsumeTopics([]string{topic}...),
		kgo.ConsumerGroup(consumerGroup),
	}...)
	require.NoError(t, err)

	defer func() {
		// We need to manually trigger a commit before closing the client because the default is to autocommit every 5s
		require.NoError(t, client.CommitUncommittedOffsets(t.Context()))
		client.Close()
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()
	fetches := client.PollRecords(ctx, count)
	require.False(t, fetches.IsClientClosed())

	err = fetches.Err()
	// If the context was cancelled, the producer finished so we won't get
	// any more messages.
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	require.NoError(t, err)

	it := fetches.RecordIter()

	var keys []int
	for !it.Done() {
		rec := it.Next()
		key, err := strconv.Atoi(string(rec.Key))
		require.NoError(t, err)
		keys = append(keys, key)
	}
	return keys
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

// produceMessages produces `count` messages to the given `topic` with the given `message` content. The
// `timestampOffset` indicates an offset which gets added to the `counter()` Bloblang function which is used to generate
// the message timestamps sequentially, the first one being `1 + timestampOffset`.
func produceMessages(t *testing.T, rpe redpandatest.Endpoints, topic, message string, timestampOffset, count int, encode bool, delay time.Duration) {
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
		if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
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
