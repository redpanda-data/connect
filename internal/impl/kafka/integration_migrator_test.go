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
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
)

func runMigratorBundle(t *testing.T, source, destination redpandatest.RedpandaEndpoints, topic, topicPrefix string, suppressLogs bool, callback func(*service.Message)) {
	const migratorBundleTmpl = `
input:
  redpanda_migrator_bundle:
    redpanda_migrator:
      seed_brokers: [ {{.Source.BrokerAddr}} ]
      topics: [ {{.Topic}} ]
      consumer_group: migrator_cg
      start_from_oldest: true
    schema_registry:
      url: {{.Source.SchemaRegistryURL}}
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
                      url: {{.Source.SchemaRegistryURL}}
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
      seed_brokers: [ {{.Destination.BrokerAddr}} ]
      topic_prefix: "{{.TopicPrefix}}"
      replication_factor_override: true
      replication_factor: -1
      # TODO: Remove this compression setting once https://github.com/redpanda-data/redpanda/issues/25769 is fixed
      compression: none
    schema_registry:
      url: {{.Destination.SchemaRegistryURL}}
`
	tmpl, err := template.New("migrator-bundle").Parse(migratorBundleTmpl)
	require.NoError(t, err)
	data := struct {
		Source      redpandatest.RedpandaEndpoints
		Destination redpandatest.RedpandaEndpoints
		Topic       string
		TopicPrefix string
	}{
		Source:      source,
		Destination: destination,
		Topic:       topic,
		TopicPrefix: topicPrefix,
	}
	var yamlBuf bytes.Buffer
	require.NoError(t, tmpl.Execute(&yamlBuf, data))

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(yamlBuf.String()))
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
			case <-time.After(20 * time.Second):
				require.FailNow(t, "timed out waiting for migrator transfer")
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
			readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)

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
    poll_interval: 2s

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
			require.NoError(t, err)
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

func TestRedpandaMigratorOffsetsSkipRewindsIntegration(t *testing.T) {
	integration.CheckSkip(t)

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
	readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)

	t.Log("Finished setting up messages in the source and destination clusters")

	runMigrator := func() string {
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
    topics: [ %s ]
    poll_interval: 2s

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

		var migratorLogs bytes.Buffer
		streamBuilder.SetLogger(slog.New(slog.NewTextHandler(&migratorLogs, &slog.HandlerOptions{})))

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

		migratorUpdateWG.Wait()

		// Shutdown the Migrator stream.
		require.NoError(t, stream.StopWithin(3*time.Second))
		<-migratorCloseChan

		t.Log("Finished running Migrator stream")

		return migratorLogs.String()
	}

	expectedWarning := `Skipping consumer offset update for topic \"test\" and partition 0 (timestamp 5) because the destination consumer group \"test_cg\" already has an offset of 10 which is ahead of the one we're trying to write: 4`

	// Run the Migrator stream to migrate the consumer group offsets.
	logs := runMigrator()
	assert.NotContains(t, logs, expectedWarning)

	// Produce some more messages in the destination cluster.
	produceMessages(t, destination, dummyTopic, dummyMessage, messageCount, messageCount, false, 0)

	// Read the new messages from the destination cluster using a consumer group.
	readMessagesWithCG(t, destination, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)

	t.Log("Finished writing extra messages and updating the consumer group in the destination cluster")

	// Run the Migrator stream again so it attempts to overwrite the consumer group offset.
	logs = runMigrator()
	t.Log(logs)
	assert.Contains(t, logs, expectedWarning)

	client, err := kgo.NewClient(kgo.SeedBrokers([]string{destination.BrokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)
	offsets, err := adm.FetchOffsets(t.Context(), dummyConsumerGroup)
	require.NoError(t, err)
	currentCGOffset, ok := offsets.Lookup(dummyTopic, 0)
	require.True(t, ok)

	endOffset := int64(messageCount * 2)
	assert.Equal(t, endOffset, currentCGOffset.At)
	assert.Equal(t, dummyTopic, currentCGOffset.Topic)
}

func TestRedpandaMigratorOffsetsNonMonotonicallyIncreasingTimestampsIntegration(t *testing.T) {
	integration.CheckSkip(t)

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

	// Produce a batch of messages in the source cluster.
	produceMessages(t, source, dummyTopic, dummyMessage, 0, messageCount, false, 0)

	// Produce 3 batches of messages in the destination cluster with out of order and overlapping timestamps.
	// offsets: 0, 1, 2, 3, 4, 5
	// timestamps: 3, 4, 5, 6, 7
	produceMessages(t, destination, dummyTopic, dummyMessage, 2, messageCount, false, 0)
	// offsets: 6, 7, 8, 9, 10
	// timestamps: 4, 5, 6, 7, 8
	produceMessages(t, destination, dummyTopic, dummyMessage, 3, messageCount, false, 0)
	// offsets: 11, 12, 13, 14, 15
	// timestamps: 1, 2, 3, 4, 5
	produceMessages(t, destination, dummyTopic, dummyMessage, 0, messageCount, false, 0)

	// Read messageCount messages from the source cluster using a consumer group. The consumer group will point to the
	// last message in the batch, which has timestamp 5.
	readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)

	t.Log("Finished setting up messages in the source and destination clusters")

	// Migrate the consumer group offsets.
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
    topics: [ %s ]
    poll_interval: 2s

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

	client, err := kgo.NewClient(kgo.SeedBrokers([]string{destination.BrokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)
	offsets, err := adm.FetchOffsets(t.Context(), dummyConsumerGroup)
	currentCGOffset, ok := offsets.Lookup(dummyTopic, 0)
	require.True(t, ok)

	// Even though we have 3 batches of messages in the destination cluster, the consumer group should point to the
	// offset of the first message which has timestamp 5.
	assert.Equal(t, int64(2), currentCGOffset.At)
}

func TestRedpandaMigratorTopicConfigAndACLsIntegration(t *testing.T) {
	integration.CheckSkip(t)

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
