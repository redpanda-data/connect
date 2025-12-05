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
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/twmb/franz-go/pkg/sr"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
)

const httpAddr = "127.0.0.1:8080"

func startMigrator(t *testing.T, src, dst EmbeddedRedpandaCluster, cb service.MessageHandlerFunc) {
	t.Helper()

	const yamlTmpl = `
http:
  enabled: true
  address: {{.HTTPAddr}}

input:
  redpanda_migrator:
    seed_brokers: 
      - {{.Src.BrokerAddr}}
    topics: 
      - {{.Topic}}
    consumer_group: redpanda_migrator_cg
    fetch_max_bytes: 512B
    {{- if .Src.SchemaRegistryURL }}
    schema_registry:
      url: {{.Src.SchemaRegistryURL}}
    {{- end }}
output:
  redpanda_migrator:
    seed_brokers: [ {{.Dst.BrokerAddr}} ]
    {{- if .Dst.SchemaRegistryURL }}
    schema_registry:
      url: {{.Dst.SchemaRegistryURL}}
    {{- end }}
    consumer_groups:
      interval: 1s
metrics:
  json_api: {}
logger:
  level: DEBUG
`
	tmpl, err := template.New("migrator").Parse(yamlTmpl)
	require.NoError(t, err)

	data := struct {
		Src      EmbeddedRedpandaCluster
		Dst      EmbeddedRedpandaCluster
		Topic    string
		HTTPAddr string
	}{
		Src:      src,
		Dst:      dst,
		Topic:    migratorTestTopic,
		HTTPAddr: httpAddr,
	}
	var yamlBuf bytes.Buffer
	require.NoError(t, tmpl.Execute(&yamlBuf, data))

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(yamlBuf.String()))
	if cb != nil {
		require.NoError(t, sb.AddConsumerFunc(cb))
	}

	stream, err := sb.Build()
	require.NoError(t, err)

	// Run stream in the background and shut it down when the test is finished
	go func() {
		if err := stream.Run(t.Context()); err != nil {
			if !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		}
		t.Log("Migrator pipeline shutdown")
	}()
	t.Cleanup(func() {
		require.NoError(t, stream.StopWithin(stopStreamTimeout))
	})
}

func readMetrics(t *testing.T, baseURL string) map[string]any {
	t.Helper()

	resp, err := http.Get(baseURL + "/stats")
	if err != nil {
		t.Logf("Failed to fetch metrics: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Logf("Metrics endpoint returned status %d", resp.StatusCode)
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Logf("Failed to read metrics response: %v", err)
		return nil
	}

	var metrics map[string]any
	if err := json.Unmarshal(body, &metrics); err != nil {
		t.Logf("Failed to parse metrics JSON: %v", err)
		return nil
	}

	return metrics
}

func startMigratorAndWaitForMessages(t *testing.T, src, dst EmbeddedRedpandaCluster, numMessages int) {
	done := make(chan struct{})
	startMigrator(t, src, dst, func(_ context.Context, _ *service.Message) error {
		done <- struct{}{}
		return nil
	})
	for range numMessages {
		select {
		case <-done:
			continue
		case <-time.After(redpandaTestOpTimeout):
			t.Fatal("Timed out waiting for messages")
		}
	}
}

func TestIntegrationMigratorSinglePartition(t *testing.T) {
	integration.CheckSkip(t)

	const numMessages = 100

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)
	src.SchemaRegistryURL = ""
	dst.SchemaRegistryURL = ""

	t.Log("When: Messages are written to partition 0 of the source cluster")
	writeToTopic(src, numMessages)

	t.Log("And: Migrator is started")
	startMigratorAndWaitForMessages(t, src, dst, numMessages)

	t.Logf("Then: %d messages are present in destination topic %s", numMessages, migratorTestTopic)
	records := readTopicContent(dst, numMessages)
	require.Len(t, records, numMessages)

	t.Log("And: Messages are in correct order in partition 0")
	for i, record := range records {
		assert.Equal(t, int32(0), record.Partition, "Message %d should be in partition 0", i)
		assert.Equal(t, []byte(strconv.Itoa(i)), record.Key, "Message %d should have correct key", i)
		assert.Equal(t, []byte(strconv.Itoa(i)), record.Value, "Message %d should have correct value", i)
	}
}

func TestIntegrationMigratorSinglePartitionMalformedSchemaID(t *testing.T) {
	integration.CheckSkip(t)

	const (
		numMessages = 100
		subj        = "foo"
		schema      = `{"type":"int"}`
	)

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: Schema registry containing a subject and schema")
	{
		srScr, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
		require.NoError(t, err)
		_, err = srScr.CreateSchema(t.Context(), subj, sr.Schema{Schema: schema})
		require.NoError(t, err)
	}

	t.Log("And: Destination schema registry subject is set to import mode")
	{
		srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
		require.NoError(t, err)
		modeRes := srDst.SetMode(t.Context(), sr.ModeImport, subj)
		require.NoError(t, modeRes[0].Err)
	}

	pfx := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	t.Log("When: Messages with malformed schema ID headers are written to source cluster")
	for i := range numMessages {
		src.Produce(migratorTestTopic, append(pfx, []byte(strconv.Itoa(i))...))
	}
	t.Logf("Successfully wrote %d messages with malformed headers to topic %s", numMessages, migratorTestTopic)

	t.Log("And: Migrator is started")
	startMigratorAndWaitForMessages(t, src, dst, numMessages)

	t.Logf("Then: %d messages are present in destination topic %s", numMessages, migratorTestTopic)
	records := readTopicContent(dst, numMessages)
	assert.Len(t, records, numMessages)

	t.Log("And: Messages have correct value")
	for i, record := range records {
		assert.Equal(t, append(pfx, []byte(strconv.Itoa(i))...), record.Value, "Message %d should have correct value", i)
	}
}

func TestIntegrationMigratorMultiPartitionSchemaAwareWithConsumerGroups(t *testing.T) {
	integration.CheckSkip(t)

	const (
		numMessages = 10_000
		subj        = "foo"
		schema      = `{"type":"int"}`

		group = "foo_cg"
	)

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: Schema registry containing a subject and schema")
	srScr, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
	require.NoError(t, err)
	ss, err := srScr.CreateSchema(t.Context(), subj, sr.Schema{Schema: schema})
	require.NoError(t, err)

	t.Log("And: Destination schema registry subject is set to import mode")
	{
		srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
		require.NoError(t, err)
		modeRes := srDst.SetMode(t.Context(), sr.ModeImport, subj)
		require.NoError(t, modeRes[0].Err)
	}

	t.Log("When: Messages are written to the source cluster")
	{
		// Produce directly in 1000-record batches using ProduceSync to speed up test
		const batchSize = 1000
		records := make([]*kgo.Record, 0, batchSize)
		for i := 0; i < numMessages; i++ {
			r := &kgo.Record{
				Topic:     migratorTestTopic,
				Key:       []byte(strconv.Itoa(i)),
				Value:     []byte(strconv.Itoa(i)),
				Partition: int32(i % 2),
				Timestamp: time.Unix(100, 0).Add(time.Duration(i) * 100 * time.Millisecond),
			}
			// Apply schema id header the same way as ProduceWithSchemaIDOpt
			ProduceWithSchemaIDOpt(ss.ID)(r)
			records = append(records, r)
			if len(records) == batchSize || i == numMessages-1 {
				ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
				require.NoError(t, src.Client.ProduceSync(ctx, records...).FirstErr())
				cancel()
				records = records[:0]
			}
		}
	}

	t.Log("And: Consumer group reads from source cluster")
	{
		var offsets kadm.Offsets
		offsets.Add(kadm.Offset{
			Topic:     migratorTestTopic,
			Partition: 0,
			At:        1000,
		})
		offsets.Add(kadm.Offset{
			Topic:     migratorTestTopic,
			Partition: 1,
			At:        1002,
		})
		resp, err := src.Admin.CommitOffsets(t.Context(), group, offsets)
		require.NoError(t, err)
		require.NoError(t, resp.Error())
	}

	t.Log("And: Migrator is started")
	startMigratorAndWaitForMessages(t, src, dst, numMessages)

	t.Log("Then: Schema is visible at destination")
	srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
	require.NoError(t, err)
	txt, err := srDst.SchemaTextByVersion(t.Context(), subj, 1)
	require.NoError(t, err)
	assert.Equal(t, schema, txt)

	t.Logf("And: %d schema-encoded messages are present in destination topic %s", numMessages, migratorTestTopic)
	records := readTopicContent(dst, numMessages)
	assert.Len(t, records, numMessages)

	t.Logf("And: partition and timestamp are correctly set for each message")
	sort.Slice(records, func(i, j int) bool {
		a, err := strconv.Atoi(string(records[i].Value[5:]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := strconv.Atoi(string(records[j].Value[5:]))
		if err != nil {
			t.Fatal(err)
		}
		return a < b
	})
	for i, r := range records {
		hdr := make([]byte, 5)
		hdr[0] = 0
		binary.BigEndian.PutUint32(hdr[1:], uint32(ss.ID))
		assert.Equal(t, hdr, r.Value[0:5])
		assert.Equal(t, []byte(strconv.Itoa(i)), r.Value[5:])
		assert.Equal(t, int32(i%2), r.Partition)
		assert.Equal(t, time.Unix(100, 0).Add(time.Duration(i)*100*time.Millisecond), r.Timestamp)
	}

	t.Log("And: Consumer group is migrated")
	assert.Eventually(t, func() bool {
		offsets, err := dst.Admin.FetchOffsets(t.Context(), group)
		require.NoError(t, err)
		t.Log(offsets)
		return offsets[migratorTestTopic][0].At == 1000 && offsets[migratorTestTopic][1].At == 1002
	}, redpandaTestWaitTimeout, time.Second)

	t.Log("And: Metrics are available and can be listed")
	metrics := readMetrics(t, "http://"+httpAddr)
	require.NotEmpty(t, metrics)

	for key, value := range metrics {
		if strings.Contains(key, "redpanda") {
			t.Logf("  %s: %v", key, value)
		}
	}
}

func TestIntegrationMigratorInputKafkaFranzConsumerGroup(t *testing.T) {
	integration.CheckSkip(t)

	const group = "foobar_cg"

	// readMessageWithKafkaFranzInput reads 1 message from the given topic with
	// the test consumer group.
	readMessageWithKafkaFranzInput := func(cluster EmbeddedRedpandaCluster) string {
		configYAML := fmt.Sprintf(`
input:
  kafka_franz:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: %s

output:
  drop: {}

logger:
  level: DEBUG
`, cluster.BrokerAddr, migratorTestTopic, group)

		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(configYAML))

		msgCh := make(chan []byte)
		require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			b, err := m.AsBytes()
			require.NoError(t, err)
			msgCh <- b
			return nil
		}))

		stream, err := sb.Build()
		require.NoError(t, err)

		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
			defer cancel()
			require.NoError(t, stream.Run(ctx))
		}()

		msg := <-msgCh
		require.NoError(t, stream.StopWithin(stopStreamTimeout))
		return string(msg)
	}

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)
	src.SchemaRegistryURL = ""
	dst.SchemaRegistryURL = ""

	t.Log("When: first message is produced to source")
	msg1 := `{"test":"foo"}`
	src.Produce(migratorTestTopic, []byte(msg1))

	t.Log("And: migrator is started")
	msgChan := make(chan *service.Message, 10)

	startMigrator(t, src, dst, func(_ context.Context, m *service.Message) error {
		msgChan <- m
		return nil
	})

	t.Log("Then: the first message is migrated")
	select {
	case <-msgChan:
		t.Log("First message migrated")
	case <-time.After(redpandaTestWaitTimeout):
		require.FailNow(t, "timed out waiting for migrator transfer")
	}

	t.Log("And: Consumer group reads from source using connect pipeline")
	assert.Equal(t, msg1, readMessageWithKafkaFranzInput(src))

	t.Log("When: Second message is produced to source")
	msg2 := `{"test":"bar"}`
	src.Produce(migratorTestTopic, []byte(msg2))

	select {
	case <-msgChan:
		t.Log("Second message migrated")
	case <-time.After(redpandaTestWaitTimeout):
		require.FailNow(t, "timed out waiting for second message migration")
	}

	t.Log("And: consumer group is updated in destination cluster")
	assert.Eventually(t, func() bool {
		cgo, err := dst.Admin.FetchOffsets(t.Context(), group)
		if err != nil {
			t.Logf("Failed to fetch offsets: %v", err)
			return false
		}
		t.Logf("Consumer group offsets: %+v", cgo)

		var ok bool
		cgo.Each(func(resp kadm.OffsetResponse) {
			require.NoError(t, resp.Err)
			require.Equal(t, migratorTestTopic, resp.Topic)
			if resp.At > 0 {
				ok = true
			}
		})
		return ok
	}, 1*time.Minute, time.Second)

	t.Log("Then: Consumer group reads from destination using connect pipeline")
	assert.Equal(t, msg2, readMessageWithKafkaFranzInput(dst))
}

// TestIntegrationRealMigratorConfluentToServerless tests the migration from
// Confluent to Redpanda Serverless. Confluent is running in a Docker container
// and Redpanda Serverless is a hand provisioned cluster.
//
// In order to run this test, you need to set the REDPANDA_SERVERLESS_SEED and
// REDPANDA_SCHEMA_REGISTRY_URL environment variables pointing to a Redpanda
// Serverless cluster seed node address and Schema Registry URL. You can copy
// them from the Redpanda Serverless UI.
//
// The Redpanda Serverless cluster must have user migrator with permissions to
// read and write to all topics and Schema Registry.
func TestIntegrationRealMigratorConfluentToServerless(t *testing.T) {
	integration.CheckSkip(t)

	redpandaServerlessSeed := os.Getenv("REDPANDA_SERVERLESS_SEED")
	if redpandaServerlessSeed == "" {
		t.Skip("Skipping because of missing REDPANDA_SERVERLESS_SEED")
	}
	redpandaServerlessSchemaRegistryURL := os.Getenv("REDPANDA_SCHEMA_REGISTRY_URL")
	if redpandaServerlessSchemaRegistryURL == "" {
		t.Skip("Skipping because of missing REDPANDA_SCHEMA_REGISTRY_URL")
	}

	const (
		numMessages = 10_000
		batchSize   = 1_000
	)
	topics := []string{"foo", "bar"}

	t.Log("Given: Confluent server with Schema Registry as source")
	src := startConfluent(t)
	ctx := t.Context()

	t.Log("And: Topics and ACLs initialized on source")
	{
		// Create topics
		for _, topic := range topics {
			_, err := src.Admin.CreateTopic(ctx, 2, 1, nil, topic)
			require.NoError(t, err)
			t.Logf("Created topic: %s", topic)
		}

		// Create ACLs...
		// Allow redpanda user to read from foo topic
		allowACL := kadm.NewACLs().
			Topics("foo").
			ResourcePatternType(kadm.ACLPatternLiteral).
			Operations(kmsg.ACLOperationRead).
			Allow("User:redpanda")
		_, err := src.Admin.CreateACLs(ctx, allowACL)
		require.NoError(t, err)
		t.Log("Created ALLOW ACL for User:redpanda on topic foo")

		// Deny redpanda user to read from bar topic
		denyACL := kadm.NewACLs().
			Topics("bar").
			ResourcePatternType(kadm.ACLPatternLiteral).
			Operations(kmsg.ACLOperationRead).
			Deny("User:redpanda")
		_, err = src.Admin.CreateACLs(ctx, denyACL)
		require.NoError(t, err)
	}

	t.Log("And: Schema Registry initialized on source with two identical schemas with different IDs")
	{
		const schema = `{"type":"record","name":"SyntheticData","fields":[{"name":"data","type":"int"}]}`

		srClient, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
		require.NoError(t, err)

		fooSchema, err := srClient.CreateSchema(t.Context(), "foo", sr.Schema{
			Schema: schema,
			SchemaMetadata: &sr.SchemaMetadata{
				Tags: map[string][]string{
					"confluent.io/subject": {"foo"},
				},
			},
		})
		require.NoError(t, err)

		barSchema, err := srClient.CreateSchema(t.Context(), "bar", sr.Schema{
			Schema: schema,
			SchemaMetadata: &sr.SchemaMetadata{
				Tags: map[string][]string{
					"confluent.io/subject": {"bar"},
				},
			},
		})
		require.NoError(t, err)

		assert.NotEqual(t, fooSchema.ID, barSchema.ID)
	}

	t.Logf("When: running data generator with %d messages", numMessages)
	{
		configYAML := fmt.Sprintf(`
http:
  enabled: false

input:
  generate:
    mapping: |
      let msg = counter()
      root.data = $msg
      
      meta kafka_topic = match $msg %% 2 {
        0 => "foo"
        1 => "bar"
      }
      
      # Set manual timestamp (1 second per message)
      meta timestamp = 489621600 + $msg
    count: %d
    batch_size: %d

  processors:
    - schema_registry_encode:
        url: "%s"
        subject: ${! metadata("kafka_topic") }
        avro_raw_json: true

output:
  kafka_franz:
    seed_brokers: [ "%s" ]
    topic: ${! @kafka_topic }
    partitioner: manual
    partition: ${! random_int(min:0, max:1) }
    timestamp: ${! @timestamp }

logger:
  level: info
`, numMessages, batchSize, src.SchemaRegistryURL, src.BrokerAddr)

		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(configYAML))
		stream, err := sb.Build()
		require.NoError(t, err)
		require.NoError(t, stream.Run(ctx))

		t.Log("Then: data is written to all partitions in all topics")
		eo, err := src.Admin.ListEndOffsets(t.Context(), topics...)
		require.NoError(t, err)
		total := int64(0)
		eo.Each(func(lo kadm.ListedOffset) {
			total += lo.Offset
			t.Logf("Topic %s partition %d: end offset=%d", lo.Topic, lo.Partition, lo.Offset)
			assert.InEpsilon(t, numMessages/4, lo.Offset, 0.1)
		})
		assert.Equal(t, int64(numMessages), total)
	}

	t.Log("When: consumer group has read from topic 'foo'")
	const group = "foobar_cg"
	{
		configYAML := fmt.Sprintf(`
input:
  kafka_franz:
    seed_brokers: [ "%s" ]
    topics: [ "%s" ]
    consumer_group: "%s"
    fetch_max_partition_bytes: 100B
    batching:
      count: 1

  processors:
    - schema_registry_decode:
        url: "%s"

output:
  drop: {}
  # Replace drop with the following to see the messages in stdout
  #stdout: {}
  #processors:
  #  - mapping: |
  #      root = this.merge({"count": counter(), "topic": @kafka_topic, "partition": @kafka_partition})
`, src.BrokerAddr, "foo", group, src.SchemaRegistryURL)
		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(configYAML))

		msgCh := make(chan *service.Message)
		require.NoError(t, sb.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			select {
			case msgCh <- msg:
			case <-ctx.Done():
			}
			return nil
		}))

		stream, err := sb.Build()
		require.NoError(t, err)

		go func() {
			require.NoError(t, stream.Run(ctx))
		}()

		for range 1_000 {
			select {
			case <-msgCh:
			case <-time.After(redpandaTestOpTimeout):
				t.Fatal("timeout waiting for message")
			}
		}
		stopStreamAndWait(t, stream, stopStreamTimeout)
	}

	t.Log("Then: consumer group metadata is updated in source cluster")
	{
		cgo, err := src.Admin.FetchOffsets(ctx, group)
		require.NoError(t, err)
		assert.Len(t, cgo["foo"], 2)
		cgo.Each(func(resp kadm.OffsetResponse) {
			require.NoError(t, resp.Err)
			t.Logf("Topic %s partition %d: offset=%d", resp.Topic, resp.Partition, resp.At)
			require.Equal(t, "foo", resp.Topic)
			require.Greater(t, resp.At, int64(0))
		})
	}

	// Create dstAdmin client to verify consumer group migration
	opts := []kgo.Opt{
		kgo.SeedBrokers(redpandaServerlessSeed),
		kgo.DialTLSConfig(new(tls.Config)),
		kgo.SASL(scram.Auth{
			User: "migrator",
			Pass: "migrator",
		}.AsSha256Mechanism()),
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	dstAdmin := kadm.NewClient(client)
	defer dstAdmin.Close()

	t.Log("When: Migrator is started")
	{
		configYAML := fmt.Sprintf(`
http:
  enabled: true

input:
  redpanda_migrator:
    seed_brokers: [ "%s" ]
    topics:
      - '^[^_]'
    regexp_topics: true
    consumer_group: migrator_cg
    schema_registry:
      url: "%s"

output:
  redpanda_migrator:
    seed_brokers: [ "%s" ]
    tls:
      enabled: true
    sasl:
      - mechanism: SCRAM-SHA-256
        username: migrator
        password: migrator
    schema_registry:
      url: "%s"
      basic_auth:
        enabled: true
        username: migrator
        password: migrator
      translate_ids: true
    consumer_groups:
      interval: 2s
    serverless: true

logger:
  level: debug
`, src.BrokerAddr, src.SchemaRegistryURL, redpandaServerlessSeed, redpandaServerlessSchemaRegistryURL)

		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(configYAML))

		msgCh := make(chan *service.Message)
		require.NoError(t, sb.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			select {
			case msgCh <- msg:
			case <-ctx.Done():
			}
			return nil
		}))

		stream, err := sb.Build()
		require.NoError(t, err)

		t.Log("Starting data migration from source to serverless destination...")
		go func() {
			require.NoError(t, stream.Run(ctx))
		}()

		count := 0
		for range numMessages {
			select {
			case <-msgCh:
				count += 1
				if count%1000 == 0 {
					t.Logf("Migrated %d messages", count)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("timeout waiting for message")
			}
		}

		t.Log("Waiting for consumer group migration to complete...")
		assert.Eventually(t, func() bool {
			cgo, err := dstAdmin.FetchOffsets(ctx, group)
			if err != nil {
				t.Logf("Failed to fetch offsets: %v", err)
				return false
			}
			t.Logf("Consumer group offsets: %+v", cgo)

			p0, ok := cgo.Lookup("foo", 0)
			if !ok {
				return false
			}
			if p0.At == 0 {
				return false
			}
			p1, ok := cgo.Lookup("foo", 1)
			if !ok {
				return false
			}
			if p1.At == 0 {
				return false
			}

			return true
		}, 1*time.Minute, redpandaTestWaitTimeout)

		stopStreamAndWait(t, stream, stopStreamTimeout)
	}

	t.Log("Then: consumer group metadata is updated in destination cluster")
	{
		cgo, err := dstAdmin.FetchOffsets(ctx, group)
		require.NoError(t, err)
		assert.Len(t, cgo["foo"], 2)
		cgo.Each(func(resp kadm.OffsetResponse) {
			require.NoError(t, resp.Err)
			t.Logf("Destination topic %s partition %d: offset=%d", resp.Topic, resp.Partition, resp.At)
			require.Equal(t, "foo", resp.Topic)
			require.Greater(t, resp.At, int64(0))
		})
	}

	t.Log("Then: consumer group can continue to read from topic 'foo' in destination cluster")
	{
		configYAML := fmt.Sprintf(`
input:
  kafka_franz:
    seed_brokers: [ "%s" ]
    tls:
      enabled: true
    sasl:
      - mechanism: SCRAM-SHA-256
        username: migrator
        password: migrator
    topics: [ "%s" ]
    consumer_group: "%s"

  processors:
    - schema_registry_decode:
        url: "%s"
        basic_auth:
          enabled: true
          username: migrator
          password: migrator
        avro_raw_json: true

output:
  stdout: {}
  processors:
    - mapping: |
        root = this.merge({"count": counter(), "topic": @kafka_topic, "partition": @kafka_partition})
`, redpandaServerlessSeed, "foo", group, redpandaServerlessSchemaRegistryURL)
		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(configYAML))

		msgCh := make(chan *service.Message)
		require.NoError(t, sb.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
			b, err := msg.AsBytes()
			require.NoError(t, err)
			v := struct {
				Data int `json:"data"`
			}{}
			require.NoError(t, json.Unmarshal(b, &v))

			select {
			case msgCh <- msg:
			case <-ctx.Done():
			}
			return nil
		}))

		stream, err := sb.Build()
		require.NoError(t, err)

		go func() {
			require.NoError(t, stream.Run(ctx))
		}()

		for range 10 {
			select {
			case <-msgCh:
			case <-time.After(10 * time.Second):
				t.Fatal("timeout waiting for message")
			}
		}
		require.NoError(t, stream.StopWithin(stopStreamTimeout))
	}
}

func TestIntegrationMigratorTwoWayWithProvenanceHeaders(t *testing.T) {
	integration.CheckSkip(t)

	const numMessages = 10

	t.Log("Given: Two Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)
	src.SchemaRegistryURL = ""
	dst.SchemaRegistryURL = ""
	dst.CreateTopic(migratorTestTopic)

	t.Log("When: Migrator is started from src to dst")
	startMigrator(t, src, dst, nil)

	t.Log("And: Migrator is started from dst to src")
	startMigrator(t, dst, src, nil)

	t.Log("And: 10 messages are produced to src")
	for i := range numMessages {
		src.Produce(migratorTestTopic, []byte(fmt.Sprintf("src-%d", i)))
	}

	t.Log("And: 10 messages are produced to dst")
	for i := range numMessages {
		dst.Produce(migratorTestTopic, []byte(fmt.Sprintf("dst-%d", i)))
	}

	t.Log("Then: Both clusters have 20 messages")
	assert.Eventually(t, func() bool {
		srcRecords := countMessages(t, src)
		dstRecords := countMessages(t, dst)
		t.Logf("src has %d messages, dst has %d messages", srcRecords, dstRecords)
		return srcRecords == 20 && dstRecords == 20
	}, redpandaTestWaitTimeout, 500*time.Millisecond)
	assert.Never(t, func() bool {
		srcRecords := countMessages(t, src)
		dstRecords := countMessages(t, dst)
		return srcRecords != 20 || dstRecords != 20
	}, time.Second, 100*time.Millisecond)
}

func countMessages(t *testing.T, cluster EmbeddedRedpandaCluster) int {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestOpTimeout)
	defer cancel()

	offsets, err := cluster.Admin.ListEndOffsets(ctx, migratorTestTopic)
	if err != nil {
		t.Logf("Failed to list end offsets: %v", err)
		return 0
	}

	total := 0
	offsets.Each(func(o kadm.ListedOffset) {
		total += int(o.Offset)
	})
	return total
}

const stopStreamTimeout = 3 * time.Second

func stopStreamAndWait(t *testing.T, stream *service.Stream, d time.Duration) {
	start := time.Now()
	require.NoError(t, stream.StopWithin(d))
	d = d - time.Since(start)
	if d > 0 {
		time.Sleep(d)
	}
}

func TestIntegrationMigratorJiraCON229(t *testing.T) {
	integration.CheckSkip(t)

	const (
		numMessages   = 1000
		numPartitions = 4
		topicA        = "topicA"
		topicB        = "topicB"
		topicC        = "topicC"
		topicD        = "topicD"
		consumerGroup = "use2-aa-pfx-tp-pipe"
		schemaSubject = "test-value"
		schema        = `{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"int"},{"name":"data","type":"string"}]}`
	)

	t.Log("Given: Redpanda clusters with schema registry")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: ACLs configured for idempotent writes")
	src.CreateClusterACLAllow("User:*", kmsg.ACLOperationIdempotentWrite)
	dst.CreateClusterACLAllow("User:*", kmsg.ACLOperationIdempotentWrite)

	t.Log("And: Schema registry initialized with test schema")
	srSrc, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
	require.NoError(t, err)
	ss, err := srSrc.CreateSchema(t.Context(), schemaSubject, sr.Schema{Schema: schema})
	require.NoError(t, err)
	t.Logf("Created schema with ID: %d", ss.ID)

	t.Log("And: Destination schema registry subject is set to import mode")
	{
		srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
		require.NoError(t, err)
		modeRes := srDst.SetMode(t.Context(), sr.ModeImport, schemaSubject)
		require.NoError(t, modeRes[0].Err)
	}

	t.Log("And: Multiple topics created with multiple partitions")
	for _, topic := range []string{topicA, topicB, topicC, topicD} {
		_, err := src.Admin.CreateTopic(t.Context(), numPartitions, 1, nil, topic)
		require.NoError(t, err)
		t.Logf("Created topic: %s", topic)
	}

	t.Log("When: 1000 messages are written to each partition of each topic")
	{
		addSchemaID := ProduceWithSchemaIDOpt(ss.ID)

		for _, topic := range []string{topicA, topicB, topicC, topicD} {
			records := make([]*kgo.Record, 0, numMessages)
			for i := 0; i < numMessages; i++ {
				r := &kgo.Record{
					Topic:     topic,
					Key:       []byte(fmt.Sprintf("%s-key-%d", topic, i)),
					Value:     []byte(fmt.Sprintf(`{"id":%d,"data":"msg-%d"}`, i, i)),
					Partition: int32(i % numPartitions),
					Timestamp: time.Unix(100, 0).Add(time.Duration(i) * 100 * time.Millisecond),
				}
				addSchemaID(r)
				records = append(records, r)
			}
			require.NoError(t, src.Client.ProduceSync(t.Context(), records...).FirstErr())
		}
		t.Logf("Successfully wrote %d messages to each of 4 topics", numMessages)
	}

	t.Log("And: Migrator is started with schema registry and consumer group migration")
	{
		const yamlTmpl = `
http:
  enabled: true
  address: {{.HTTPAddr}}

input:
  redpanda_migrator:
    seed_brokers: 
      - {{.Src.BrokerAddr}}
    topics: 
      - {{.TopicA}}
      - {{.TopicB}}
      - {{.TopicC}}
      - {{.TopicD}}
    consumer_group: {{.ConsumerGroup}}
    auto_replay_nacks: false
    commit_period: 5s
    conn_idle_timeout: 60s
    fetch_max_bytes: 100MiB
    fetch_max_partition_bytes: 10MiB
    fetch_max_wait: 1s
    fetch_min_bytes: 100KB
    heartbeat_interval: 3s
    max_yield_batch_bytes: 100MB
    metadata_max_age: 1m
    partition_buffer_bytes: 10MB
    rebalance_timeout: 45s
    session_timeout: 1m
    start_offset: earliest
    topic_lag_refresh_period: 5s
    schema_registry:
      url: {{.Src.SchemaRegistryURL}}

output:
  redpanda_migrator:
    seed_brokers: [ {{.Dst.BrokerAddr}} ]
    allow_auto_topic_creation: true
    topic: use1_${! @kafka_topic }
    broker_write_max_bytes: 100MiB
    compression: snappy
    conn_idle_timeout: 120s
    consumer_groups:
      enabled: true
      fetch_timeout: 10s
      interval: 5s
      only_empty: false
    idempotent_write: true
    max_message_bytes: 100MB
    metadata_max_age: 5s
    sync_topic_acls: false
    timeout: 10s
    schema_registry:
      url: {{.Dst.SchemaRegistryURL}}
      enabled: true

logger:
  level: DEBUG
`
		tmpl, err := template.New("migrator").Parse(yamlTmpl)
		require.NoError(t, err)

		data := struct {
			Src           EmbeddedRedpandaCluster
			Dst           EmbeddedRedpandaCluster
			TopicA        string
			TopicB        string
			TopicC        string
			TopicD        string
			ConsumerGroup string
			HTTPAddr      string
		}{
			Src:           src,
			Dst:           dst,
			TopicA:        topicA,
			TopicB:        topicB,
			TopicC:        topicC,
			TopicD:        topicD,
			ConsumerGroup: consumerGroup,
			HTTPAddr:      httpAddr,
		}
		var yamlBuf bytes.Buffer
		require.NoError(t, tmpl.Execute(&yamlBuf, data))

		sb := service.NewStreamBuilder()
		require.NoError(t, sb.SetYAML(yamlBuf.String()))

		msgChan := make(chan *service.Message, 1000)
		require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			msgChan <- m
			return nil
		}))

		stream, err := sb.Build()
		require.NoError(t, err)

		go func() {
			if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
			t.Log("Migrator pipeline shutdown")
		}()

		t.Cleanup(func() {
			require.NoError(t, stream.StopWithin(stopStreamTimeout))
		})

		totalMessages := numMessages * 4
		t.Logf("Then: Waiting for %d messages to be migrated", totalMessages)
		for i := 0; i < totalMessages; i++ {
			select {
			case <-msgChan:
				if (i+1)%100 == 0 {
					t.Logf("Migrated %d messages", i+1)
				}
			case <-time.After(redpandaTestWaitTimeout):
				t.Fatalf("Timed out waiting for message %d of %d", i+1, totalMessages)
			}
		}
	}

	t.Log("Then: Schema is visible at destination")
	srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
	require.NoError(t, err)
	txt, err := srDst.SchemaTextByVersion(t.Context(), schemaSubject, 1)
	require.NoError(t, err)
	assert.Equal(t, schema, txt)

	t.Log("And: Destination topics exist with correct partitions")
	for _, topic := range []string{topicA, topicB, topicC, topicD} {
		dstTopic := fmt.Sprintf("use1_%s", topic)
		details := dst.DescribeTopic(dstTopic)
		assert.Len(t, details.Partitions, numPartitions, "Topic %s should have %d partitions", dstTopic, numPartitions)
		t.Logf("Topic %s exists with %d partitions", dstTopic, len(details.Partitions))
	}

	t.Log("And: All messages are present in destination topics")
	ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
	defer cancel()
	for _, topic := range []string{topicA, topicB, topicC, topicD} {
		dstTopic := fmt.Sprintf("use1_%s", topic)
		assert.Eventually(t, func() bool {
			eo, err := dst.Admin.ListEndOffsets(ctx, dstTopic)
			if err != nil {
				t.Logf("list end offsets error for %s: %v", dstTopic, err)
				return false
			}
			var total int64
			eo.Each(func(lo kadm.ListedOffset) {
				total += lo.Offset
			})
			return total == int64(numMessages)
		}, redpandaTestWaitTimeout, 500*time.Millisecond, "Topic %s should have %d messages", dstTopic, numMessages)
	}
}
