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
	"errors"
	"sort"
	"strconv"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/twmb/franz-go/pkg/sr"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func startMigrator(t *testing.T, src, dst EmbeddedRedpandaCluster, cb service.MessageHandlerFunc) {
	t.Helper()

	const yamlTmpl = `
input:
  redpanda_migrator:
    seed_brokers: 
      - {{.Src.BrokerAddr}}
    topics: 
      - {{.Topic}}
    consumer_group: migrator_cg
    start_from_oldest: true
    fetch_max_bytes: 512B
    {{- if .Src.SchemaRegistryURL }}
    schema_registry:
      url: {{.Src.SchemaRegistryURL}}
    {{- end }}
output:
  redpanda_migrator:
    seed_brokers: [ {{.Dst.BrokerAddr}} ]
    topic: ${! metadata("kafka_topic").or(throw("missing kafka_topic metadata")) }
    sync_topic_acls: true
    {{- if .Dst.SchemaRegistryURL }}
    schema_registry:
      url: {{.Dst.SchemaRegistryURL}}
    {{- end }}
    consumer_groups:
      interval: 1s
      only_empty: true
logger:
  level: DEBUG
`
	tmpl, err := template.New("migrator").Parse(yamlTmpl)
	require.NoError(t, err)

	data := struct {
		Src   EmbeddedRedpandaCluster
		Dst   EmbeddedRedpandaCluster
		Topic string
	}{
		Src:   src,
		Dst:   dst,
		Topic: migratorTestTopic,
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
		require.NoError(t, stream.StopWithin(time.Second))
	})
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
		assert.Equal(t, []byte(strconv.Itoa(i)), record.Value, "Message %d should have correct value", i)
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

	t.Log("When: Messages are written to the source cluster")
	{
		n := 0
		writeToTopic(src, numMessages, ProduceWithSchemaIDOpt(ss.ID), func(r *kgo.Record) {
			r.Partition = int32(n % 2)
			r.Timestamp = time.Unix(100, 0).Add(time.Duration(n) * 100 * time.Millisecond)
			n += 1
		})
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
	}, 10*time.Second, time.Second)
}
