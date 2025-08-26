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
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func startMigrator(t *testing.T, src, dst EmbeddedRedpandaCluster, cb service.MessageHandlerFunc) {
	t.Helper()

	const yamlTmpl = `
input:
  redpanda_migrator2:
    seed_brokers: 
      - {{.Src.BrokerAddr}}
    topics: 
      - {{.Topic}}
    consumer_group: migrator_cg
    start_from_oldest: true
    {{- if .Src.SchemaRegistryURL }}
    schema_registry:
      url: {{.Src.SchemaRegistryURL}}
    {{- end }}
output:
  redpanda_migrator2:
    seed_brokers: [ {{.Dst.BrokerAddr}} ]
    topic: ${! metadata("kafka_topic").or(throw("missing kafka_topic metadata")) }
    sync_topic_acls: true
    {{- if .Dst.SchemaRegistryURL }}
    schema_registry:
      url: {{.Dst.SchemaRegistryURL}}
    {{- end }}
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

func TestIntegrationMigrator(t *testing.T) {
	integration.CheckSkip(t)

	const numMessages = 10

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)
	src.SchemaRegistryURL = ""
	dst.SchemaRegistryURL = ""

	t.Log("When: Messages are written to the source cluster")
	writeToTopic(src, numMessages)

	t.Log("And: Migrator is started")
	startMigratorAndWaitForMessages(t, src, dst, numMessages)

	t.Logf("Then: %d messages are present in destination topic %s", numMessages, migratorTestTopic)
	assertTopicContent(dst, numMessages)
}

func TestIntegrationMigratorWithSchema(t *testing.T) {
	integration.CheckSkip(t)

	const (
		numMessages = 10
		subj        = "foo"
		schema      = `{"type":"int"}`
	)

	t.Log("Given: Redpanda clusters")
	src, dst := startRedpandaSourceAndDestination(t)

	t.Log("And: Schema registry containing a subject and schema")
	srScr, err := sr.NewClient(sr.URLs(src.SchemaRegistryURL))
	require.NoError(t, err)
	ss, err := srScr.CreateSchema(t.Context(), subj, sr.Schema{Schema: schema})
	require.NoError(t, err)

	t.Log("When: Messages are written to the source cluster")
	writeToTopic(src, numMessages, ProduceWithSchemaIDOpt(ss.ID))

	t.Log("And: Migrator is started")
	startMigratorAndWaitForMessages(t, src, dst, numMessages)

	t.Log("Then: Schema is visible at destination")
	srDst, err := sr.NewClient(sr.URLs(dst.SchemaRegistryURL))
	require.NoError(t, err)
	txt, err := srDst.SchemaTextByVersion(t.Context(), subj, 1)
	require.NoError(t, err)
	assert.Equal(t, schema, txt)

	t.Logf("And: %d schema-encoded messages are present in destination topic %s", numMessages, migratorTestTopic)
	withSchema := func(fn func(int) []byte, schemaID int) func(int) []byte {
		hdr := make([]byte, 5)
		hdr[0] = 0
		binary.BigEndian.PutUint32(hdr[1:], uint32(schemaID))

		return func(i int) []byte {
			return append(hdr, fn(i)...)
		}
	}
	assertTopicContentWithGoldenFunc(dst, numMessages, withSchema(goldenIntMsg, ss.ID))
}
