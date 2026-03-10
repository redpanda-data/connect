// Copyright 2026 Redpanda Data, Inc.
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

package confluent

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// integrationMockRegistry creates a mock Confluent Schema Registry that
// supports both GET (for registry-pull mode) and POST (for CreateSchema in
// metadata-push mode), including the franz-go follow-up GET requests.
func integrationMockRegistry(t *testing.T, preloaded map[string]integrationSchema) *httptest.Server {
	t.Helper()

	var (
		mu          sync.Mutex
		nextID      = 1
		schemas     = map[int]integrationSchema{} // id → schema
		subjectVer  = map[string]int{}            // subject → next version
		idToSubject = map[int]string{}
		idToVersion = map[int]int{}
	)

	// Preload schemas for registry-pull tests.
	for subject, s := range preloaded {
		id := nextID
		nextID++
		schemas[id] = s
		subjectVer[subject] = 1
		idToSubject[id] = subject
		idToVersion[id] = 1
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		path := r.URL.Path

		// POST /subjects/{subject}/versions — CreateSchema
		if r.Method == http.MethodPost && strings.Contains(path, "/subjects/") && strings.HasSuffix(path, "/versions") {
			body, _ := io.ReadAll(r.Body)
			subject := strings.TrimPrefix(path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions")

			var posted map[string]any
			_ = json.Unmarshal(body, &posted)
			schemaStr, _ := posted["schema"].(string)
			schemaType, _ := posted["schemaType"].(string)

			id := nextID
			nextID++
			schemas[id] = integrationSchema{Schema: schemaStr, SchemaType: schemaType}
			idToSubject[id] = subject
			subjectVer[subject]++
			idToVersion[id] = subjectVer[subject]

			_, _ = w.Write(mustJBytes(t, map[string]int{"id": id}))
			return
		}

		// GET /subjects/{subject}/versions/latest — GetLatestSchema (registry-pull)
		if r.Method == http.MethodGet && strings.Contains(path, "/subjects/") && strings.HasSuffix(path, "/versions/latest") {
			subject := strings.TrimPrefix(path, "/subjects/")
			subject = strings.TrimSuffix(subject, "/versions/latest")
			for id, subj := range idToSubject {
				if subj == subject {
					s := schemas[id]
					resp := map[string]any{
						"subject": subject,
						"version": idToVersion[id],
						"id":      id,
						"schema":  s.Schema,
					}
					if s.SchemaType != "" {
						resp["schemaType"] = s.SchemaType
					}
					_, _ = w.Write(mustJBytes(t, resp))
					return
				}
			}
		}

		// GET /schemas/ids/{id}/versions
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			idPart = strings.TrimSuffix(idPart, "/versions")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if subject, ok := idToSubject[id]; ok {
					_, _ = w.Write(mustJBytes(t, []map[string]any{
						{"subject": subject, "version": idToVersion[id]},
					}))
					return
				}
			}
		}

		// GET /schemas/ids/{id}
		if r.Method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/") && !strings.HasSuffix(path, "/versions") {
			idPart := strings.TrimPrefix(path, "/schemas/ids/")
			var id int
			if _, err := fmt.Sscanf(idPart, "%d", &id); err == nil {
				if s, ok := schemas[id]; ok {
					resp := map[string]any{"schema": s.Schema, "id": id}
					if s.SchemaType != "" {
						resp["schemaType"] = s.SchemaType
					}
					_, _ = w.Write(mustJBytes(t, resp))
					return
				}
			}
		}

		// GET /subjects/{subject}/versions/{version}
		if r.Method == http.MethodGet && strings.Contains(path, "/subjects/") && strings.Contains(path, "/versions/") {
			parts := strings.SplitN(strings.TrimPrefix(path, "/subjects/"), "/versions/", 2)
			if len(parts) == 2 && parts[1] != "latest" {
				var version int
				if _, err := fmt.Sscanf(parts[1], "%d", &version); err == nil {
					for id, subj := range idToSubject {
						if subj == parts[0] && idToVersion[id] == version {
							s := schemas[id]
							resp := map[string]any{
								"subject": parts[0],
								"version": version,
								"id":      id,
								"schema":  s.Schema,
							}
							if s.SchemaType != "" {
								resp["schemaType"] = s.SchemaType
							}
							_, _ = w.Write(mustJBytes(t, resp))
							return
						}
					}
				}
			}
		}

		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(ts.Close)
	return ts
}

type integrationSchema struct {
	Schema     string
	SchemaType string // "" for Avro, "JSON" for JSON Schema
}

//------------------------------------------------------------------------------
// Registry-pull mode integration tests
//------------------------------------------------------------------------------

func TestIntegrationSchemaRegistryEncodeAvro(t *testing.T) {
	const avroSchema = `{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	ts := integrationMockRegistry(t, map[string]integrationSchema{
		"person-value": {Schema: avroSchema},
	})

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = "{\"name\":\"Alice\",\"age\":30}"'
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: person-value
        avro_raw_json: true

output:
  drop: {}
`, ts.URL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	// Verify Confluent wire format header.
	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Equal(t, uint32(1), schemaID)
}

func TestIntegrationSchemaRegistryEncodeJSON(t *testing.T) {
	const jsonSchema = `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name"]
	}`

	ts := integrationMockRegistry(t, map[string]integrationSchema{
		"person-value": {Schema: jsonSchema, SchemaType: "JSON"},
	})

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = "{\"name\":\"Alice\",\"age\":30}"'
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: person-value

output:
  drop: {}
`, ts.URL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg)
	b, err := msg.AsBytes()
	require.NoError(t, err)

	// JSON Schema: payload passes through with wire header.
	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	assert.Equal(t, `{"name":"Alice","age":30}`, string(b[5:]))
}

//------------------------------------------------------------------------------
// Metadata-push mode integration tests
//------------------------------------------------------------------------------

func TestIntegrationSchemaRegistryEncodeMetadataAvro(t *testing.T) {
	ts := integrationMockRegistry(t, nil)

	// This pipeline:
	// 1. Generates a JSON message
	// 2. Uses bloblang to attach a common schema as metadata
	// 3. Encodes via schema_registry_encode in metadata mode
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Person","children":[{"type":"STRING","name":"name"},{"type":"INT32","name":"age"}],"fingerprint":"abc123"}
      root = "{\"name\":\"Alice\",\"age\":30}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: person-value
        schema_metadata: schema
        format: avro
        avro:
          raw_json: true

output:
  drop: {}
`, ts.URL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	// Verify wire format: magic byte + schema ID + Avro binary payload.
	require.Greater(t, len(b), 5, "output must have wire header + payload")
	assert.Equal(t, byte(0x00), b[0])
	schemaID := binary.BigEndian.Uint32(b[1:5])
	assert.Greater(t, schemaID, uint32(0), "schema ID should be assigned")
}

func TestIntegrationSchemaRegistryEncodeMetadataJSONSchema(t *testing.T) {
	ts := integrationMockRegistry(t, nil)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Person","children":[{"type":"STRING","name":"name"},{"type":"INT32","name":"age"}],"fingerprint":"def456"}
      root = "{\"name\":\"Bob\",\"age\":25}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: person-value
        schema_metadata: schema
        format: json_schema

output:
  drop: {}
`, ts.URL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	// JSON Schema: wire header + passthrough payload.
	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	assert.Equal(t, `{"name":"Bob","age":25}`, string(b[5:]))
}

func TestIntegrationSchemaRegistryEncodeMetadataRoundTrip(t *testing.T) {
	// End-to-end: encode with metadata mode, then decode with schema_registry_decode.
	// This verifies the Avro binary produced by metadata mode is decodable.
	ts := integrationMockRegistry(t, nil)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Record","children":[{"type":"STRING","name":"name"},{"type":"INT64","name":"count"}],"fingerprint":"rt001"}
      root = "{\"name\":\"test\",\"count\":42}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: roundtrip-value
        schema_metadata: schema
        format: avro
        avro:
          raw_json: true
    - schema_registry_decode:
        url: %s
        avro:
          raw_unions: true

output:
  drop: {}
`, ts.URL, ts.URL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 5*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	var actual map[string]any
	require.NoError(t, json.Unmarshal(b, &actual))
	assert.Equal(t, "test", actual["name"])
	// JSON numbers decode as float64.
	assert.Equal(t, 42., actual["count"])
}
