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

package confluent_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/confluent"

	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
)

func startRedpanda(t *testing.T) redpandatest.Endpoints {
	t.Helper()
	integration.CheckSkip(t)

	endpoints, _, err := redpandatest.StartSingleBroker(t)
	require.NoError(t, err)
	return endpoints
}

// srCreateSchema registers a schema with the Redpanda Schema Registry via HTTP.
func srCreateSchema(t *testing.T, srURL, subject, schemaStr, schemaType string) int {
	t.Helper()

	body := map[string]string{"schema": schemaStr}
	if schemaType != "" {
		body["schemaType"] = schemaType
	}
	b, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		fmt.Sprintf("%s/subjects/%s/versions", srURL, subject),
		bytes.NewReader(b))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create schema failed: %s", string(respBody))

	var result struct {
		ID int `json:"id"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result.ID
}

// srGetSchema fetches a schema by ID from the Schema Registry.
func srGetSchema(t *testing.T, srURL string, id int) string {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("%s/schemas/ids/%d", srURL, id), nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "get schema failed: %s", string(respBody))

	var result struct {
		Schema string `json:"schema"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result.Schema
}

// srDeleteSubject deletes a subject from the Schema Registry.
func srDeleteSubject(t *testing.T, srURL, subject string, permanent bool) {
	t.Helper()

	url := fmt.Sprintf("%s/subjects/%s", srURL, subject)
	if permanent {
		url += "?permanent=true"
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
}

//------------------------------------------------------------------------------
// Registry-pull mode with real Redpanda
//------------------------------------------------------------------------------

func TestRedpandaIntegrationSchemaRegistryEncodeAvro(t *testing.T) {
	rp := startRedpanda(t)

	const avroSchema = `{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`

	subject := "person-avro-encode-test-value"
	schemaID := srCreateSchema(t, rp.SchemaRegistryURL, subject, avroSchema, "")
	defer srDeleteSubject(t, rp.SchemaRegistryURL, subject, true)

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
        subject: %s
        avro_raw_json: true

output:
  drop: {}
`, rp.SchemaRegistryURL, subject)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg)
	b, err := msg.AsBytes()
	require.NoError(t, err)

	require.Greater(t, len(b), 5, "must have wire header + payload")
	assert.Equal(t, byte(0x00), b[0])
	gotID := int(binary.BigEndian.Uint32(b[1:5]))
	assert.Equal(t, schemaID, gotID, "schema ID in wire header must match registered schema")
}

func TestRedpandaIntegrationSchemaRegistryEncodeJSON(t *testing.T) {
	rp := startRedpanda(t)

	const jsonSchema = `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name"]
	}`

	subject := "person-json-encode-test-value"
	schemaID := srCreateSchema(t, rp.SchemaRegistryURL, subject, jsonSchema, "JSON")
	defer srDeleteSubject(t, rp.SchemaRegistryURL, subject, true)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = "{\"name\":\"Bob\",\"age\":25}"'
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s

output:
  drop: {}
`, rp.SchemaRegistryURL, subject)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg)
	b, err := msg.AsBytes()
	require.NoError(t, err)

	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	gotID := int(binary.BigEndian.Uint32(b[1:5]))
	assert.Equal(t, schemaID, gotID)
	assert.Equal(t, `{"name":"Bob","age":25}`, string(b[5:]))
}

//------------------------------------------------------------------------------
// Metadata-push mode with real Redpanda
//------------------------------------------------------------------------------

func TestRedpandaIntegrationSchemaRegistryEncodeMetadataAvro(t *testing.T) {
	rp := startRedpanda(t)

	subject := "person-meta-avro-test-value"
	defer srDeleteSubject(t, rp.SchemaRegistryURL, subject, true)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Person","children":[{"type":"STRING","name":"name"},{"type":"INT32","name":"age"}],"fingerprint":"rptest001"}
      root = "{\"name\":\"Alice\",\"age\":30}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s
        schema_metadata: schema
        format: avro
        avro:
          raw_json: true

output:
  drop: {}
`, rp.SchemaRegistryURL, subject)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	// Verify wire format.
	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	schemaID := int(binary.BigEndian.Uint32(b[1:5]))
	assert.Greater(t, schemaID, 0, "registry should have assigned a schema ID")

	// Verify the schema was actually registered with Redpanda's registry.
	registeredSchema := srGetSchema(t, rp.SchemaRegistryURL, schemaID)
	var avro map[string]any
	require.NoError(t, json.Unmarshal([]byte(registeredSchema), &avro))
	assert.Equal(t, "record", avro["type"])
	assert.Equal(t, "Person", avro["name"])
}

func TestRedpandaIntegrationSchemaRegistryEncodeMetadataJSONSchema(t *testing.T) {
	rp := startRedpanda(t)

	subject := "person-meta-json-test-value"
	defer srDeleteSubject(t, rp.SchemaRegistryURL, subject, true)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Person","children":[{"type":"STRING","name":"name"},{"type":"INT32","name":"age"}],"fingerprint":"rptest002"}
      root = "{\"name\":\"Bob\",\"age\":25}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s
        schema_metadata: schema
        format: json_schema

output:
  drop: {}
`, rp.SchemaRegistryURL, subject)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg)
	b, err := msg.AsBytes()
	require.NoError(t, err)

	require.Greater(t, len(b), 5)
	assert.Equal(t, byte(0x00), b[0])
	schemaID := int(binary.BigEndian.Uint32(b[1:5]))
	assert.Greater(t, schemaID, 0)

	// JSON Schema: payload passes through unchanged.
	assert.Equal(t, `{"name":"Bob","age":25}`, string(b[5:]))

	// Verify registered schema is valid JSON Schema.
	registeredSchema := srGetSchema(t, rp.SchemaRegistryURL, schemaID)
	var js map[string]any
	require.NoError(t, json.Unmarshal([]byte(registeredSchema), &js))
	assert.Equal(t, "object", js["type"])
}

func TestRedpandaIntegrationSchemaRegistryEncodeMetadataRoundTrip(t *testing.T) {
	rp := startRedpanda(t)

	subject := "roundtrip-meta-test-value"
	defer srDeleteSubject(t, rp.SchemaRegistryURL, subject, true)

	// Encode with metadata mode, then decode with schema_registry_decode.
	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: |
      meta schema = {"type":"OBJECT","name":"Record","children":[{"type":"STRING","name":"name"},{"type":"INT64","name":"count"}],"fingerprint":"rprt001"}
      root = "{\"name\":\"test\",\"count\":42}"
    count: 1

pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s
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
`, rp.SchemaRegistryURL, subject, rp.SchemaRegistryURL)))
	require.NoError(t, sb.SetLoggerYAML(`level: OFF`))

	msgCh := make(chan *service.Message, 1)
	require.NoError(t, sb.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		msgCh <- msg
		return nil
	}))
	stream, err := sb.Build()
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), 10*time.Second)
	defer done()
	require.NoError(t, stream.Run(ctx))

	msg := <-msgCh
	require.NotNil(t, msg, "no message received")
	b, err := msg.AsBytes()
	require.NoError(t, err)

	var actual map[string]any
	require.NoError(t, json.Unmarshal(b, &actual))
	assert.Equal(t, "test", actual["name"])
	assert.Equal(t, 42., actual["count"])

	// Verify schema_id metadata was set by the decoder.
	schemaIDMeta, ok := msg.MetaGetMut("schema_id")
	assert.True(t, ok, "schema_id metadata should be set by decoder")
	assert.NotNil(t, schemaIDMeta)
}
