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

package confluent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestIntegrationSchemaRegistryDecode(t *testing.T) {
	const schema = `{
		"type": "record",
		"name": "Person",
		"fields": [
			{"name": "name", "type": "string"},
			{"name": "age", "type": "int"}
		]
	}`
	schemaID := 1

	data := "\x08John\x2a"
	expected := map[string]any{
		"name": "John",
		"age":  21.,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/schemas/ids/%d", schemaID) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(mustJBytes(t, map[string]any{
				"schema": schema,
			}))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = "%s".decode("base64")'
    count: 1

pipeline:
  processors:
    - label: add_header
      bloblang: |
        root = with_schema_registry_header(%d, content())
    - label: decode
      schema_registry_decode:
        url: %s

output:
  drop: {}
`, base64.StdEncoding.EncodeToString([]byte(data)), schemaID, ts.URL)))
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
	assert.Equal(t, expected, actual)

	schemaIDMeta, ok := msg.MetaGetMut("schema_id")
	assert.True(t, ok)
	assert.Equal(t, schemaID, schemaIDMeta)
}

func TestIntegrationSchemaRegistryDecodeProtobuf(t *testing.T) {
	const schema = `
syntax = "proto3";
package test;

message User {
  string name = 1;
  int32 age = 2;
}`
	schemaID := 1

	data := "\x00\x0a\x04John\x10\x1e"
	expected := map[string]any{
		"name": "John",
		"age":  30.,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == fmt.Sprintf("/schemas/ids/%d", schemaID) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(mustJBytes(t, map[string]any{
				"schema":     schema,
				"schemaType": "PROTOBUF",
			}))
			return
		}
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(fmt.Sprintf(`
input:
  generate:
    mapping: 'root = "%s".decode("base64")'
    count: 1

pipeline:
  processors:
    - label: add_header
      bloblang: |
        root = with_schema_registry_header(%d, content())
    - label: decode
      schema_registry_decode:
        url: %s

output:
  drop: {}
`, base64.StdEncoding.EncodeToString([]byte(data)), schemaID, ts.URL)))
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
	assert.Equal(t, expected, actual)

	schemaIDMeta, ok := msg.MetaGetMut("schema_id")
	assert.True(t, ok)
	assert.Equal(t, schemaID, schemaIDMeta)
}
