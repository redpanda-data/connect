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

package aerospike

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	_ "github.com/redpanda-data/connect/v4/internal/impl/avro"
	_ "github.com/redpanda-data/connect/v4/internal/impl/confluent"
	_ "github.com/redpanda-data/connect/v4/internal/impl/msgpack"
)

// userRecordSchema is a flat Avro record. It has no unions, so both the avro
// processor and schema_registry_decode produce a plain object whose field
// names are the bin names.
const userRecordSchema = `{"type":"record","name":"user","namespace":"rpcn.test","fields":[{"name":"id","type":"string"},{"name":"tier","type":"string"}]}`

// TestIntegrationDecodedFormats writes one record per payload encoding.
// JSON and flat JSON are already objects. MessagePack, Avro, and Kafka Avro
// are decoded by the same processors a pipeline would run, and the Aerospike
// output then sees that object.
func TestIntegrationDecodedFormats(t *testing.T) {
	integration.CheckSkip(t)
	avroBytes := encodeAvroUser(t, "u-avro", "gold")

	t.Run("json", func(t *testing.T) {
		w, client := outputSetup(t, "")
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			msg(t, `{"id":"u-json","tier":"gold","prefs":{"theme":"dark"}}`),
		}))
		rec := outputRead(t, client, "u-json")
		require.NotNil(t, rec)
		assertNestedBins(t, rec.Bins, map[string]any{
			"tier":  "gold",
			"prefs": map[string]any{"theme": "dark"},
		})
	})

	t.Run("flat json", func(t *testing.T) {
		w, client := outputSetup(t, "")
		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{
			msg(t, `{"id":"u-flat","tier":"gold","score":10}`),
		}))
		rec := outputRead(t, client, "u-flat")
		require.NotNil(t, rec)
		assertNestedBins(t, rec.Bins, map[string]any{
			"tier":  "gold",
			"score": 10,
		})
	})

	t.Run("messagepack", func(t *testing.T) {
		w, client := outputSetup(t, "")
		raw, err := msgpack.Marshal(map[string]any{"id": "u-msgpack", "tier": "gold"})
		require.NoError(t, err)
		decoded := decodeWithProcessor(t, "msgpack:\n  operator: to_json\n", raw)

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{decoded}))
		rec := outputRead(t, client, "u-msgpack")
		require.NotNil(t, rec)
		assertNestedBins(t, rec.Bins, map[string]any{"tier": "gold"})
	})

	t.Run("avro", func(t *testing.T) {
		w, client := outputSetup(t, "")
		decoded := decodeWithProcessor(t, `
avro:
  operator: to_json
  encoding: binary
  schema: |
    `+userRecordSchema, avroBytes)

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{decoded}))
		rec := outputRead(t, client, "u-avro")
		require.NotNil(t, rec)
		assertNestedBins(t, rec.Bins, map[string]any{"tier": "gold"})
	})

	t.Run("kafka avro", func(t *testing.T) {
		w, client := outputSetup(t, "")
		const schemaID = 7
		url := schemaRegistryServer(t, schemaID, userRecordSchema)
		decoded := decodeWithProcessor(t, `
schema_registry_decode:
  url: `+url+`
`, confluentWire(schemaID, encodeAvroUser(t, "u-kavro", "gold")))

		require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{decoded}))
		rec := outputRead(t, client, "u-kavro")
		require.NotNil(t, rec)
		assertNestedBins(t, rec.Bins, map[string]any{"tier": "gold"})
	})
}

// TestIntegrationUndecodedFormatsFail proves the output does not detect an
// encoding. The key comes from metadata, so it resolves. Raw MessagePack,
// Avro, and Kafka Avro bytes are not JSON, so the bins mapping fails and
// nothing is written under that key.
func TestIntegrationUndecodedFormatsFail(t *testing.T) {
	w, client := outputSetup(t, `
key: '${! meta("id") }'
bins: 'root = this.without("id")'
`)

	cases := []struct {
		name string
		raw  []byte
		id   string
	}{
		{name: "messagepack", raw: mustMsgpack(t, "u-raw-mp"), id: "u-raw-mp"},
		{name: "avro", raw: encodeAvroUser(t, "u-raw-avro", "gold"), id: "u-raw-avro"},
		{name: "kafka avro", raw: confluentWire(7, encodeAvroUser(t, "u-raw-kavro", "gold")), id: "u-raw-kavro"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rawMsg := service.NewMessage(tc.raw)
			rawMsg.MetaSet("id", tc.id)
			batch := service.MessageBatch{rawMsg}
			indexer := batch.Index()

			err := w.WriteBatch(t.Context(), batch)
			require.Error(t, err)
			assert.Contains(t, firstIndexedError(t, indexer, err).Error(), "bins")
			assert.Nil(t, outputRead(t, client, tc.id))
		})
	}
}

func encodeAvroUser(t *testing.T, id, tier string) []byte {
	t.Helper()
	schema, err := avro.Parse(userRecordSchema)
	require.NoError(t, err)
	raw, err := schema.Encode(map[string]any{"id": id, "tier": tier})
	require.NoError(t, err)
	return raw
}

func mustMsgpack(t *testing.T, id string) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(map[string]any{"id": id, "tier": "gold"})
	require.NoError(t, err)
	return raw
}

func confluentWire(schemaID int, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], payload)
	return out
}

func schemaRegistryServer(t *testing.T, schemaID int, schema string) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{"schema": schema})
	require.NoError(t, err)
	path := "/schemas/ids/" + strconv.Itoa(schemaID)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != path {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// decodeWithProcessor runs one Connect processor over raw bytes and returns
// that processor's message, metadata included. That is the message the next
// stage of a pipeline would see.
func decodeWithProcessor(t *testing.T, processorYAML string, raw []byte) *service.Message {
	t.Helper()

	spec := service.NewConfigSpec().Field(service.NewProcessorField("proc"))
	parsed, err := spec.ParseYAML("proc:\n"+indentYAML(processorYAML), nil)
	require.NoError(t, err)
	proc, err := parsed.FieldProcessor("proc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = proc.Close(context.Background()) })

	batch, err := proc.Process(t.Context(), service.NewMessage(raw))
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, batch[0].GetError())
	return batch[0]
}

func indentYAML(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, line := range lines {
		lines[i] = "  " + line
	}
	return strings.Join(lines, "\n")
}
