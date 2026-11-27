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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
)

func registerProtobufSchema(t *testing.T, srURL, subject, schema string) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"schema": schema, "schemaType": "PROTOBUF"})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
		fmt.Sprintf("%s/subjects/%s/versions", srURL, subject), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "registering schema: %s", respBody)

	var result struct {
		ID int `json:"id"`
	}
	require.NoError(t, json.Unmarshal(respBody, &result))
	return result.ID
}

// TestParquetEncodeProtobufIntegration checks the processor against the
// schema registry of a real Redpanda broker: every batch must come out the
// same as from schema_registry_decode followed by parquet_encode.
func TestParquetEncodeProtobufIntegration(t *testing.T) {
	integration.CheckSkip(t)

	endpoints, _, err := redpandatest.StartSingleBroker(t)
	require.NoError(t, err)
	url := endpoints.SchemaRegistryURL

	flatID := registerProtobufSchema(t, url, "impressions-value", directProtoFlat)
	nestedID := registerProtobufSchema(t, url, "nested-value", directProtoNested)
	require.NotEqual(t, flatID, nestedID)

	impression := directTestMessageDescriptor(t, directProtoFlat, "Impression")
	click := directTestMessageDescriptor(t, directProtoFlat, "Click")
	nested := directTestMessageDescriptor(t, directProtoNested, "Impression")

	var flat, clicks, nestedMsgs [][]byte
	for i := range 200 {
		flat = append(flat, directFrame(flatID, []int{0}, directImpression(t, impression, i)))
		clicks = append(clicks, directFrame(flatID, []int{1}, directImpression(t, click, i)))
		nestedMsgs = append(nestedMsgs, directFrame(nestedID, []int{1}, directImpression(t, nested, i)))
	}

	tests := []struct {
		name     string
		payloads [][]byte
		direct   bool
		rows     int
	}{
		{"flat protobuf takes the direct path", flat, true, 200},
		{"second message type of the schema", clicks, true, 200},
		{"both message types in one batch", append(append([][]byte{}, flat[:20]...), clicks[:20]...), true, 40},
		{"nested schema takes the regular path", nestedMsgs, false, 200},
		{"unknown schema id fails the same way", [][]byte{directFrame(987654, []int{0}, nil)}, false, -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := newDirectTestPipelines(t, url, directDecodeConf, directParquetSchema)
			assert.Equal(t, test.rows, p.requireSameOutcome(t, test.payloads...))
			assert.Equal(t, test.direct, p.tookDirectPath(t, test.payloads...))
		})
	}
}
