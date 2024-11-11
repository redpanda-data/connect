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

package qdrant

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	qc "github.com/testcontainers/testcontainers-go/modules/qdrant"
)

const (
	collectionName = `redpanda`
	template       = `
output:
  label: 'qdrant'
  qdrant:
    grpc_host: 'localhost:$PORT'
    tls: {enabled: false}
    id: 'root = $POINT_ID'
    collection_name: $COLLECTION_NAME
    vector_mapping: 'root = $VECTOR'
    payload_mapping: 'root = $PAYLOAD'
`
)

func TestIntegrationQdrant(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	ctx := context.Background()
	qdrantContainer, err := qc.Run(ctx, "qdrant/qdrant:v1.10.1")
	require.NoError(t, err, "failed to start container")

	testCases := []struct {
		name    string
		pointID string
		vector  string
	}{
		{
			name:    "Test With default dense vector",
			pointID: `1`,
			vector:  `[0.352,0.532,0.532]`,
		},
		{
			name:    "Test With sparse vector",
			pointID: `2`,
			vector:  `{"some_sparse": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}}`,
		},
		{
			name:    "Test With multi vector",
			pointID: `3`,
			vector:  `{"some_multi": [[0.352,0.532,0.532],[0.352,0.532,0.532]]}`,
		},
		{
			name:    "Test With dense and sparse vector",
			pointID: `"465213dd-3f11-4534-8daf-9fedf203549a"`,
			vector:  `{"some_dense": [0.352,0.532,0.532],"some_sparse": {"indices": [23,325,532],"values": [0.352,0.532,0.532]}}`,
		},
	}

	containerPort, err := qdrantContainer.MappedPort(ctx, "6334/tcp")
	require.NoError(t, err, "failed to get container port")

	addr, err := qdrantContainer.GRPCEndpoint(ctx)
	require.NoError(t, err, "failed to get container grpc endpoint")

	payload := map[string]any{
		"content": "hello world",
		"str":     "str_value",
		"number":  42,
		"bool":    true,
		"array":   []any{13, "str"},
		"nested": map[string]any{
			"nested_str": "nested_str_value",
			"nested_num": 13,
		},
	}

	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err, "failed to marshal payload")

	err = setupCollection(ctx, addr, collectionName)
	require.NoError(t, err, "failed to setup collection")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			host, port, err := parseHostAndPort(addr)
			require.NoError(t, err, "failed to parse host and port")
			queryPoint := func(ctx context.Context, testID, messageID string) (string, []string, error) {
				client, err := qdrant.NewClient(&qdrant.Config{
					Host: host,
					Port: port,
				})
				require.NoError(t, err, "failed to create qdrant client")

				points, err := client.Get(ctx, &qdrant.GetPoints{
					CollectionName: collectionName,
					Ids:            []*qdrant.PointId{parsePointID(tc.pointID)},
					WithPayload:    qdrant.NewWithPayload(true),
				})

				require.NoError(t, err, "failed to get point")

				assert.Len(t, points, 1)

				point := points[0]

				err = assertPayloadStructure(t, point.Payload, payload)
				require.NoError(t, err, "failed to assert payload structure")

				return fmt.Sprintf(`{"content":"%v","id":%v}`, point.Payload["content"].GetStringValue(), messageID), nil, err
			}

			suite := integration.StreamTests(
				integration.StreamTestOutputOnlySendBatch(10, queryPoint),
				integration.StreamTestOutputOnlySendSequential(10, queryPoint),
			)
			suite.Run(
				t, template, integration.StreamTestOptPort(containerPort.Port()),
				integration.StreamTestOptVarSet("POINT_ID", tc.pointID),
				integration.StreamTestOptVarSet("COLLECTION_NAME", collectionName),
				integration.StreamTestOptVarSet("VECTOR", tc.vector),
				integration.StreamTestOptVarSet("PAYLOAD", string(payloadBytes)),
			)

		})
	}

	require.NoError(t, qdrantContainer.Terminate(ctx), "failed to terminate container")
}

func setupCollection(ctx context.Context, addr, collectionName string) error {

	host, port, err := parseHostAndPort(addr)
	if err != nil {
		return err
	}
	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	if err != nil {
		return err
	}

	err = client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			// Default unnamed vector
			// Created when using https://qdrant.tech/documentation/concepts/collections/#create-a-collection
			"": {
				Size:     3,
				Distance: qdrant.Distance_Cosine,
			},
			"some_dense": {
				Size:     3,
				Distance: qdrant.Distance_Cosine,
			},
			"some_multi": {
				Size:     3,
				Distance: qdrant.Distance_Cosine,
				MultivectorConfig: &qdrant.MultiVectorConfig{
					Comparator: qdrant.MultiVectorComparator_MaxSim,
				},
			},
		}),
		SparseVectorsConfig: qdrant.NewSparseVectorsConfig(map[string]*qdrant.SparseVectorParams{
			"some_sparse": {},
		}),
	})

	return err
}

func assertPayloadStructure(t *testing.T, actual map[string]*qdrant.Value, expected map[string]any) error {
	valueMap, err := qdrant.TryValueMap(expected)
	if err != nil {
		return err
	}

	for key, value := range valueMap {
		assert.Equal(t, actual[key], value)
	}

	return nil
}

func parsePointID(input string) *qdrant.PointId {
	// Try to convert the input string to a number
	if num, err := strconv.ParseUint(input, 10, 64); err == nil {
		return qdrant.NewIDNum(num)
	}

	// Remove the quotes from the input string
	uuid := strings.Trim(input, `"`)
	return qdrant.NewID(uuid)
}
