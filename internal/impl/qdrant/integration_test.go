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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	qc "github.com/testcontainers/testcontainers-go/modules/qdrant"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
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

func TestIntegrationQdrant_Output(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	ctx := t.Context()
	qdrantContainer, err := qc.Run(ctx, "qdrant/qdrant:v1.14.0")
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

func TestIntegrationQdrant_Processor(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	ctx := t.Context()
	qdrantContainer, err := qc.Run(ctx, "qdrant/qdrant:v1.14.0")
	require.NoError(t, err, "failed to start container")

	vectors := []any{
		[]any{0.352, 0.532, 0.532},
		map[string]any{"some_sparse": map[string]any{"indices": []any{23, 325, 532}, "values": []any{0.352, 0.532, 0.532}}},
		map[string]any{"some_dense": []any{0.352, 0.532, 0.532}, "some_sparse": map[string]any{"indices": []any{23, 325, 532}, "values": []any{0.352, 0.532, 0.532}}},
	}

	payloads := []map[string]any{
		{
			"city":  "London",
			"color": "red",
		},
		{
			"city":  "London",
			"color": "blue",
		},
		{
			"city":  "New York",
			"color": "blue",
		},
	}

	addr, err := qdrantContainer.GRPCEndpoint(ctx)
	require.NoError(t, err, "failed to get container grpc endpoint")

	host, port, err := parseHostAndPort(addr)
	require.NoError(t, err, "failed to parse host and port")

	err = setupCollection(ctx, addr, collectionName)
	require.NoError(t, err, "failed to setup collection")

	client, err := qdrant.NewClient(&qdrant.Config{
		Host: host,
		Port: port,
	})
	require.NoError(t, err, "failed to create qdrant client")
	var points []*qdrant.PointStruct
	for i, vector := range vectors {
		v, err := newVectors(vector)
		require.NoError(t, err, "failed to create vector")
		for j, payload := range payloads {
			points = append(points, &qdrant.PointStruct{
				Id:      qdrant.NewIDNum(uint64((i * len(payloads)) + j)),
				Payload: qdrant.NewValueMap(payload),
				Vectors: qdrant.NewVectorsMap(v),
			})
		}
	}
	wait := true
	_, err = client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
		Wait:           &wait,
		Ordering:       &qdrant.WriteOrdering{Type: qdrant.WriteOrderingType_Strong},
	})
	require.NoError(t, err, "failed to upsert point")

	builder := service.NewStreamBuilder()
	err = builder.AddProcessorYAML(strings.NewReplacer(
		"$PORT", strconv.Itoa(port),
		"$COLLECTION_NAME", collectionName,
	).Replace(`
qdrant:
  grpc_host: 'localhost:$PORT'
  collection_name: $COLLECTION_NAME
  vector_mapping: this.vector
  filter: this.filter
  payload_fields: ['city']
  payload_filter: exclude
  limit: 1`))
	require.NoError(t, err, "failed to create processor")
	produce, err := builder.AddProducerFunc()
	require.NoError(t, err, "failed to create producer")
	output := service.MessageBatch{}
	var mu sync.Mutex
	err = builder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		mu.Lock()
		defer mu.Unlock()
		output = append(output, m)
		return nil
	})
	require.NoError(t, err, "failed to create conusmer")
	stream, err := builder.Build()
	require.NoError(t, err, "failed to create stream")
	streamCtx, cancel := context.WithCancel(ctx)
	streamDone := make(chan any)
	go func() {
		err := stream.Run(streamCtx)
		if errors.Is(err, streamCtx.Err()) {
			err = nil
		}
		require.NoError(t, err)
		close(streamDone)
	}()
	err = produce(ctx, service.NewMessage([]byte(`{
		"vector": [0.352,0.532,0.532],
		"filter": {"must": [{"field":{"key": "color", "match": {"text": "red"}}}]}
	}`)))
	require.NoError(t, err, "failed to produce message")
	err = produce(ctx, service.NewMessage([]byte(`{
		"vector": {"some_sparse": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}},
		"filter": {
			"must": [{"has_id":{"has_id":[{"num": 8}]}}],
			"must_not": [{"field":{"key": "city", "match": {"text": "London"}}}]
		}
	}`)))
	require.NoError(t, err, "failed to produce message")
	cancel()
	<-streamDone

	expected := []string{
		`[{"id":{"num":"0"},"payload":{"color":{"stringValue":"red"}},"score":0.9999999}]`,
		`[{"id":{"num":"8"},"payload":{"color":{"stringValue":"blue"}},"score":0.689952}]`,
	}

	for i, m := range output {
		require.NoError(t, m.GetError(), "message had error")
		b, err := m.AsBytes()
		require.NoError(t, err, "failed to get message bytes")
		require.Equal(t, expected[i], string(b))
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
