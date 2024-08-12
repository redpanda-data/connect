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
	"fmt"
	"testing"

	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	collectionName = `redpanda`
	template       = `
output:
  label: 'qdrant'
  qdrant:
    grpc_host: 'localhost:$PORT'
    tls: {enabled: false}
    id: 'root.id = $POINT_ID'
    collection_name: $COLLECTION_NAME
    vector_mapping: 'root = $VECTOR'
    payload_mapping: 'root = $PAYLOAD'
`
)

func TestIntegrationQdrant(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	ctx := context.Background()
	qdrantContainer, err := qdrant.Run(ctx, "qdrant/qdrant:v1.10.1")
	require.NoError(t, err, "failed to start container")

	testCases := []struct {
		name    string
		pointID string
		vector  string
		payload string
	}{
		{
			name:    "Test With default dense vector",
			pointID: `1`,
			vector:  `[0.352,0.532,0.532]`,
			payload: `{ "content": this.content, "some_number": 42 }`,
		},
		{
			name:    "Test With sparse vector",
			pointID: `2`,
			vector:  `{"some_sparse": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}}`,
			payload: `{"content":"test payload"}`,
		},
		{
			name:    "Test With multi vector",
			pointID: `3`,
			vector:  `{"some_multi": [[0.352,0.532,0.532],[0.352,0.532,0.532]]}`,
			payload: `{"content":"test payload"}`,
		},
		{
			name:    "Test With dense and sparse vector",
			pointID: `"465213dd-3f11-4534-8daf-9fedf203549a"`,
			vector:  `{"some_dense": [0.352,0.532,0.532],"some_sparse": {"indices": [23,325,532],"values": [0.352,0.532,0.532]}}`,
			payload: `{"content":"test payload"}`,
		},
	}

	containerPort, err := qdrantContainer.MappedPort(ctx, "6334/tcp")
	require.NoError(t, err, "failed to get container port")

	err = setupCollection(ctx, fmt.Sprintf("localhost:%v", containerPort.Port()), collectionName)
	require.NoError(t, err, "failed to setup collection")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			defaultQuery := func(ctx context.Context, testID, messageID string) (string, []string, error) {
				return fmt.Sprintf(`{"content":"%v","id":%v}`, "hello world", messageID), nil, err
			}

			suite := integration.StreamTests(
				// Is is possible to test output only without a `getFn GetMessageFunc` arg?
				integration.StreamTestOutputOnlySendBatch(10, defaultQuery),
			)
			suite.Run(
				t, template, integration.StreamTestOptPort(containerPort.Port()),
				integration.StreamTestOptVarSet("POINT_ID", tc.pointID),
				integration.StreamTestOptVarSet("COLLECTION_NAME", collectionName),
				integration.StreamTestOptVarSet("VECTOR", tc.vector),
				integration.StreamTestOptVarSet("PAYLOAD", tc.payload),
			)
		})
	}

	require.NoError(t, qdrantContainer.Terminate(ctx), "failed to terminate container")
}

func setupCollection(ctx context.Context, host, collectionName string) error {

	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return err
	}

	collectionsClient := pb.NewCollectionsClient(conn)

	_, err = collectionsClient.Create(ctx, &pb.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: &pb.VectorsConfig{
			Config: &pb.VectorsConfig_ParamsMap{
				ParamsMap: &pb.VectorParamsMap{
					Map: map[string]*pb.VectorParams{
						// Default unnamed vector
						// Created when using https://qdrant.tech/documentation/concepts/collections/#create-a-collection
						"": {
							Size:     3,
							Distance: pb.Distance_Cosine,
						},
						"some_dense": {
							Size:     3,
							Distance: pb.Distance_Cosine,
						},
						"some_multi": {
							Size:     3,
							Distance: pb.Distance_Cosine,
							MultivectorConfig: &pb.MultiVectorConfig{
								Comparator: pb.MultiVectorComparator_MaxSim,
							},
						},
					},
				},
			},
		},
		SparseVectorsConfig: &pb.SparseVectorConfig{
			Map: map[string]*pb.SparseVectorParams{
				"some_sparse": {},
			},
		},
	})

	return err
}
