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
	"crypto/tls"
	"fmt"
	"log"

	pb "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type qdrantClient struct {
	pointsClient  pb.PointsClient
	serviceClient pb.QdrantClient
	connection    *grpc.ClientConn
}

func newQdrantClient(host, apiKey string, useTLS bool, config *tls.Config) (*qdrantClient, error) {

	var tlsCredential credentials.TransportCredentials

	if !useTLS && apiKey != "" {
		log.Println("Warning: API key is set but TLS is not enabled. The API key will be sent in plaintext.")
		log.Println("May fail when using Qdrant cloud.")
	}

	if useTLS {
		tlsCredential = credentials.NewTLS(config)
	} else {
		tlsCredential = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(host, grpc.WithTransportCredentials(tlsCredential), withAPIKeyInterceptor(apiKey))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	return &qdrantClient{
		serviceClient: pb.NewQdrantClient(conn),
		pointsClient:  pb.NewPointsClient(conn),
		connection:    conn,
	}, nil
}

func (c *qdrantClient) Upsert(ctx context.Context, collectionName string, points []*pb.PointStruct) error {
	wait := true
	request := &pb.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
		Wait:           &wait,
	}
	_, err := c.pointsClient.Upsert(ctx, request)

	return err
}

func (c *qdrantClient) Connect(ctx context.Context) error {
	_, err := c.serviceClient.HealthCheck(ctx, &pb.HealthCheckRequest{})

	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	return nil
}

func (c *qdrantClient) Close() error {
	if c.connection != nil {
		return c.connection.Close()
	}
	return nil
}

// Appends "api-key" to the metadata for authentication
func withAPIKeyInterceptor(apiKey string) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "api-key", apiKey)
		return invoker(newCtx, method, req, reply, cc, opts...)
	})
}
