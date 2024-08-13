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

	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type qdrantClient struct {
	pointsClient  pb.PointsClient
	serviceClient pb.QdrantClient
	connection    *grpc.ClientConn

	logger *service.Logger
}

func newQdrantClient(host, apiKey string, useTLS bool, config *tls.Config, logger *service.Logger) (*qdrantClient, error) {

	var tlsCredential credentials.TransportCredentials

	if !useTLS && apiKey != "" {
		logger.Warn("API key is set but TLS is not enabled. The API key will be sent in plaintext.")
		logger.Warn("May fail when using Qdrant cloud.")
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
		logger:        logger,
	}, nil
}

func (c *qdrantClient) Upsert(ctx context.Context, collectionName string, points []*pb.PointStruct) error {
	c.logger.Debugf("Upserting %d points to collection %s", len(points), collectionName)
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
	c.logger.Debug("Checking connection to Qdrant")
	_, err := c.serviceClient.HealthCheck(ctx, &pb.HealthCheckRequest{})

	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	return nil
}

func (c *qdrantClient) Close() error {
	c.logger.Debug("Closing connection to Qdrant")
	return c.connection.Close()
}

// Appends "api-key" to the metadata for authentication
func withAPIKeyInterceptor(apiKey string) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "api-key", apiKey)
		return invoker(newCtx, method, req, reply, cc, opts...)
	})
}
