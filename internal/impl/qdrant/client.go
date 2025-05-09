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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/qdrant/go-client/qdrant"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type qdrantClient struct {
	client *qdrant.Client

	logger *service.Logger
}

func newQdrantClient(host, apiKey string, useTLS bool, config *tls.Config, logger *service.Logger) (*qdrantClient, error) {
	hostName, portInt, err := parseHostAndPort(host)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host and port: %w", err)
	}

	client, err := qdrant.NewClient(&qdrant.Config{
		Host:      hostName,
		Port:      portInt,
		APIKey:    apiKey,
		UseTLS:    useTLS,
		TLSConfig: config,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Qdrant client: %w", err)
	}

	return &qdrantClient{
		client: client,
		logger: logger,
	}, nil
}

func parseHostAndPort(host string) (string, int, error) {
	splits := strings.Split(host, ":")
	if len(splits) != 2 {
		return "", 0, errors.New("invalid host format, expected 'host:port'")
	}

	portInt, err := strconv.Atoi(splits[1])
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse port: %w", err)
	}

	return splits[0], portInt, nil
}

func (c *qdrantClient) Upsert(ctx context.Context, collectionName string, points []*qdrant.PointStruct) error {
	c.logger.Debugf("Upserting %d points to collection %s", len(points), collectionName)
	wait := true
	request := &qdrant.UpsertPoints{
		CollectionName: collectionName,
		Points:         points,
		Wait:           &wait,
	}
	_, err := c.client.Upsert(ctx, request)

	return err
}

func (c *qdrantClient) Query(
	ctx context.Context,
	collectionName string,
	vectorName *string,
	vector *qdrant.VectorInput,
	payload *qdrant.WithPayloadSelector,
	filter *qdrant.Filter,
	limit uint64,
) ([]*qdrant.ScoredPoint, error) {
	request := &qdrant.QueryPoints{
		CollectionName: collectionName,
		Using:          vectorName,
		Query: &qdrant.Query{
			Variant: &qdrant.Query_Nearest{
				Nearest: vector,
			},
		},
		Filter:      filter,
		WithPayload: payload,
		Limit:       &limit,
	}
	return c.client.Query(ctx, request)
}

func (c *qdrantClient) Connect(ctx context.Context) error {
	c.logger.Debug("Checking connection to Qdrant")
	_, err := c.client.HealthCheck(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Qdrant: %w", err)
	}

	return nil
}

func (c *qdrantClient) Close() error {
	c.logger.Debug("Closing connection to Qdrant")
	return c.client.Close()
}
