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

package pinecone

import (
	"context"
	"io"

	"github.com/pinecone-io/go-pinecone/pinecone"
)

// Interfaces for pinecone client to enable mocking
type (
	client interface {
		Index(host string) (indexClient, error)
	}
	indexClient interface {
		SetNamespace(namespace string)
		UpdateVector(ctx context.Context, req *pinecone.UpdateVectorRequest) error
		UpsertVectors(ctx context.Context, req []*pinecone.Vector) error
		DeleteVectorsByID(ctx context.Context, ids []string) error
		io.Closer
	}
)

type realClient struct {
	client *pinecone.Client
}

func (c *realClient) Index(host string) (indexClient, error) {
	i, err := c.client.Index(pinecone.NewIndexConnParams{
		Host: host,
	})
	if err != nil {
		return nil, err
	}
	return &realIndexClient{i}, nil
}

type realIndexClient struct {
	client *pinecone.IndexConnection
}

func (c *realIndexClient) SetNamespace(ns string) {
	c.client.Namespace = ns
}

func (c *realIndexClient) UpdateVector(ctx context.Context, req *pinecone.UpdateVectorRequest) error {
	return c.client.UpdateVector(ctx, req)
}

func (c *realIndexClient) UpsertVectors(ctx context.Context, req []*pinecone.Vector) error {
	_, err := c.client.UpsertVectors(ctx, req)
	return err
}

func (c *realIndexClient) DeleteVectorsByID(ctx context.Context, ids []string) error {
	return c.client.DeleteVectorsById(ctx, ids)
}

func (c *realIndexClient) Close() error {
	return c.client.Close()
}
