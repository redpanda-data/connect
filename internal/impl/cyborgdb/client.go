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

package cyborgdb

import (
	"context"
	"fmt"
	"io"

	"github.com/cyborginc/cyborgdb-go"
)

// Interfaces for cyborgdb client to enable mocking
type (
	client interface {
		ListIndexes(ctx context.Context) ([]string, error)
		CreateIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error)
		GetIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error)
	}
	
	indexClient interface {
		Upsert(ctx context.Context, items []cyborgdb.VectorItem) error
		Delete(ctx context.Context, ids []string) error
		io.Closer
	}
)

type cyborgdbClient struct {
	client *cyborgdb.Client
}

func (c *cyborgdbClient) ListIndexes(ctx context.Context) ([]string, error) {
	return c.client.ListIndexes(ctx)
}

func (c *cyborgdbClient) CreateIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error) {
	// Create index with IVFFlat configuration - CyborgDB will auto-detect dimension
	params := &cyborgdb.CreateIndexParams{
		IndexName:   indexName,
		IndexKey:    indexKey,
		IndexConfig: cyborgdb.IndexIVFFlat(0),
	}

	return c.client.CreateIndex(ctx, params)
}

func (c *cyborgdbClient) GetIndex(ctx context.Context, indexName string, indexKey []byte) (*cyborgdb.EncryptedIndex, error) {
	return c.client.LoadIndex(ctx, indexName, indexKey)
}

type cyborgdbEncryptedIndex struct {
	index *cyborgdb.EncryptedIndex
}

func (c *cyborgdbEncryptedIndex) Upsert(ctx context.Context, items []cyborgdb.VectorItem) error {
	return c.index.Upsert(ctx, items)
}

func (c *cyborgdbEncryptedIndex) Delete(ctx context.Context, ids []string) error {
	return c.index.Delete(ctx, ids)
}

func (c *cyborgdbEncryptedIndex) Close() error {
	return nil
}

func isIndexNotFound(err error) bool {
	if err == nil {
		return false
	}
	return fmt.Sprintf("%v", err) == "index not found" || fmt.Sprintf("%v", err) == "404 Not Found"
}