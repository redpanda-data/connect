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

package couchbase

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/gocb/v2"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase/client"
)

// CacheConfig export couchbase Cache specification.
func CacheConfig() *service.ConfigSpec {
	return client.NewConfigSpec().
		// TODO Stable().
		Version("4.12.0").
		Summary(`Use a Couchbase instance as a cache.`).
		Field(service.NewDurationField("default_ttl").
			Description("An optional default TTL to set for items, calculated from the moment the item is cached.").
			Optional().
			Advanced())
}

func init() {
	service.MustRegisterCache("couchbase", CacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return NewCache(conf, mgr)
		},
	)
}

//------------------------------------------------------------------------------

// Cache stores or retrieves data from couchbase to be used as a cache
type Cache struct {
	*couchbaseClient

	ttl *time.Duration
}

// NewCache returns a Couchbase cache.
func NewCache(conf *service.ParsedConfig, _ *service.Resources) (*Cache, error) {
	cl, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	var ttl *time.Duration
	if conf.Contains("default_ttl") {
		ttlTmp, err := conf.FieldDuration("default_ttl")
		if err != nil {
			return nil, err
		}
		ttl = &ttlTmp
	}

	return &Cache{
		couchbaseClient: cl,
		ttl:             ttl,
	}, nil
}

// Get retrieve from cache.
func (c *Cache) Get(ctx context.Context, key string) (data []byte, err error) {
	out, err := c.collection.Get(key, &gocb.GetOptions{
		Context: ctx, // this may change in future gocb.
	})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, service.ErrKeyNotFound
		}
		return nil, err
	}

	err = out.Content(&data)
	return data, err
}

// Set update cache.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if ttl == nil {
		ttl = c.ttl // load default ttl
	}
	opts := &gocb.UpsertOptions{
		Context: ctx, // this may change in future gocb.
	}
	if ttl != nil {
		opts.Expiry = *ttl
	}
	_, err := c.collection.Upsert(key, value, opts)

	return err
}

// Add insert into cache.
func (c *Cache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if ttl == nil {
		ttl = c.ttl // load default ttl
	}
	opts := &gocb.InsertOptions{
		Context: ctx, // this may change in future gocb.
	}
	if ttl != nil {
		opts.Expiry = *ttl
	}
	_, err := c.collection.Insert(key, value, opts)

	if err != nil && errors.Is(err, gocb.ErrDocumentExists) {
		return service.ErrKeyAlreadyExists
	}

	return err
}

// Delete remove from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	_, err := c.collection.Remove(key, &gocb.RemoveOptions{
		Context: ctx, // this may change in future gocb.
	})

	return err
}
