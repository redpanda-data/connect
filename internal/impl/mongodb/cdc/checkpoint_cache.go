// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// errCorruptCheckpoint marks a checkpoint whose stored bytes are not decodable
// as a resume token. Unlike a cache that is merely unreachable this can never
// succeed on a retry, so callers treat it as recoverable-by-clearing rather than
// as a reason to keep failing Connect.
var errCorruptCheckpoint = errors.New("stored checkpoint could not be decoded")

type checkpointCache struct {
	resources *service.Resources
	cacheName string
	cacheKey  string
}

func (c *checkpointCache) Store(ctx context.Context, resumeToken bson.Raw) error {
	b, err := bson.MarshalExtJSON(resumeToken, true, false)
	if err != nil {
		return err
	}
	return setCacheKey(ctx, c.resources, c.cacheName, c.cacheKey, b)
}

func (c *checkpointCache) Load(ctx context.Context) (bson.Raw, error) {
	cVal, err := getCacheKey(ctx, c.resources, c.cacheName, c.cacheKey)
	if cVal == nil || err != nil {
		return nil, err
	}
	var resumeToken bson.Raw
	if err = bson.UnmarshalExtJSON(cVal, true, &resumeToken); err != nil {
		return nil, fmt.Errorf("%w: %w", errCorruptCheckpoint, err)
	}
	return resumeToken, nil
}

// Delete removes the stored checkpoint, so that the next start behaves as if one
// had never been written.
func (c *checkpointCache) Delete(ctx context.Context) error {
	return deleteCacheKey(ctx, c.resources, c.cacheName, c.cacheKey)
}

// incStateCache stores the incremental snapshot's progress, in the same cache
// as the resume token but under its own key, so the token's format is
// untouched and either can be cleared alone.
type incStateCache struct {
	resources *service.Resources
	cacheName string
	cacheKey  string
}

// Store writes state, already JSON-encoded by the caller.
func (c *incStateCache) Store(ctx context.Context, state []byte) error {
	return setCacheKey(ctx, c.resources, c.cacheName, c.cacheKey, state)
}

// Load returns the stored state, or nil when there is none. An entry that does
// not decode, or was written with another State layout, wraps
// errCorruptCheckpoint: retrying cannot fix it.
func (c *incStateCache) Load(ctx context.Context) (*incrementalsnapshot.State, error) {
	b, err := getCacheKey(ctx, c.resources, c.cacheName, c.cacheKey)
	if b == nil || err != nil {
		return nil, err
	}
	var state incrementalsnapshot.State
	if err := json.Unmarshal(b, &state); err != nil {
		if errors.Is(err, incrementalsnapshot.ErrUnsupportedStateVersion) {
			// Written by another build, most likely a newer one being rolled
			// back: valid, just not readable here. Clearing it would also
			// clear a good stream position, so fail and keep both.
			return nil, fmt.Errorf("incremental snapshot progress under %s was written by an incompatible version: %w", c.cacheKey, err)
		}
		return nil, fmt.Errorf("%w: incremental snapshot state: %w", errCorruptCheckpoint, err)
	}
	return &state, nil
}

// Delete removes the stored state.
func (c *incStateCache) Delete(ctx context.Context) error {
	return deleteCacheKey(ctx, c.resources, c.cacheName, c.cacheKey)
}

func setCacheKey(ctx context.Context, res *service.Resources, cacheName, key string, value []byte) error {
	var cErr error
	err := res.AccessCache(ctx, cacheName, func(cache service.Cache) {
		cErr = cache.Set(ctx, key, value, nil)
	})
	if err == nil {
		err = cErr
	}
	return err
}

// getCacheKey returns nil, nil for a key that is not set.
func getCacheKey(ctx context.Context, res *service.Resources, cacheName, key string) ([]byte, error) {
	var cVal []byte
	var cErr error
	err := res.AccessCache(ctx, cacheName, func(cache service.Cache) {
		cVal, cErr = cache.Get(ctx, key)
	})
	if err == nil {
		err = cErr
	}
	if errors.Is(err, service.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cVal, nil
}

// deleteCacheKey removes key from the cache.
//
// A key that is already absent is success, since the caller's goal is that no
// entry remains. Both spellings of that condition are accepted: the
// documented service.ErrKeyNotFound, and fs.ErrNotExist for cache
// implementations that surface a filesystem error instead (the `file` cache
// returns os.Remove's error unwrapped).
func deleteCacheKey(ctx context.Context, res *service.Resources, cacheName, key string) error {
	var cErr error
	err := res.AccessCache(ctx, cacheName, func(cache service.Cache) {
		cErr = cache.Delete(ctx, key)
	})
	if err == nil {
		err = cErr
	}
	if errors.Is(err, service.ErrKeyNotFound) || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
