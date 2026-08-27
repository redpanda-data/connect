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
	"errors"
	"fmt"
	"io/fs"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"
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
	var cErr error
	err = c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cErr = cache.Set(ctx, c.cacheKey, b, nil)
	})
	if err == nil {
		err = cErr
	}
	return err
}

func (c *checkpointCache) Load(ctx context.Context) (bson.Raw, error) {
	var cVal []byte
	var cErr error
	err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cVal, cErr = cache.Get(ctx, c.cacheKey)
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
	var resumeToken bson.Raw
	if err = bson.UnmarshalExtJSON(cVal, true, &resumeToken); err != nil {
		return nil, fmt.Errorf("%w: %w", errCorruptCheckpoint, err)
	}
	return resumeToken, nil
}

// Delete removes the stored checkpoint, so that the next start behaves as if one
// had never been written.
//
// A key that is already absent is success, since the caller's goal is that no
// checkpoint remains. Both spellings of that condition are accepted: the
// documented service.ErrKeyNotFound, and fs.ErrNotExist for cache
// implementations that surface a filesystem error instead (the `file` cache
// returns os.Remove's error unwrapped).
func (c *checkpointCache) Delete(ctx context.Context) error {
	var cErr error
	err := c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cErr = cache.Delete(ctx, c.cacheKey)
	})
	if err == nil {
		err = cErr
	}
	if errors.Is(err, service.ErrKeyNotFound) || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
