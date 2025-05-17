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

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/redpanda-data/benthos/v4/public/service"
)

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

func (c *checkpointCache) Load(ctx context.Context) (resumeToken bson.Raw, err error) {
	var cVal []byte
	var cErr error
	err = c.resources.AccessCache(ctx, c.cacheName, func(cache service.Cache) {
		cVal, cErr = cache.Get(ctx, c.cacheKey)
	})
	if err == nil {
		err = cErr
	}
	if err == service.ErrKeyNotFound {
		return nil, nil
	}
	if err == nil {
		err = bson.UnmarshalExtJSON(cVal, true, &resumeToken)
	}
	return
}
