// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package secrets

import (
	"context"
	"errors"
	"log/slog"
	"net/url"

	"github.com/redis/go-redis/v9"
)

type redisSecretsClient struct {
	logger *slog.Logger
	client *redis.Client
}

func (r *redisSecretsClient) lookup(ctx context.Context, key string) (string, bool) {
	res, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			// An error that isn't due to key-not-found gets logged
			r.logger.With("error", err, "key", key).Error("Failed to look up secret")
		}
		return "", false
	}
	return res, true
}

func (r *redisSecretsClient) exists(ctx context.Context, key string) bool {
	_, found := r.lookup(ctx, key)
	return found
}

func newRedisSecretsLookup(ctx context.Context, logger *slog.Logger, url *url.URL) (LookupFn, ExistsFn, error) {
	opts, err := redis.ParseURL(url.String())
	if err != nil {
		return nil, nil, err
	}

	r := &redisSecretsClient{
		logger: logger,
		client: redis.NewClient(opts),
	}
	return r.lookup, r.exists, nil
}
