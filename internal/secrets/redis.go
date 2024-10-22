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

func newRedisSecretsLookup(ctx context.Context, logger *slog.Logger, url *url.URL) (LookupFn, error) {
	opts, err := redis.ParseURL(url.String())
	if err != nil {
		return nil, err
	}

	r := &redisSecretsClient{
		logger: logger,
		client: redis.NewClient(opts),
	}
	return r.lookup, nil
}
