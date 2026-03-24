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
	"fmt"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationRedis(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctr, err := testcontainers.Run(t.Context(), "redis:latest",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	redisPort, err := ctr.MappedPort(t.Context(), "6379/tcp")
	require.NoError(t, err)

	urlStr := fmt.Sprintf("redis://localhost:%v", redisPort.Port())
	uri, err := url.Parse(urlStr)
	require.NoError(t, err)

	opts, err := redis.ParseURL(uri.String())
	require.NoError(t, err)

	client := redis.NewClient(opts)

	require.Eventually(t, func() bool {
		return client.Ping(t.Context()).Err() == nil
	}, 30*time.Second, time.Second)

	ctx, done := context.WithTimeout(t.Context(), time.Minute)
	defer done()

	require.NoError(t, client.Set(ctx, "bar", "meow", time.Minute).Err())

	secretsLookup, err := parseSecretsLookupURN(ctx, slog.Default(), urlStr)
	require.NoError(t, err)

	v, exists := secretsLookup(ctx, "foo")
	assert.False(t, exists)
	assert.Empty(t, v)

	v, exists = secretsLookup(ctx, "bar")
	assert.True(t, exists)
	assert.Equal(t, "meow", v)
}
