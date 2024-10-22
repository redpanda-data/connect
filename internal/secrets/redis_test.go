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
	"fmt"
	"log/slog"
	"net/url"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationRedis(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("redis", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	urlStr := fmt.Sprintf("redis://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		t.Fatal(err)
	}

	opts, err := redis.ParseURL(uri.String())
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(opts)

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return client.Ping(context.Background()).Err()
	}))

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	require.NoError(t, client.Set(ctx, "bar", "meow", time.Minute).Err())

	secretsLookup, err := parseSecretsLookupURN(ctx, slog.Default(), urlStr)
	require.NoError(t, err)

	v, exists := secretsLookup(ctx, "foo")
	assert.False(t, exists)
	assert.Equal(t, "", v)

	v, exists = secretsLookup(ctx, "bar")
	assert.True(t, exists)
	assert.Equal(t, "meow", v)
}
