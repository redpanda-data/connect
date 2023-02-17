package redis

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationRedisRateLimit(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	ctx := context.Background()
	if err = pool.Retry(func() error {
		return client.Ping(ctx).Err()
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	defer client.Close()

	t.Run("testRedisRateLimitBasic", func(t *testing.T) {
		testRedisRateLimitBasic(t, urlStr)
	})

	t.Run("testRedisRateLimitRefresh", func(t *testing.T) {
		testRedisRateLimitRefresh(t, urlStr)
	})
}

func testRedisRateLimitBasic(t *testing.T, url string) {
	conf, err := redisRatelimitConfig().ParseYAML(`
key: rate_limit_basic
count: 10
interval: 1s
url: `+url, nil)
	require.NoError(t, err)

	rl, err := newRedisRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		period, err := rl.Access(ctx)
		require.NoError(t, err)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	period, err := rl.Access(ctx)
	require.NoError(t, err)
	if period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func testRedisRateLimitRefresh(t *testing.T, url string) {
	conf, err := redisRatelimitConfig().ParseYAML(`
key: rate_limit_refresh
count: 10
interval: 100ms
url: `+url, nil)
	require.NoError(t, err)

	rl, err := newRedisRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			period, err := rl.Access(ctx)
			require.NoError(t, err)
			if period > 0 {
				t.Errorf("Period above zero: %v", period)
			}
		}()
	}
	wg.Wait()

	period, err := rl.Access(ctx)
	require.NoError(t, err)
	if period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(150 * time.Millisecond)

	wg.Add(10)
	for i := 0; i < 10; i++ {
		i := i
		go func() {
			defer wg.Done()
			period, err := rl.Access(ctx)
			require.NoError(t, err)
			if period != 0 {
				t.Errorf("Rate limited on get %v", i)
			}
		}()
	}
	wg.Wait()

	period, err = rl.Access(ctx)
	require.NoError(t, err)
	if period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}
