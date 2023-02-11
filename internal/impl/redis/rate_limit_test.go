package redis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedisRateLimitConfErrors(t *testing.T) {
	conf, err := redisRatelimitConfig().ParseYAML(`
url: redis://localhost:6379
count: -1
key: asdf`, nil)
	require.NoError(t, err)

	_, err = newRedisRatelimitFromConfig(conf)
	require.Error(t, err)

	_, err = redisRatelimitConfig().ParseYAML(`
url: redis://localhost:6379
interval: nope
key: asdf`, nil)
	require.NoError(t, err)

	_, err = newRedisRatelimitFromConfig(conf)
	require.Error(t, err)

	_, err = redisRatelimitConfig().ParseYAML(`key: asdf`, nil)
	require.Error(t, err)

	_, err = redisRatelimitConfig().ParseYAML(`url: redis://localhost:6379`, nil)
	require.Error(t, err)
}
