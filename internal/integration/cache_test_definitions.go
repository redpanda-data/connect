package integration

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
)

type conf struct {
	key   string
	value []byte
}

func (c *conf) SetDefaults(key string, value []byte) {
	c.key = key
	c.value = value
}

type Option func(*conf)

func WithKey(key string) Option {
	return func(c *conf) {
		c.key = key
	}
}

func WithValue(value []byte) Option {
	return func(c *conf) {
		c.value = value
	}
}

// CacheTestOpenClose checks that the cache can be started, an item added, and
// then stopped.
func CacheTestOpenClose(opts ...Option) CacheTestDefinition {
	var c conf

	c.SetDefaults("foo", []byte("bar"))

	for _, opt := range opts {
		opt(&c)
	}

	return namedCacheTest(
		"can open and close",
		func(t *testing.T, env *cacheTestEnvironment) {
			key, value := c.key, c.value

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add(env.ctx, key, value, nil))

			res, err := cache.Get(env.ctx, key)
			require.NoError(t, err)
			assert.Equal(t, value, res)
		},
	)
}

// CacheTestMissingKey checks that we get an error on missing key.
func CacheTestMissingKey() CacheTestDefinition {
	return namedCacheTest(
		"return consistent error on missing key",
		func(t *testing.T, env *cacheTestEnvironment) {
			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			_, err := cache.Get(env.ctx, "missingkey")
			assert.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestDoubleAdd ensures that a double add returns an error.
func CacheTestDoubleAdd(opts ...Option) CacheTestDefinition {
	var c conf

	c.SetDefaults("addkey", []byte("first"))

	for _, opt := range opts {
		opt(&c)
	}

	return namedCacheTest(
		"add with duplicate key fails",
		func(t *testing.T, env *cacheTestEnvironment) {
			key, firstValue, secondValue := c.key, c.value, []byte("second")
			require.NotEqual(t, secondValue, firstValue, "first value should be different than the first")

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add(env.ctx, key, firstValue, nil))

			assert.Eventually(t, func() bool {
				return errors.Is(cache.Add(env.ctx, key, secondValue, nil), component.ErrKeyAlreadyExists)
			}, time.Minute, time.Second)

			res, err := cache.Get(env.ctx, key)
			require.NoError(t, err)
			assert.Equal(t, firstValue, res)
		},
	)
}

// CacheTestDelete checks that deletes work.
func CacheTestDelete(opts ...Option) CacheTestDefinition {
	var c conf

	c.SetDefaults("addkey", []byte("first"))

	for _, opt := range opts {
		opt(&c)
	}

	return namedCacheTest(
		"can set and delete keys",
		func(t *testing.T, env *cacheTestEnvironment) {
			key, value := c.key, c.value

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Set(env.ctx, key, value, nil), "expect no error to add key")

			res, err := cache.Get(env.ctx, key)
			require.NoError(t, err, "expect no error to fetch key")
			assert.Equal(t, value, res, "compare value")

			require.NoError(t, cache.Delete(env.ctx, key), "should have no error to delete key")

			_, err = cache.Get(env.ctx, key)
			require.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestGetAndSet checks that we can set and then get n items.
func CacheTestGetAndSet(n int, opts ...Option) CacheTestDefinition {
	var c conf

	c.SetDefaults("key:$i", []byte("value:$i"))

	for _, opt := range opts {
		opt(&c)
	}

	return namedCacheTest(
		"can get and set",
		func(t *testing.T, env *cacheTestEnvironment) {
			keyTemplate, valueTemplate := c.key, c.value

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			entries := map[string][]byte{}

			for i := 0; i < n; i++ {
				index := strconv.Itoa(i)
				key := strings.ReplaceAll(keyTemplate, "$i", index)                   // fmt.Sprintf("key:%v", i)
				value := bytes.ReplaceAll(valueTemplate, []byte("$i"), []byte(index)) // fmt.Sprintf("value:%v", i)

				entries[key] = value
			}

			for key, value := range entries {
				require.NoError(t, cache.Set(env.ctx, key, value, nil))
			}

			for key, value := range entries {
				res, err := cache.Get(env.ctx, key)
				require.NoError(t, err)
				assert.Equal(t, value, res)
			}
		},
	)
}
