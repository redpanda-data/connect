package integration

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CacheTestOpenClose checks that the cache can be started, an item added, and
// then stopped.
func CacheTestOpenClose() CacheTestDefinition {
	return namedCacheTest(
		"can open and close",
		func(t *testing.T, env *cacheTestEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add("foo", []byte("bar")))

			res, err := cache.Get("foo")
			require.NoError(t, err)
			assert.Equal(t, "bar", string(res))
		},
	)
}

// CacheTestMissingKey checks that we get an error on missing key.
func CacheTestMissingKey() CacheTestDefinition {
	return namedCacheTest(
		"return consistent error on missing key",
		func(t *testing.T, env *cacheTestEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			_, err := cache.Get("missingkey")
			assert.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestDoubleAdd ensures that a double add returns an error.
func CacheTestDoubleAdd() CacheTestDefinition {
	return namedCacheTest(
		"add with duplicate key fails",
		func(t *testing.T, env *cacheTestEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add("addkey", []byte("first")))

			assert.Eventually(t, func() bool {
				return errors.Is(cache.Add("addkey", []byte("second")), types.ErrKeyAlreadyExists)
			}, time.Minute, time.Second)

			res, err := cache.Get("addkey")
			require.NoError(t, err)
			assert.Equal(t, "first", string(res))
		},
	)
}

// CacheTestDelete checks that deletes work.
func CacheTestDelete() CacheTestDefinition {
	return namedCacheTest(
		"can set and delete keys",
		func(t *testing.T, env *cacheTestEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add("addkey", []byte("first")))

			res, err := cache.Get("addkey")
			require.NoError(t, err)
			assert.Equal(t, "first", string(res))

			require.NoError(t, cache.Delete("addkey"))

			_, err = cache.Get("addkey")
			require.EqualError(t, err, "key does not exist")
		},
	)
}

// CacheTestGetAndSet checks that we can set and then get n items.
func CacheTestGetAndSet(n int) CacheTestDefinition {
	return namedCacheTest(
		"can get and set",
		func(t *testing.T, env *cacheTestEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			for i := 0; i < n; i++ {
				key := fmt.Sprintf("key:%v", i)
				value := fmt.Sprintf("value:%v", i)
				require.NoError(t, cache.Set(key, []byte(value)))
			}

			for i := 0; i < n; i++ {
				key := fmt.Sprintf("key:%v", i)
				value := fmt.Sprintf("value:%v", i)

				res, err := cache.Get(key)
				require.NoError(t, err)
				assert.Equal(t, value, string(res))
			}
		},
	)
}
