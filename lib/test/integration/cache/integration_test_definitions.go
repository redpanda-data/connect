package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func integrationTestOpenClose() testDefinition {
	return namedTest(
		"can open and close",
		func(t *testing.T, env *testEnvironment) {
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

func integrationTestMissingKey() testDefinition {
	return namedTest(
		"return consistent error on missing key",
		func(t *testing.T, env *testEnvironment) {
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

func integrationTestDoubleAdd() testDefinition {
	return namedTest(
		"add with duplicate key fails",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			cache := initCache(t, env)
			t.Cleanup(func() {
				closeCache(t, cache)
			})

			require.NoError(t, cache.Add("addkey", []byte("first")))
			time.Sleep(time.Second * 10)
			assert.EqualError(t, cache.Add("addkey", []byte("second")), "key already exists")

			res, err := cache.Get("addkey")
			require.NoError(t, err)
			assert.Equal(t, "first", string(res))
		},
	)
}

func integrationTestDelete() testDefinition {
	return namedTest(
		"can set and delete keys",
		func(t *testing.T, env *testEnvironment) {
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

func integrationTestGetAndSet(n int) testDefinition {
	return namedTest(
		"can get and set",
		func(t *testing.T, env *testEnvironment) {
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
