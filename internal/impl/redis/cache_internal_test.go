package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/impl/redis/redismock"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisCache(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label      string
		defaultTTL time.Duration
		prefix     string
		prepare    func(*redismock.RedisCacheAdaptor)
		verify     func(*testing.T, service.Cache)
	}{
		{
			label: "should get from redis using cache adaptor",
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				rca.On("Get", context.Background(), "foo").Return([]byte("value"), true, nil)
				rca.On("Get", context.Background(), "bar").Return(nil, false, nil)
				rca.On("Get", context.Background(), "baz").Return(nil, false, errors.New("ops"))
			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()

				{
					value, err := c.Get(context.Background(), "foo")
					assert.NoError(t, err)
					assert.Equal(t, []byte("value"), value)
				}

				{
					value, err := c.Get(context.Background(), "bar")
					assert.EqualError(t, err, "key does not exist")
					assert.Empty(t, value)
				}

				{
					value, err := c.Get(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
					assert.Empty(t, value)
				}
			},
		},
		{
			label:      "should add on redis using cache adaptor",
			defaultTTL: 5 * time.Minute,
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				var (
					noTTL      time.Duration
					defaultTTL = 5 * time.Minute
				)

				rca.On("Add", context.Background(), "foo", []byte("value"), defaultTTL).Return(true, nil)
				rca.On("Add", context.Background(), "bar", []byte("value"), 10*time.Minute).Return(false, nil)
				rca.On("Add", context.Background(), "baz", []byte("value"), noTTL).Return(false, errors.New("ops"))
			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()

				{
					err := c.Add(context.Background(), "foo", []byte("value"), nil)
					assert.NoError(t, err)
				}

				{
					ttl := 10 * time.Minute
					err := c.Add(context.Background(), "bar", []byte("value"), &ttl)
					assert.EqualError(t, err, "key already exists")
				}

				{
					var noTTL time.Duration
					err := c.Add(context.Background(), "baz", []byte("value"), &noTTL)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:  "should set on redis using cache adaptor",
			prefix: "prefix_",
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				var noTTL time.Duration

				rca.On("Set", context.Background(), "prefix_foo", []byte("value"), noTTL).Return(nil)
				rca.On("Set", context.Background(), "prefix_bar", []byte("value"), 10*time.Minute).Return(errors.New("ops"))
			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()

				{
					err := c.Set(context.Background(), "foo", []byte("value"), nil)
					assert.NoError(t, err)
				}
				{
					ttl := 10 * time.Minute
					err := c.Set(context.Background(), "bar", []byte("value"), &ttl)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label: "should delete on redis using cache adaptor",
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				rca.On("Delete", context.Background(), "foo").Return(nil)
				rca.On("Delete", context.Background(), "bar").Return(errors.New("ops"))

			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()
				{
					err := c.Delete(context.Background(), "foo")
					assert.NoError(t, err)
				}
				{
					err := c.Delete(context.Background(), "bar")
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:      "should support SetMulti, but delegate to several calls to Set method",
			defaultTTL: 5 * time.Minute,
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				var (
					noTTL      time.Duration
					defaultTTL = 5 * time.Minute
				)

				rca.On("Set", context.Background(), "foo", []byte("value"), defaultTTL).Return(nil)
				rca.On("Set", context.Background(), "bar", []byte("value"), 10*time.Minute).Return(nil)
				rca.On("Set", context.Background(), "baz", []byte("value"), noTTL).Return(nil)
				rca.On("Set", context.Background(), "bam", []byte("value"), defaultTTL).Return(errors.New("ops"))
			},
			verify: func(t *testing.T, c service.Cache) {
				rms, ok := c.(redisMultiSetter)
				require.True(t, ok)

				var (
					noTTL      time.Duration
					tenMinutes = 10 * time.Minute
				)

				items := []service.CacheItem{
					{
						Key:   "foo",
						Value: []byte("value"),
					},
					{
						Key:   "bar",
						Value: []byte("value"),
						TTL:   &tenMinutes,
					},
					{
						Key:   "baz",
						Value: []byte("value"),
						TTL:   &noTTL,
					},
					{
						Key:   "bam", // will return error and interrupt the loop
						Value: []byte("value"),
					},
					{
						Key:   "should not be called",
						Value: []byte("value"),
					},
				}

				err := rms.SetMulti(context.Background(), items...)
				assert.EqualError(t, err, "ops")
			},
		},
		{
			label: "should be able to close with success",
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				rca.On("Close").Return(nil)
			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()

				err := c.Close(context.Background())
				assert.NoError(t, err)
			},
		},
		{
			label: "should be able to close with failure",
			prepare: func(rca *redismock.RedisCacheAdaptor) {
				rca.On("Close").Return(errors.New("ops"))
			},
			verify: func(t *testing.T, c service.Cache) {
				t.Helper()

				err := c.Close(context.Background())
				assert.EqualError(t, err, "ops")
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			cacheAdaptor := redismock.NewRedisCacheAdaptor(t)

			retriesDefaults := backoff.NewExponentialBackOff()
			retriesDefaults.InitialInterval = time.Millisecond * 500
			retriesDefaults.MaxInterval = time.Second
			retriesDefaults.MaxElapsedTime = time.Second * 5

			tc.prepare(cacheAdaptor)

			cache := newRedisCache(tc.defaultTTL, tc.prefix, cacheAdaptor, retriesDefaults)

			tc.verify(t, cache)
		})
	}
}

func TestRedisCacheSetMulti(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label  string
		err    error
		errMsg string
	}{
		{
			label: "should call set multi with success",
		},
		{
			label:  "should call set multi with failure",
			err:    errors.New("ops"),
			errMsg: "ops",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			var (
				noTTL      time.Duration
				defaultTTL = 5 * time.Minute
				tenMinutes = 10 * time.Minute
			)

			cacheAdaptor := redismock.NewRedisMultiCacheAdaptor(t)

			cacheAdaptor.On("SetMulti", context.Background(),
				service.CacheItem{
					Key:   "foo",
					Value: []byte("value"),
				},
				service.CacheItem{
					Key:   "bar",
					Value: []byte("value"),
					TTL:   &tenMinutes,
				},
				service.CacheItem{
					Key:   "baz",
					Value: []byte("value"),
					TTL:   &noTTL,
				}).Return(tc.err)

			retriesDefaults := backoff.NewExponentialBackOff()
			retriesDefaults.InitialInterval = time.Millisecond * 500
			retriesDefaults.MaxInterval = time.Second
			retriesDefaults.MaxElapsedTime = time.Second * 5

			cache := newRedisCache(defaultTTL, "", cacheAdaptor, retriesDefaults)

			items := []service.CacheItem{
				{
					Key:   "foo",
					Value: []byte("value"),
				},
				{
					Key:   "bar",
					Value: []byte("value"),
					TTL:   &tenMinutes,
				},
				{
					Key:   "baz",
					Value: []byte("value"),
					TTL:   &noTTL,
				},
			}

			err := cache.SetMulti(context.Background(), items...)
			if tc.errMsg != "" {
				assert.EqualError(t, err, tc.errMsg)

				return
			}

			assert.NoError(t, err)
		})
	}
}
