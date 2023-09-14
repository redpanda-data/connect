package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/benthosdev/benthos/v4/internal/impl/redis"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/redismock"
	"github.com/benthosdev/benthos/v4/public/service"

	redis_client "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestCRUDRedisAdaptor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		prepare func(*redismock.RedisCRUD)

		verify func(t *testing.T, adaptor redis.RedisCacheAdaptor)
	}{
		{
			label: "adaptor.Get should call 'Get' from inner client with success",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.StringCmd

				cmd.SetVal("bar")

				rmi.On("Get", context.Background(), "foo").Return(&cmd, nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				v, found, err := adaptor.Get(context.Background(), "foo")
				assert.NoError(t, err)
				assert.True(t, found)
				assert.Equal(t, []byte("bar"), v)
			},
		},
		{
			label: "adaptor.Get should call 'Get' from inner client with error 'service.ErrKeyNotFound'",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.StringCmd

				cmd.SetErr(redis_client.Nil)

				rmi.On("Get", context.Background(), "foo").Return(&cmd, nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				v, found, err := adaptor.Get(context.Background(), "foo")
				assert.NoError(t, err)
				assert.False(t, found)
				assert.Empty(t, v)
			},
		},
		{
			label: "adaptor.Get should call 'Get' from inner client with failure",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.StringCmd

				cmd.SetErr(errors.New("ops"))

				rmi.On("Get", context.Background(), "foo").Return(&cmd, nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				v, found, err := adaptor.Get(context.Background(), "foo")
				assert.EqualError(t, err, "ops")
				assert.False(t, found)
				assert.Empty(t, v)
			},
		},
		{
			label: "adaptor.Add should call 'SetNX' from inner client with success",
			prepare: func(rmi *redismock.RedisCRUD) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rmi.On("SetNX", context.Background(), "foo", []byte("bar"), 1*time.Minute).Return(&cmd)
				}

				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rmi.On("SetNX", context.Background(), "baz", []byte("bam"), 1*time.Minute).Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				{
					ok, err := adaptor.Add(context.Background(), "foo", []byte("bar"), 1*time.Minute)
					assert.NoError(t, err)
					assert.True(t, ok)
				}

				{
					ok, err := adaptor.Add(context.Background(), "baz", []byte("bam"), 1*time.Minute)
					assert.NoError(t, err)
					assert.False(t, ok)
				}
			},
		},
		{
			label: "adaptor.Set should call 'Set' from inner client with success",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.StatusCmd

				rmi.On("Set", context.Background(), "foo", []byte("bar"), 1*time.Minute).Return(&cmd)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Set(context.Background(), "foo", []byte("bar"), 1*time.Minute)
				assert.NoError(t, err)
			},
		},
		{
			label: "adaptor.Set should call 'Set' from inner client with failure",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.StatusCmd

				cmd.SetErr(errors.New("ops"))

				rmi.On("Set", context.Background(), "foo", []byte("bar"), 1*time.Minute).Return(&cmd)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Set(context.Background(), "foo", []byte("bar"), 1*time.Minute)
				assert.EqualError(t, err, "ops")
			},
		},
		{
			label: "adaptor.Delete should call 'Del' from inner client with success",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.IntCmd

				rmi.On("Del", context.Background(), "foo").Return(&cmd)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.NoError(t, err)
			},
		},
		{
			label: "adaptor.Delete should call 'Del' from inner client with failure",
			prepare: func(rmi *redismock.RedisCRUD) {
				var cmd redis_client.IntCmd

				cmd.SetErr(errors.New("ops"))

				rmi.On("Del", context.Background(), "foo").Return(&cmd)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.EqualError(t, err, "ops")
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with success",
			prepare: func(rmi *redismock.RedisCRUD) {
				rmi.On("Close").Return(nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.NoError(t, err)
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with failure",
			prepare: func(rmi *redismock.RedisCRUD) {
				rmi.On("Close").Return(errors.New("ops"))
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.EqualError(t, err, "ops")
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			client := redismock.NewRedisCRUD(t)

			tc.prepare(client)

			adaptor := redis.NewCRUDRedisCacheAdaptor(client)

			tc.verify(t, adaptor)
		})
	}
}

func TestBloomFilterRedisAdaptor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		opts []redis.AdaptorOption

		prepare func(*redismock.RedisBloomFilter)

		verify func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor)
	}{
		{
			label: "adaptor.Add should call 'BFAdd' from inner client",
			opts: []redis.AdaptorOption{
				redis.WithFilterKeyTemplate("other-bf-benthos-%Y%m%d%H%M%S"),
			},
			prepare: func(rbf *redismock.RedisBloomFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbf.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbf.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbf.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					ok, err := adaptor.Add(context.Background(), "foo", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
					assert.True(t, ok)
				}
				{
					ok, err := adaptor.Add(context.Background(), "bar", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
					assert.False(t, ok)
				}
				{
					ok, err := adaptor.Add(context.Background(), "baz", []byte("t"), 1*time.Minute)
					assert.EqualError(t, err, "ops")
					assert.False(t, ok)
				}
			},
		},
		{
			label: "adaptor.Get should call 'BFExists' on inner client",
			prepare: func(rbf *redismock.RedisBloomFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbf.On("BFExists", context.Background(), "bf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbf.On("BFExists", context.Background(), "bf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbf.On("BFExists", context.Background(), "bf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					v, found, err := adaptor.Get(context.Background(), "foo")
					assert.NoError(t, err)
					assert.True(t, found)
					assert.Equal(t, []byte("t"), v)
				}
				{
					v, found, err := adaptor.Get(context.Background(), "bar")
					assert.NoError(t, err)
					assert.False(t, found)
					assert.Empty(t, v)
				}
				{
					v, found, err := adaptor.Get(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
					assert.False(t, found)
					assert.Empty(t, v)
				}
			},
		},
		{
			label: "adapter.Set should call 'BFAdd' in inner client",
			prepare: func(rbf *redismock.RedisBloomFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbf.On("BFAdd", context.Background(), "bf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbf.On("BFAdd", context.Background(), "bf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbf.On("BFAdd", context.Background(), "bf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					err := adaptor.Set(context.Background(), "foo", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
				}
				{
					err := adaptor.Set(context.Background(), "bar", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
				}
				{
					err := adaptor.Set(context.Background(), "baz", []byte("t"), 1*time.Minute)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:   "adaptor.Delete should do nothing on strict mode false",
			opts:    []redis.AdaptorOption{redis.WithStrict(false)},
			prepare: func(rbf *redismock.RedisBloomFilter) {},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.NoError(t, err)
			},
		},
		{
			label:   "adaptor.Delete should return error on strict mode true",
			opts:    []redis.AdaptorOption{redis.WithStrict(true)},
			prepare: func(rbf *redismock.RedisBloomFilter) {},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.EqualError(t, err, "delete operation not supported")
			},
		},
		{
			label: "adaptor.SetMulti should call 'BFInsert' from inner client",
			prepare: func(rbf *redismock.RedisBloomFilter) {
				{
					{
						var cmd redis_client.BoolSliceCmd

						cmd.SetVal([]bool{true, true})

						rbf.On("BFInsert", context.Background(),
							"bf-benthos-19700101", (*redis_client.BFInsertOptions)(nil), "foo", "bar").Return(&cmd)
					}
					{
						var cmd redis_client.BoolSliceCmd

						cmd.SetErr(errors.New("ops"))

						rbf.On("BFInsert", context.Background(),
							"bf-benthos-19700101", (*redis_client.BFInsertOptions)(nil), "baz", "bam").Return(&cmd)
					}
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					items := []service.CacheItem{
						{Key: "foo", Value: []byte("t")},
						{Key: "bar", Value: []byte("t")},
					}

					err := adaptor.SetMulti(context.Background(), items...)
					assert.NoError(t, err)
				}

				{
					items := []service.CacheItem{
						{Key: "baz", Value: []byte("t")},
						{Key: "bam", Value: []byte("t")},
					}

					err := adaptor.SetMulti(context.Background(), items...)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with success",
			prepare: func(rmi *redismock.RedisBloomFilter) {
				rmi.On("Close").Return(nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.NoError(t, err)
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with failure",
			prepare: func(rmi *redismock.RedisBloomFilter) {
				rmi.On("Close").Return(errors.New("ops"))
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.EqualError(t, err, "ops")
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			client := redismock.NewRedisBloomFilter(t)
			clock := clock.NewMock()

			clock.Add(1*time.Hour + 15*time.Minute + 45*time.Second)

			tc.prepare(client)

			opts := []redis.AdaptorOption{
				redis.WithClock(clock),
				redis.WithLocation(time.UTC),
			}

			opts = append(opts, tc.opts...)

			adaptor := redis.NewBloomFilterRedisCacheAdaptor(client, opts...)

			tc.verify(t, adaptor)
		})
	}
}

func TestCuckooFilterRedisAdaptor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		opts []redis.AdaptorOption

		prepare func(*redismock.RedisCuckooFilter)

		verify func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor)
	}{
		{
			label: "adaptor.Add should call 'CFAddNX' from inner client",
			opts: []redis.AdaptorOption{
				redis.WithFilterKeyTemplate("other-cf-benthos-%Y%m%d%H%M%S"),
			},
			prepare: func(rcf *redismock.RedisCuckooFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rcf.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rcf.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rcf.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					ok, err := adaptor.Add(context.Background(), "foo", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
					assert.True(t, ok)
				}
				{
					ok, err := adaptor.Add(context.Background(), "bar", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
					assert.False(t, ok)
				}
				{
					ok, err := adaptor.Add(context.Background(), "baz", []byte("t"), 1*time.Minute)
					assert.EqualError(t, err, "ops")
					assert.False(t, ok)
				}
			},
		},
		{
			label: "adaptor.Get should call 'CFExists' on inner client",
			prepare: func(rcf *redismock.RedisCuckooFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rcf.On("CFExists", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rcf.On("CFExists", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rcf.On("CFExists", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					v, found, err := adaptor.Get(context.Background(), "foo")
					assert.NoError(t, err)
					assert.True(t, found)
					assert.Equal(t, []byte("t"), v)
				}
				{
					v, found, err := adaptor.Get(context.Background(), "bar")
					assert.NoError(t, err)
					assert.False(t, found)
					assert.Empty(t, v)
				}
				{
					v, found, err := adaptor.Get(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
					assert.False(t, found)
					assert.Empty(t, v)
				}
			},
		},
		{
			label: "adapter.Set should call 'CFAdd' in inner client",
			prepare: func(rcf *redismock.RedisCuckooFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rcf.On("CFAdd", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rcf.On("CFAdd", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rcf.On("CFAdd", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					err := adaptor.Set(context.Background(), "foo", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
				}
				{
					err := adaptor.Set(context.Background(), "bar", []byte("t"), 1*time.Minute)
					assert.NoError(t, err)
				}
				{
					err := adaptor.Set(context.Background(), "baz", []byte("t"), 1*time.Minute)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label: "adaptor.Delete should call 'CFDel' from inner client",
			opts:  []redis.AdaptorOption{redis.WithStrict(false)},
			prepare: func(rcf *redismock.RedisCuckooFilter) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rcf.On("CFDel", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rcf.On("CFDel", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rcf.On("CFDel", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					err := adaptor.Delete(context.Background(), "foo")
					assert.NoError(t, err)
				}
				{
					err := adaptor.Delete(context.Background(), "bar")
					assert.NoError(t, err)
				}
				{
					err := adaptor.Delete(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label: "adaptor.SetMulti should call 'CFInsert' from inner client",
			prepare: func(rcf *redismock.RedisCuckooFilter) {
				{
					{
						var cmd redis_client.BoolSliceCmd

						cmd.SetVal([]bool{true, true})

						rcf.On("CFInsert", context.Background(),
							"cf-benthos-19700101", (*redis_client.CFInsertOptions)(nil), "foo", "bar").Return(&cmd)
					}
					{
						var cmd redis_client.BoolSliceCmd

						cmd.SetErr(errors.New("ops"))

						rcf.On("CFInsert", context.Background(),
							"cf-benthos-19700101", (*redis_client.CFInsertOptions)(nil), "baz", "bam").Return(&cmd)
					}
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				{
					items := []service.CacheItem{
						{Key: "foo", Value: []byte("t")},
						{Key: "bar", Value: []byte("t")},
					}

					err := adaptor.SetMulti(context.Background(), items...)
					assert.NoError(t, err)
				}

				{
					items := []service.CacheItem{
						{Key: "baz", Value: []byte("t")},
						{Key: "bam", Value: []byte("t")},
					}

					err := adaptor.SetMulti(context.Background(), items...)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with success",
			prepare: func(rmi *redismock.RedisCuckooFilter) {
				rmi.On("Close").Return(nil)
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.NoError(t, err)
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with failure",
			prepare: func(rmi *redismock.RedisCuckooFilter) {
				rmi.On("Close").Return(errors.New("ops"))
			},
			verify: func(t *testing.T, adaptor redis.RedisMultiCacheAdaptor) {
				t.Helper()

				err := adaptor.Close()
				assert.EqualError(t, err, "ops")
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			client := redismock.NewRedisCuckooFilter(t)
			clock := clock.NewMock()

			clock.Add(1*time.Hour + 15*time.Minute + 45*time.Second)

			tc.prepare(client)

			opts := []redis.AdaptorOption{
				redis.WithClock(clock),
				redis.WithLocation(time.UTC),
			}

			opts = append(opts, tc.opts...)

			adaptor := redis.NewCuckooFilterRedisCacheAdaptor(client, opts...)

			tc.verify(t, adaptor)
		})
	}
}

func TestBloomFilterKeyGeneration(t *testing.T) {
	t.Parallel()

	client := redismock.NewRedisBloomFilter(t)

	var cmd redis_client.BoolCmd

	client.On("BFExists", context.Background(), "other-bf-benthos-19700101000000", "foo").Return(&cmd)
	client.On("BFExists", context.Background(), "other-bf-benthos-19700101011530", "bar").Return(&cmd)

	clock := clock.NewMock()

	instance := redis.NewBloomFilterRedisCacheAdaptor(client,
		redis.WithInterval(10*time.Second),
		redis.WithClock(clock),
		redis.WithFilterKeyTemplate(`other-bf-benthos-%Y%m%d%H%M%S`),
	)

	{
		_, _, err := instance.Get(context.Background(), "foo")
		assert.NoError(t, err)
	}

	clock.Add(1*time.Hour + 15*time.Minute + 31*time.Second)

	{
		_, _, err := instance.Get(context.Background(), "bar")
		assert.NoError(t, err)
	}
}

func TestCuckooFilterKeyGeneration(t *testing.T) {
	t.Parallel()

	client := redismock.NewRedisCuckooFilter(t)

	var cmd redis_client.BoolCmd

	client.On("CFExists", context.Background(), "other-cf-benthos-19700101000000", "foo").Return(&cmd)
	client.On("CFExists", context.Background(), "other-cf-benthos-19700101011530", "bar").Return(&cmd)

	clock := clock.NewMock()

	instance := redis.NewCuckooFilterRedisCacheAdaptor(client,
		redis.WithInterval(10*time.Second),
		redis.WithClock(clock),
		redis.WithFilterKeyTemplate(`other-cf-benthos-%Y%m%d%H%M%S`),
	)

	{
		_, _, err := instance.Get(context.Background(), "foo")
		assert.NoError(t, err)
	}

	clock.Add(1*time.Hour + 15*time.Minute + 31*time.Second)

	{
		_, _, err := instance.Get(context.Background(), "bar")
		assert.NoError(t, err)
	}
}
