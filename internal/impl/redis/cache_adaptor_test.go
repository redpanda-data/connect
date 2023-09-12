package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/benthosdev/benthos/v4/internal/impl/redis"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/redismock"

	redis_client "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRedisAdaptor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		prepare func(*redismock.RedisMinimalInterface)

		verify func(t *testing.T, adaptor redis.RedisCacheAdaptor)
	}{
		{
			label: "adaptor.Get should call 'Get' from inner client with success",
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisMinimalInterface) {
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

			client := redismock.NewRedisMinimalInterface(t)

			tc.prepare(client)

			adaptor := redis.NewDefaultRedisCacheAdaptor(client)

			tc.verify(t, adaptor)
		})
	}
}

func TestBloomFilterRedisAdaptor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		opts []redis.Option

		prepare func(*redismock.RedisBloomFilterMinimalInterface)

		verify func(t *testing.T, adaptor redis.RedisCacheAdaptor)
	}{
		{
			label: "adaptor.Add should call 'BFAdd' from inner client",
			opts: []redis.Option{
				redis.WithFilterKey("other-bf-benthos-%Y%m%d%H%M%S"),
			},
			prepare: func(rbfmi *redismock.RedisBloomFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("BFAdd", context.Background(), "other-bf-benthos-19700101011545", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			prepare: func(rbfmi *redismock.RedisBloomFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("BFExists", context.Background(), "bf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("BFExists", context.Background(), "bf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("BFExists", context.Background(), "bf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			prepare: func(rbfmi *redismock.RedisBloomFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("BFAdd", context.Background(), "bf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("BFAdd", context.Background(), "bf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("BFAdd", context.Background(), "bf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			opts:    []redis.Option{redis.WithStrict(false)},
			prepare: func(rbfmi *redismock.RedisBloomFilterMinimalInterface) {},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.NoError(t, err)
			},
		},
		{
			label:   "adaptor.Delete should return error on strict mode true",
			opts:    []redis.Option{redis.WithStrict(true)},
			prepare: func(rbfmi *redismock.RedisBloomFilterMinimalInterface) {},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
				t.Helper()

				err := adaptor.Delete(context.Background(), "foo")
				assert.EqualError(t, err, "delete operation not supported")
			},
		},
		{
			label: "adaptor.Close should call 'Close' from inner client with success",
			prepare: func(rmi *redismock.RedisBloomFilterMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisBloomFilterMinimalInterface) {
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

			client := redismock.NewRedisBloomFilterMinimalInterface(t)
			clock := clock.NewMock()

			clock.Add(1*time.Hour + 15*time.Minute + 45*time.Second)

			tc.prepare(client)

			opts := []redis.Option{
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

		opts []redis.Option

		prepare func(*redismock.RedisCuckooFilterMinimalInterface)

		verify func(t *testing.T, adaptor redis.RedisCacheAdaptor)
	}{
		{
			label: "adaptor.Add should call 'CFAddNX' from inner client",
			opts: []redis.Option{
				redis.WithFilterKey("other-cf-benthos-%Y%m%d%H%M%S"),
			},
			prepare: func(rbfmi *redismock.RedisCuckooFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("CFAddNX", context.Background(), "other-cf-benthos-19700101011545", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			prepare: func(rbfmi *redismock.RedisCuckooFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("CFExists", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("CFExists", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("CFExists", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			prepare: func(rbfmi *redismock.RedisCuckooFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("CFAdd", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("CFAdd", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("CFAdd", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			opts:  []redis.Option{redis.WithStrict(false)},
			prepare: func(rbfmi *redismock.RedisCuckooFilterMinimalInterface) {
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(true)

					rbfmi.On("CFDel", context.Background(), "cf-benthos-19700101", "foo").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetVal(false)

					rbfmi.On("CFDel", context.Background(), "cf-benthos-19700101", "bar").Return(&cmd)
				}
				{
					var cmd redis_client.BoolCmd

					cmd.SetErr(errors.New("ops"))

					rbfmi.On("CFDel", context.Background(), "cf-benthos-19700101", "baz").Return(&cmd)
				}
			},
			verify: func(t *testing.T, adaptor redis.RedisCacheAdaptor) {
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
			label: "adaptor.Close should call 'Close' from inner client with success",
			prepare: func(rmi *redismock.RedisCuckooFilterMinimalInterface) {
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
			prepare: func(rmi *redismock.RedisCuckooFilterMinimalInterface) {
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

			client := redismock.NewRedisCuckooFilterMinimalInterface(t)
			clock := clock.NewMock()

			clock.Add(1*time.Hour + 15*time.Minute + 45*time.Second)

			tc.prepare(client)

			opts := []redis.Option{
				redis.WithClock(clock),
				redis.WithLocation(time.UTC),
			}

			opts = append(opts, tc.opts...)

			adaptor := redis.NewCuckooFilterRedisCacheAdaptor(client, opts...)

			tc.verify(t, adaptor)
		})
	}
}
