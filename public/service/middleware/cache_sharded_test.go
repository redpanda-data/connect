package middleware_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/benthosdev/benthos/v4/public/service/middleware"
	"github.com/benthosdev/benthos/v4/public/service/servicemock"
)

func TestShardedCache(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label   string
		nShards int

		prepare func(mocks []*servicemock.Cache)
		verify  func(t *testing.T, cache service.Cache)
	}{
		{
			label:   "Get should ask for one specific shard",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[1]
					mock.On("Get", context.Background(), "foo").Return([]byte("bar"), nil)
				}
				{
					mock := mocks[0]
					mock.On("Get", context.Background(), "baz").Return(nil, errors.New("ops"))
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				{
					value, err := cache.Get(context.Background(), "foo")
					assert.NoError(t, err)
					assert.EqualValues(t, "bar", value)
				}
				{
					value, err := cache.Get(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
					assert.Empty(t, value)
				}
			},
		},
		{
			label:   "Set should apply for one specific shard",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[1]
					mock.On("Set", context.Background(), "foo", []byte("bar"), (*time.Duration)(nil)).Return(nil)
				}
				{
					mock := mocks[0]
					ttl := 5 * time.Second
					mock.On("Set", context.Background(), "baz", []byte("bam"), &ttl).Return(errors.New("ops"))
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				{
					err := cache.Set(context.Background(), "foo", []byte("bar"), nil)
					assert.NoError(t, err)
				}
				{
					ttl := 5 * time.Second
					err := cache.Set(context.Background(), "baz", []byte("bam"), &ttl)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:   "Add should apply for one specific shard",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[1]
					mock.On("Add", context.Background(), "foo", []byte("bar"), (*time.Duration)(nil)).Return(nil)
				}
				{
					mock := mocks[0]
					ttl := 5 * time.Second
					mock.On("Add", context.Background(), "baz", []byte("bam"), &ttl).Return(errors.New("ops"))
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				{
					err := cache.Add(context.Background(), "foo", []byte("bar"), nil)
					assert.NoError(t, err)
				}
				{
					ttl := 5 * time.Second
					err := cache.Add(context.Background(), "baz", []byte("bam"), &ttl)
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:   "Delete should apply for one specific shard",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[1]
					mock.On("Delete", context.Background(), "foo").Return(nil)
				}
				{
					mock := mocks[0]
					mock.On("Delete", context.Background(), "baz").Return(errors.New("ops"))
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				{
					err := cache.Delete(context.Background(), "foo")
					assert.NoError(t, err)
				}
				{
					err := cache.Delete(context.Background(), "baz")
					assert.EqualError(t, err, "ops")
				}
			},
		},
		{
			label:   "call sharded Close should call Close on each shard with success",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[0]
					mock.On("Close", context.Background()).Return(nil)
				}
				{
					mock := mocks[1]
					mock.On("Close", context.Background()).Return(nil)
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				err := cache.Close(context.Background())
				assert.NoError(t, err)
			},
		},
		{
			label:   "call sharded Close should call Close on each shard with all failures",
			nShards: 2,
			prepare: func(mocks []*servicemock.Cache) {
				{
					mock := mocks[0]
					mock.On("Close", context.Background()).Return(errors.New("ops"))
				}
				{
					mock := mocks[1]
					mock.On("Close", context.Background()).Return(errors.New("ops"))
				}
			},
			verify: func(t *testing.T, cache service.Cache) {
				t.Helper()

				err := cache.Close(context.Background())
				assert.EqualError(t, err, `error while closing shard #0: ops
error while closing shard #1: ops`)
			},
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			var mocks []*servicemock.Cache
			instance, err := middleware.NewShardedCache(context.Background(),
				tc.nShards, func(_ middleware.ShardInfo) (service.Cache, error) {
					cache := servicemock.NewCache(t)

					mocks = append(mocks, cache)

					return cache, nil
				})

			require.NoError(t, err)
			require.Len(t, mocks, tc.nShards)

			tc.prepare(mocks)

			tc.verify(t, instance)
		})
	}
}

func TestSetMultiWithABatchedCached(t *testing.T) {
	t.Parallel()

	var mocks []*batchedMockCache

	instance, err := middleware.NewShardedCache(context.Background(),
		2, func(_ middleware.ShardInfo) (service.Cache, error) {
			mock := newBatchedMockCache(t)

			mocks = append(mocks, mock)

			return mock, nil
		})

	require.NoError(t, err)

	multiInstance, ok := instance.(interface {
		SetMulti(ctx context.Context, keyValues ...service.CacheItem) error
	})
	require.True(t, ok)

	{
		mock := mocks[1]
		mock.On("SetMulti", context.Background(), []service.CacheItem{
			{
				Key:   "foo",
				Value: []byte("bar"),
			},
			{
				Key:   "foo2",
				Value: []byte("bar2"),
			},
		}).Return(nil)
	}
	{
		mock := mocks[0]
		ttl := 5 * time.Second
		mock.On("SetMulti", context.Background(), []service.CacheItem{
			{
				Key:   "baz",
				Value: []byte("bam"),
				TTL:   &ttl,
			},
			{
				Key:   "baz2",
				Value: []byte("bam2"),
				TTL:   &ttl,
			},
		}).Return(nil)
	}

	ttl := 5 * time.Second
	err = multiInstance.SetMulti(context.Background(), service.CacheItem{
		Key:   "foo",
		Value: []byte("bar"),
	}, service.CacheItem{
		Key:   "baz",
		Value: []byte("bam"),
		TTL:   &ttl,
	}, service.CacheItem{
		Key:   "foo2",
		Value: []byte("bar2"),
	}, service.CacheItem{
		Key:   "baz2",
		Value: []byte("bam2"),
		TTL:   &ttl,
	})

	assert.NoError(t, err)
}

func TestSetMultiWithoutANormalCached(t *testing.T) {
	t.Parallel()

	var mocks []*servicemock.Cache

	instance, err := middleware.NewShardedCache(context.Background(),
		2, func(_ middleware.ShardInfo) (service.Cache, error) {
			mock := servicemock.NewCache(t)

			mocks = append(mocks, mock)

			return mock, nil
		})

	require.NoError(t, err)

	multiInstance, ok := instance.(interface {
		SetMulti(ctx context.Context, keyValues ...service.CacheItem) error
	})
	require.True(t, ok)

	{
		mock := mocks[1]
		var ttlPtr *time.Duration
		mock.On("Set", context.Background(), "foo", []byte("bar"), ttlPtr).Return(nil)
		mock.On("Set", context.Background(), "foo2", []byte("bar2"), ttlPtr).Return(nil)
	}
	{
		mock := mocks[0]
		ttl := 5 * time.Second
		mock.On("Set", context.Background(), "baz", []byte("bam"), &ttl).Return(nil)
		mock.On("Set", context.Background(), "baz2", []byte("bam2"), &ttl).Return(nil)
	}

	ttl := 5 * time.Second
	err = multiInstance.SetMulti(context.Background(), service.CacheItem{
		Key:   "foo",
		Value: []byte("bar"),
	}, service.CacheItem{
		Key:   "baz",
		Value: []byte("bam"),
		TTL:   &ttl,
	}, service.CacheItem{
		Key:   "foo2",
		Value: []byte("bar2"),
	}, service.CacheItem{
		Key:   "baz2",
		Value: []byte("bam2"),
		TTL:   &ttl,
	})

	assert.NoError(t, err)
}

var _ service.Cache = (*batchedMockCache)(nil)

type batchedMockCache struct {
	*servicemock.Cache
}

func newBatchedMockCache(t *testing.T) *batchedMockCache {
	cache := servicemock.NewCache(t)
	return &batchedMockCache{Cache: cache}
}

func (m *batchedMockCache) SetMulti(ctx context.Context, keyValues ...service.CacheItem) error {
	return m.Called(ctx, keyValues).Error(0)
}

func TestShardedCacheCtor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		nShards int

		ctorBuilder func(t *testing.T) middleware.CacheCtorCallback

		errMsg string
	}{
		{
			label:   "should return error from first call to ctor",
			nShards: 2,
			ctorBuilder: func(t *testing.T) middleware.CacheCtorCallback {
				return func(info middleware.ShardInfo) (service.Cache, error) {
					if info.I == 0 {
						return nil, errors.New("ops")
					}

					panic("unexpected call")
				}
			},
			errMsg: "ops",
		},
		{
			label:   "should return error from second call and close the first cache",
			nShards: 2,
			ctorBuilder: func(t *testing.T) middleware.CacheCtorCallback {
				return func(info middleware.ShardInfo) (service.Cache, error) {
					switch info.I {
					case 0:
						mock := servicemock.NewCache(t)
						mock.On("Close", context.Background()).Return(nil)
						return mock, nil
					case 1:
						return nil, errors.New("ops")
					}

					panic("unexpected call")
				}
			},
			errMsg: "ops",
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()

			var ctor middleware.CacheCtorCallback

			if tc.ctorBuilder != nil {
				ctor = tc.ctorBuilder(t)
			}

			_, err := middleware.NewShardedCache(context.Background(), tc.nShards, ctor)
			if tc.errMsg != "" {
				assert.EqualError(t, err, tc.errMsg)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestCtorSpecialCasesShard(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string
		n     int
	}{
		{
			label: "n=1 should return same instance",
			n:     1,
		},
		{
			label: "n=0 should return same instance (same as 1)",
			n:     0,
		},
		{
			label: "n=-1 should return same instance (same as 1)",
			n:     -1,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			mock := servicemock.NewCache(t)

			var (
				lastPtr atomic.Pointer[middleware.ShardInfo]
				calls   atomic.Int64
			)

			instance, err := middleware.NewShardedCache(context.Background(),
				1, func(info middleware.ShardInfo) (service.Cache, error) {
					lastPtr.Store(&info)

					calls.Add(1)

					return mock, nil
				})

			require.NoError(t, err)
			assert.Equal(t, mock, instance, "should return same instance")

			assert.EqualValues(t, 1, calls.Load())

			info := lastPtr.Load()

			assert.Equal(t, 0, info.I, "index")
			assert.Equal(t, 1, info.N, "number of shards")
		})
	}
}
