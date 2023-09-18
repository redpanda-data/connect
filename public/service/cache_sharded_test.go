package service_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/benthosdev/benthos/v4/public/service/servicemock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			instance, err := service.NewShardedCache(context.Background(), tc.nShards, func() (service.Cache, error) {
				cache := servicemock.NewCache(t)

				mocks = append(mocks, cache)

				return cache, nil
			})

			require.NoError(t, err)
			require.Len(t, mocks, int(tc.nShards))

			tc.prepare(mocks)

			tc.verify(t, instance)
		})
	}
}

func TestSetMultiWithABatchedCached(t *testing.T) {
	t.Parallel()

	var mocks []*batchedMockCache

	instance, err := service.NewShardedCache(context.Background(), 2, func() (service.Cache, error) {
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

	instance, err := service.NewShardedCache(context.Background(), 2, func() (service.Cache, error) {
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

// TODO:

// add tests with ctor and failure
func TestShardedCacheCtor(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		label string

		nShards int

		ctorBuilder func(t *testing.T, pc *atomic.Int64) func() (service.Cache, error)

		errMsg string
	}{
		{
			label:   "ctor should return error if n=0",
			nShards: 0,
			errMsg:  "must have at least one shard",
		},
		{
			label:   "should return error from first call to ctor",
			nShards: 2,
			ctorBuilder: func(t *testing.T, pc *atomic.Int64) func() (service.Cache, error) {
				return func() (service.Cache, error) {
					if pc.Add(1) == 1 {
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
			ctorBuilder: func(t *testing.T, pc *atomic.Int64) func() (service.Cache, error) {
				return func() (service.Cache, error) {
					switch pc.Add(1) {
					case 1:
						mock := servicemock.NewCache(t)
						mock.On("Close", context.Background()).Return(nil)
						return mock, nil
					case 2:
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

			var ctor func() (service.Cache, error)

			if tc.ctorBuilder != nil {
				var pc atomic.Int64
				ctor = tc.ctorBuilder(t, &pc)
			}

			_, err := service.NewShardedCache(context.Background(), tc.nShards, ctor)
			if tc.errMsg != "" {
				assert.EqualError(t, err, tc.errMsg)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestCtorSpecialCaseOneShard(t *testing.T) {
	mock := servicemock.NewCache(t)

	instance, err := service.NewShardedCache(context.Background(),
		1, func() (service.Cache, error) {
			return mock, nil
		})

	require.NoError(t, err)
	assert.Equal(t, mock, instance, "should return same instance")
}
