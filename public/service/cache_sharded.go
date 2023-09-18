package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OneOfOne/xxhash"
)

var (
	_ Cache        = (*shardedCache)(nil)
	_ batchedCache = (*shardedCache)(nil)
)

type shardedCache struct {
	shards  []Cache
	mShards []batchedCache
}

var errMustHaveAtLeastOneShard = errors.New("must have at least one shard")

// CacheShardedFields return the fields to be added to a cache configuration to support sharded cache.
func CacheShardedFields() []*ConfigField {
	return []*ConfigField{
		NewIntField("shards").
			Description("A number of logical shards to spread keys across, increasing the shards can have a performance benefit when processing a large number of keys.").
			Default(1).
			Advanced(),
	}
}

// WrapCacheConstructorWithShards will create a sharded cache with n shards if there is a
// field 'shards' available in the configuration.
func WrapCacheConstructorWithShards(ctx context.Context, ctor CacheConstructor) CacheConstructor {
	return func(conf *ParsedConfig, mgr *Resources) (instance Cache, err error) {
		var nShards = 1
		if conf.Contains("shards") {
			nShards, err = conf.FieldInt("shards")
			if err != nil {
				return nil, err
			}
		}

		return NewShardedCache(ctx, nShards, func() (Cache, error) {
			return ctor(conf, mgr)
		})
	}
}

// NewShardedCache constructor. receive a cache constructor callback that will be
// called at least n times. Will return at first error and close the already opened shards.
// If the cache implements SetMulti method, we will delegate the calls to the respective shards.
func NewShardedCache(ctx context.Context, nShards int, ctor func() (Cache, error)) (Cache, error) {
	if nShards <= 0 {
		return nil, errMustHaveAtLeastOneShard
	}

	firstShard, err := ctor()
	if err != nil {
		return nil, err
	}

	if nShards == 1 {
		return firstShard, nil // no need to handle a sharded cache in this case
	}

	shards := make([]Cache, nShards)
	shards[0] = firstShard

	var mShards []batchedCache

	if mFirstShard, ok := firstShard.(batchedCache); ok {
		mShards = make([]batchedCache, 1, nShards)

		mShards[0] = mFirstShard
	}

	for i := 1; i < nShards; i++ {
		nextShard, err := ctor()
		if err != nil {
			cerr := closeShards(ctx, shards[:i])
			if cerr != nil {
				err = errors.Join(err, cerr)
			}

			return nil, err
		}

		shards[i] = nextShard

		if mNextShard, ok := nextShard.(batchedCache); ok {
			mShards = append(mShards, mNextShard)
		}
	}

	return &shardedCache{
		shards:  shards,
		mShards: mShards,
	}, nil
}

func (c *shardedCache) getShard(key string) Cache {
	if len(c.shards) == 1 {
		return c.shards[0]
	}

	return c.shards[xxhash.ChecksumString64(key)%uint64(len(c.shards))]
}

func (c *shardedCache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.getShard(key).Get(ctx, key)
}

func (c *shardedCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return c.getShard(key).Add(ctx, key, value, ttl)
}

func (c *shardedCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return c.getShard(key).Set(ctx, key, value, ttl)
}

func (c *shardedCache) getMultiShardIndex(key string) uint64 {
	return xxhash.ChecksumString64(key) % uint64(len(c.mShards))
}

func (c *shardedCache) SetMulti(ctx context.Context, keyValues ...CacheItem) error {
	if len(c.mShards) > 0 {
		return c.handleInnerBatchedCaches(ctx, keyValues...)
	}

	errs := make([]error, 0, len(keyValues))

	for _, keyValue := range keyValues {
		if err := c.Set(ctx, keyValue.Key, keyValue.Value, keyValue.TTL); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *shardedCache) handleInnerBatchedCaches(ctx context.Context, keyValues ...CacheItem) error {
	keyValuesByShardIndexKey := map[uint64][]CacheItem{}

	for _, keyValue := range keyValues {
		shardIndex := c.getMultiShardIndex(keyValue.Key)

		keyValuesByShardIndexKey[shardIndex] = append(keyValuesByShardIndexKey[shardIndex], keyValue)
	}

	errs := make([]error, 0, len(keyValuesByShardIndexKey))

	for shardIndex, keyValues := range keyValuesByShardIndexKey {
		if err := c.mShards[shardIndex].SetMulti(ctx, keyValues...); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (c *shardedCache) Delete(ctx context.Context, key string) error {
	return c.getShard(key).Delete(ctx, key)
}

func (c *shardedCache) Close(ctx context.Context) error {
	return closeShards(ctx, c.shards)
}

func closeShards(ctx context.Context, shards []Cache) error {
	errs := make([]error, 0, len(shards))

	for i, shard := range shards {
		if err := shard.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("error while closing shard #%d: %w", i, err))
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
