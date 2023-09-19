package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/benthosdev/benthos/v4/public/service"
)

var (
	_ service.Cache = (*shardedCache)(nil)
	_ batchedCache  = (*shardedCache)(nil)
)

// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type batchedCache interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, keyValues ...service.CacheItem) error
}

type shardedCache struct {
	shards  []service.Cache
	mShards []batchedCache
}

// CacheShardedFields return the fields to be added to a cache configuration to support sharded cache.
func CacheShardedFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewIntField("shards").
			Description("A number of logical shards to spread keys across, increasing the shards can have a performance benefit when processing a large number of keys.").
			Default(1).
			Advanced(),
	}
}

// ApplyCacheShardedFields apply CacheShardedFields into config spec.
func ApplyCacheShardedFields(spec *service.ConfigSpec) *service.ConfigSpec {
	for _, f := range CacheShardedFields() {
		spec = spec.Field(f)
	}

	return spec
}

type ShardInfo struct {
	I int
	N int
}

type shardInfoContextKeyType string

const shardInfoContextKey shardInfoContextKeyType = "shard-info-context-key"

func ShardInfoFromContext(ctx context.Context) (ShardInfo, bool) {
	info, ok := ctx.Value(shardInfoContextKey).(ShardInfo)

	return info, ok
}
func ContextWithShardInfo(ctx context.Context, info ShardInfo) context.Context {
	return context.WithValue(ctx, shardInfoContextKey, info)
}

type CtxCacheConstructor func(context.Context,
	*service.ParsedConfig, *service.Resources) (service.Cache, error)

type CtxCtorCallback func(context.Context) (service.Cache, error)

// WrapCacheConstructorWithShards will create a sharded cache with n shards if there is a
// field 'shards' available in the configuration.
func WrapCacheConstructorWithShards(ctx context.Context,
	ctor CtxCacheConstructor) service.CacheConstructor {
	return func(conf *service.ParsedConfig, mgr *service.Resources) (instance service.Cache, err error) {
		var nShards int
		if conf.Contains("shards") {
			nShards, err = conf.FieldInt("shards")
			if err != nil {
				return nil, err
			}
		}

		shardedCtor := func(ctx context.Context) (service.Cache, error) {
			return ctor(ctx, conf, mgr)
		}

		return NewShardedCache(ctx, nShards, shardedCtor, mgr.Logger())
	}
}

// NewShardedCache constructor. receive a cache constructor callback that will be
// called at least n times. Will return at first error and close the already opened shards.
// If the cache implements SetMulti method, we will delegate the calls to the respective shards.
func NewShardedCache(ctx context.Context, nShards int,
	ctor CtxCtorCallback, logger *service.Logger) (service.Cache, error) {
	if nShards < 1 {
		nShards = 1
	}

	info := ShardInfo{N: nShards}

	firstShard, err := ctor(ContextWithShardInfo(ctx, info))
	if err != nil {
		return nil, err
	}

	if nShards == 1 {
		return firstShard, nil // no need to handle a sharded cache in this case
	}

	shards := make([]service.Cache, nShards)
	shards[0] = firstShard

	var mShards []batchedCache

	if mFirstShard, ok := firstShard.(batchedCache); ok {
		mShards = make([]batchedCache, 1, nShards)

		mShards[0] = mFirstShard
	}

	for i := 1; i < nShards; i++ {
		info.I = i
		nextShard, err := ctor(ContextWithShardInfo(ctx, info))
		if err != nil {
			cerr := closeShards(ctx, shards[:i])
			if cerr != nil {
				logger.With("error", cerr).Warn("unexpected error while closing caches")
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

func (c *shardedCache) getShard(key string) service.Cache {
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

func (c *shardedCache) SetMulti(ctx context.Context, keyValues ...service.CacheItem) error {
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

func (c *shardedCache) handleInnerBatchedCaches(ctx context.Context, keyValues ...service.CacheItem) error {
	keyValuesByShardIndexKey := map[uint64][]service.CacheItem{}

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

func closeShards(ctx context.Context, shards []service.Cache) error {
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
