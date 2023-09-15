package redis

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/public/service"
)

var (
	_ RedisCacheAdaptor = (*crudRedisCacheAdaptor)(nil)
	_ RedisCacheAdaptor = (RedisMultiCacheAdaptor)(nil)

	_ RedisMultiCacheAdaptor = (*bloomFilterRedisCacheAdaptor)(nil)
	_ RedisMultiCacheAdaptor = (*cuckooFilterRedisCacheAdaptor)(nil)

	_ RedisCRUD         = (redis.UniversalClient)(nil)
	_ RedisBloomFilter  = (redis.UniversalClient)(nil)
	_ RedisCuckooFilter = (redis.UniversalClient)(nil)
)

// RedisCRUD few methods from Cmdable interface focus on basic CRUD.
type RedisCRUD interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	io.Closer
}

// RedisBloomFilter few methods from probabilistic interface focus on bloom filters.
type RedisBloomFilter interface {
	// BFAdd adds an item to a Bloom filter.
	// For more information - https://redis.io/commands/bf.add/
	BFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	// BFInsert inserts elements into a Bloom filter.
	// This function also allows for specifying additional options such as:
	// capacity, error rate, expansion rate, and non-scaling behavior.
	// For more information - https://redis.io/commands/bf.insert/
	BFInsert(ctx context.Context, key string, options *redis.BFInsertOptions, elements ...interface{}) *redis.BoolSliceCmd

	// BFExists determines whether a given item was added to a Bloom filter.
	// For more information - https://redis.io/commands/bf.exists/
	BFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	io.Closer
}

// RedisCuckooFilter few methods from probabilistic interface focus on cuckoo filters.
type RedisCuckooFilter interface {
	// CFAdd adds an element to a Cuckoo filter.
	// Returns true if the element was added to the filter or false if it already exists in the filter.
	// For more information - https://redis.io/commands/cf.add/
	CFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	// CFAddNX adds an element to a Cuckoo filter only if it does not already exist in the filter.
	// Returns true if the element was added to the filter or false if it already exists in the filter.
	// For more information - https://redis.io/commands/cf.addnx/
	CFAddNX(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	// CFInsert inserts elements into a Cuckoo filter.
	// This function also allows for specifying additional options such as capacity, error rate, expansion rate, and non-scaling behavior.
	// Returns an array of booleans indicating whether each element was added to the filter or not.
	// For more information - https://redis.io/commands/cf.insert/
	CFInsert(ctx context.Context, key string, options *redis.CFInsertOptions, elements ...interface{}) *redis.BoolSliceCmd

	// CFInsertNX inserts elements into a Cuckoo filter only if they do not already exist in the filter.
	// This function also allows for specifying additional options such as:
	// capacity, error rate, expansion rate, and non-scaling behavior.
	// Returns an array of integers indicating whether each element was added to the filter or not.
	// For more information - https://redis.io/commands/cf.insertnx/
	CFInsertNX(ctx context.Context, key string, options *redis.CFInsertOptions, elements ...interface{}) *redis.IntSliceCmd

	// CFDel deletes an item once from the cuckoo filter.
	// For more information - https://redis.io/commands/cf.del/
	CFDel(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	// CFExists determines whether an item may exist in the Cuckoo Filter or not.
	// For more information - https://redis.io/commands/cf.exists/
	CFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	io.Closer
}

// RedisCacheAdaptor is a minimal interface to use redis as cache.
type RedisCacheAdaptor interface {
	// Get a cache item.
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Set a cache item, specifying an optional TTL.
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	// Add is the same operation as Set except that it returns an error if the
	// key already exists.
	Add(ctx context.Context, key string, value []byte, expiration time.Duration) (bool, error)

	// Delete attempts to remove a key.
	Delete(ctx context.Context, key string) error

	io.Closer
}

type redisMultiSetter interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, items ...service.CacheItem) error
}

type RedisMultiCacheAdaptor interface {
	redisMultiSetter

	RedisCacheAdaptor
}

var (
	errDeleteOperationNotSupported = errors.New("delete operation not supported")
	errMissingFilterKey            = errors.New("missing filter key")
)

type crudRedisCacheAdaptor struct {
	client RedisCRUD
}

// NewCRUDRedisCacheAdaptor ctor.
func NewCRUDRedisCacheAdaptor(client RedisCRUD) RedisCacheAdaptor {
	return &crudRedisCacheAdaptor{client: client}
}

func (c *crudRedisCacheAdaptor) Add(ctx context.Context, key string, value []byte, expiration time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, expiration).Result()
}

func (c *crudRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return []byte(result), true, nil
}

func (c *crudRedisCacheAdaptor) Delete(ctx context.Context, key string) error {
	_, err := c.client.Del(ctx, key).Result()

	return err
}

func (c *crudRedisCacheAdaptor) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}

func (c *crudRedisCacheAdaptor) Close() error {
	return c.client.Close()
}

type bloomFilterRedisCacheAdaptor struct {
	client     RedisBloomFilter
	filterKey  string
	strict     bool
	insertOpts *redis.BFInsertOptions
}

// NewBloomFilterRedisCacheAdaptor ctor.
func NewBloomFilterRedisCacheAdaptor(client RedisBloomFilter,
	filterKey string, strict bool, insertOpts *redis.BFInsertOptions) (RedisMultiCacheAdaptor, error) {
	if filterKey == "" {
		return nil, errMissingFilterKey
	}

	return &bloomFilterRedisCacheAdaptor{
		client:     client,
		filterKey:  filterKey,
		strict:     strict,
		insertOpts: insertOpts,
	}, nil
}

func (c *bloomFilterRedisCacheAdaptor) Add(ctx context.Context, key string, _ []byte, _ time.Duration) (bool, error) {
	return c.client.BFAdd(ctx, c.filterKey, key).Result()
}

func (c *bloomFilterRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ok, err := c.client.BFExists(ctx, c.filterKey, key).Result()
	if err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	return []byte{'t'}, true, nil
}

func (c *bloomFilterRedisCacheAdaptor) Delete(_ context.Context, key string) error {
	if c.strict {
		return errDeleteOperationNotSupported
	}

	return nil
}

func (c *bloomFilterRedisCacheAdaptor) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.client.BFAdd(ctx, c.filterKey, key).Err()
}

func (c *bloomFilterRedisCacheAdaptor) SetMulti(ctx context.Context, items ...service.CacheItem) error {
	elements := make([]interface{}, len(items))

	for i, item := range items {
		elements[i] = item.Key
	}

	return c.client.BFInsert(ctx, c.filterKey, c.insertOpts, elements...).Err()
}

func (c *bloomFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}

type cuckooFilterRedisCacheAdaptor struct {
	client     RedisCuckooFilter
	filterKey  string
	insertOpts *redis.CFInsertOptions
}

// NewCuckooFilterRedisCacheAdaptor ctor.
// will format the current time.Time as `cf-benthos-%Y%m%d` using "github.com/itchyny/timefmt-go".Format
// can be changed with WithFilterKey(...) option.
// Cuckoo filters supports Delete operations. WithStrict option will be ignored.
func NewCuckooFilterRedisCacheAdaptor(client RedisCuckooFilter,
	filterKey string, insertOpts *redis.CFInsertOptions) (RedisMultiCacheAdaptor, error) {
	if filterKey == "" {
		return nil, errMissingFilterKey
	}

	return &cuckooFilterRedisCacheAdaptor{
		client:     client,
		filterKey:  filterKey,
		insertOpts: insertOpts,
	}, nil
}

func (c *cuckooFilterRedisCacheAdaptor) Add(ctx context.Context, key string, _ []byte, _ time.Duration) (bool, error) {
	if c.insertOpts != nil {
		r, err := c.client.CFInsertNX(ctx, c.filterKey, c.insertOpts, key).Result()
		if err != nil {
			return false, err
		}

		ok := len(r) > 0 && r[0] == 1

		return ok, nil
	}

	return c.client.CFAddNX(ctx, c.filterKey, key).Result()
}

func (c *cuckooFilterRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ok, err := c.client.CFExists(ctx, c.filterKey, key).Result()
	if err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	}

	return []byte{'t'}, true, nil
}

func (c *cuckooFilterRedisCacheAdaptor) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if c.insertOpts != nil {
		return c.client.CFInsert(ctx, c.filterKey, c.insertOpts, key).Err()
	}

	return c.client.CFAdd(ctx, c.filterKey, key).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) SetMulti(ctx context.Context, items ...service.CacheItem) error {
	elements := make([]interface{}, len(items))

	for i, item := range items {
		elements[i] = item.Key
	}

	return c.client.CFInsert(ctx, c.filterKey, c.insertOpts, elements...).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) Delete(ctx context.Context, key string) error {
	return c.client.CFDel(ctx, c.filterKey, key).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}
