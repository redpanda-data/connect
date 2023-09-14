package redis

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/itchyny/timefmt-go"
	"github.com/redis/go-redis/v9"
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

type conf struct {
	strict    bool
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

func (c *conf) SetDefaults(filterKeyPrefix string) {
	c.strict = false
	c.filterKey = filterKeyPrefix + `-benthos-%Y%m%d`
	c.location = time.UTC
	c.clock = clock.New()
}

// AdaptorOption functional option type
type AdaptorOption func(*conf)

// WithStrict can enable the strict mode. Not supported operations will fail.
// default is false.
func WithStrict(strict bool) AdaptorOption {
	return func(c *conf) {
		c.strict = strict
	}
}

// WithFilterKey can rewrite the filter key used on bloom and cuckoo filters. Accepts
// To be parsed with "github.com/itchyny/timefmt-go".Format with current time.
func WithFilterKey(filterKey string) AdaptorOption {
	return func(c *conf) {
		c.filterKey = filterKey
	}
}

// WithLocation can update the time.Location used to format filter keys based on current timestamp.
// Default is UTC.
func WithLocation(location *time.Location) AdaptorOption {
	return func(c *conf) {
		c.location = location
	}
}

// WithClock inject a custom clock for testing purposes.
func WithClock(clock clock.Clock) AdaptorOption {
	return func(c *conf) {
		c.clock = clock
	}
}

var errDeleteOperationNotSupported = errors.New("delete operation not supported")

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
	client    RedisBloomFilter
	strict    bool
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

// NewBloomFilterRedisCacheAdaptor ctor.
// will format the current time.Time as `bf-benthos-%Y%m%d` using "github.com/itchyny/timefmt-go".Format
// can be changed with WithFilterKey(...) option.
// Does not supports Delete operation. May return error if using option WithStrict(true)
func NewBloomFilterRedisCacheAdaptor(client RedisBloomFilter, opts ...AdaptorOption) RedisMultiCacheAdaptor {
	var c conf

	c.SetDefaults("bf")

	for _, opt := range opts {
		opt(&c)
	}

	return &bloomFilterRedisCacheAdaptor{
		client:    client,
		strict:    c.strict,
		filterKey: c.filterKey,
		location:  c.location,
		clock:     c.clock,
	}
}

func (c *bloomFilterRedisCacheAdaptor) buildFilterKey() string {
	now := c.clock.Now().In(c.location)
	return timefmt.Format(now.In(c.location), c.filterKey)
}

func (c *bloomFilterRedisCacheAdaptor) Add(ctx context.Context, key string, _ []byte, _ time.Duration) (bool, error) {
	filterKey := c.buildFilterKey()

	return c.client.BFAdd(ctx, filterKey, key).Result()
}

func (c *bloomFilterRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	filterKey := c.buildFilterKey()

	ok, err := c.client.BFExists(ctx, filterKey, key).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
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
	filterKey := c.buildFilterKey()

	return c.client.BFAdd(ctx, filterKey, key).Err()
}

func (c *bloomFilterRedisCacheAdaptor) SetMulti(ctx context.Context, items ...service.CacheItem) error {
	filterKey := c.buildFilterKey()

	elements := make([]interface{}, len(items))

	for i, item := range items {
		elements[i] = item.Key
	}

	return c.client.BFInsert(ctx, filterKey, nil, elements...).Err()
}

func (c *bloomFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}

type cuckooFilterRedisCacheAdaptor struct {
	client    RedisCuckooFilter
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

// NewCuckooFilterRedisCacheAdaptor ctor.
// will format the current time.Time as `cf-benthos-%Y%m%d` using "github.com/itchyny/timefmt-go".Format
// can be changed with WithFilterKey(...) option.
// Cuckoo filters supports Delete operations. WithStrict option will be ignored.
func NewCuckooFilterRedisCacheAdaptor(client RedisCuckooFilter, opts ...AdaptorOption) RedisMultiCacheAdaptor {
	var c conf

	c.SetDefaults("cf")

	for _, opt := range opts {
		opt(&c)
	}

	return &cuckooFilterRedisCacheAdaptor{
		client:    client,
		filterKey: c.filterKey,
		location:  c.location,
		clock:     c.clock,
	}
}

func (c *cuckooFilterRedisCacheAdaptor) buildFilterKey() string {
	now := c.clock.Now().In(c.location)
	return timefmt.Format(now.In(c.location), c.filterKey)
}

func (c *cuckooFilterRedisCacheAdaptor) Add(ctx context.Context, key string, _ []byte, _ time.Duration) (bool, error) {
	filterKey := c.buildFilterKey()

	return c.client.CFAddNX(ctx, filterKey, key).Result()
}

func (c *cuckooFilterRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	filterKey := c.buildFilterKey()

	ok, err := c.client.CFExists(ctx, filterKey, key).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return []byte{'t'}, true, nil
}

func (c *cuckooFilterRedisCacheAdaptor) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	filterKey := c.buildFilterKey()

	return c.client.CFAdd(ctx, filterKey, key).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) SetMulti(ctx context.Context, items ...service.CacheItem) error {
	filterKey := c.buildFilterKey()

	elements := make([]interface{}, len(items))

	for i, item := range items {
		elements[i] = item.Key
	}

	return c.client.CFInsert(ctx, filterKey, nil, elements...).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) Delete(ctx context.Context, key string) error {
	filterKey := c.buildFilterKey()

	return c.client.CFDel(ctx, filterKey, key).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}
