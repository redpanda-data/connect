package redis

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/itchyny/timefmt-go"
	"github.com/redis/go-redis/v9"
)

var (
	_ RedisCacheAdaptor = (*defaultRedisCacheAdaptor)(nil)
	_ RedisCacheAdaptor = (*bloomFilterRedisCacheAdaptor)(nil)
	_ RedisCacheAdaptor = (*cuckooFilterRedisCacheAdaptor)(nil)

	_ RedisMinimalInterface             = (redis.UniversalClient)(nil)
	_ RedisBloomFilterMinimalInterface  = (redis.UniversalClient)(nil)
	_ RedisCuckooFilterMinimalInterface = (redis.UniversalClient)(nil)
)

type RedisMinimalInterface interface {
	// from cmdable interface
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	io.Closer
}

type RedisBloomFilterMinimalInterface interface {
	// from probabilistic interface
	BFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd
	BFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	io.Closer
}

type RedisCuckooFilterMinimalInterface interface {
	CFAdd(ctx context.Context, key string, element interface{}) *redis.BoolCmd
	CFAddNX(ctx context.Context, key string, element interface{}) *redis.BoolCmd
	CFDel(ctx context.Context, key string, element interface{}) *redis.BoolCmd
	CFExists(ctx context.Context, key string, element interface{}) *redis.BoolCmd

	io.Closer
}

// RedisCacheAdaptor is a minimal interface to use redis as cache.
type RedisCacheAdaptor interface {
	Add(ctx context.Context, key string, value []byte, expiration time.Duration) (bool, error)
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Delete(ctx context.Context, key string) error
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	io.Closer
}

type conf struct {
	strict    bool
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

func (c *conf) SetDefaults(prefix string) {
	c.strict = false
	c.filterKey = prefix + `-benthos-%Y%m%d`
	c.location = time.UTC
}

// Option functional option type
type Option func(*conf)

// WithStrict can enable the strict mode. Not supported operations will fail.
func WithStrict(strict bool) Option {
	return func(c *conf) {
		c.strict = strict
	}
}

func WithFilterKey(filterKey string) Option {
	return func(c *conf) {
		c.filterKey = filterKey
	}
}

func WithLocation(location *time.Location) Option {
	return func(c *conf) {
		c.location = location
	}
}

func WithClock(clock clock.Clock) Option {
	return func(c *conf) {
		c.clock = clock
	}
}

var errDeleteOperationNotSupported = errors.New("delete operation not supported")

type defaultRedisCacheAdaptor struct {
	client RedisMinimalInterface
}

// NewDefaultRedisCacheAdaptor ctor.
func NewDefaultRedisCacheAdaptor(client RedisMinimalInterface) RedisCacheAdaptor {
	return &defaultRedisCacheAdaptor{client: client}
}

func (c *defaultRedisCacheAdaptor) Add(ctx context.Context, key string, value []byte, expiration time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, expiration).Result()
}

func (c *defaultRedisCacheAdaptor) Get(ctx context.Context, key string) ([]byte, bool, error) {
	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}

		return nil, false, err
	}

	return []byte(result), true, nil
}

func (c *defaultRedisCacheAdaptor) Delete(ctx context.Context, key string) error {
	_, err := c.client.Del(ctx, key).Result()

	return err
}

func (c *defaultRedisCacheAdaptor) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return c.client.Set(ctx, key, value, expiration).Err()
}

func (c *defaultRedisCacheAdaptor) Close() error {
	return c.client.Close()
}

type bloomFilterRedisCacheAdaptor struct {
	client    RedisBloomFilterMinimalInterface
	strict    bool
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

// NewBloomFilterRedisCacheAdaptor ctor.
func NewBloomFilterRedisCacheAdaptor(client RedisBloomFilterMinimalInterface, opts ...Option) RedisCacheAdaptor {
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

func (c *bloomFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}

type cuckooFilterRedisCacheAdaptor struct {
	client    RedisCuckooFilterMinimalInterface
	filterKey string
	location  *time.Location
	clock     clock.Clock
}

// NewCuckooFilterRedisCacheAdaptor ctor.
func NewCuckooFilterRedisCacheAdaptor(client RedisCuckooFilterMinimalInterface, opts ...Option) RedisCacheAdaptor {
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

func (c *cuckooFilterRedisCacheAdaptor) Delete(ctx context.Context, key string) error {
	filterKey := c.buildFilterKey()

	return c.client.CFDel(ctx, filterKey, key).Err()
}

func (c *cuckooFilterRedisCacheAdaptor) Close() error {
	return c.client.Close()
}
