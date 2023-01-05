package service

import (
	"context"
	"errors"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
)

// Errors returned by cache types.
var (
	ErrKeyAlreadyExists = errors.New("key already exists")
	ErrKeyNotFound      = errors.New("key does not exist")
)

// Cache is an interface implemented by Benthos caches.
type Cache interface {
	// Get a cache item.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set a cache item, specifying an optional TTL. It is okay for caches to
	// ignore the ttl parameter if it isn't possible to implement.
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Add is the same operation as Set except that it returns an error if the
	// key already exists. It is okay for caches to return nil on duplicates if
	// it isn't possible to implement.
	Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Delete attempts to remove a key. If the key does not exist then it is
	// considered correct to return an error, however, for cache implementations
	// where it is difficult to determine this then it is acceptable to return
	// nil.
	Delete(ctx context.Context, key string) error

	Closer
}

// CacheItem represents an individual cache item.
type CacheItem struct {
	Key   string
	Value []byte
	TTL   *time.Duration
}

// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
type batchedCache interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, keyValues ...CacheItem) error
}

//------------------------------------------------------------------------------

// Implements types.Cache.
type airGapCache struct {
	c  Cache
	cm batchedCache
}

func newAirGapCache(c Cache, stats metrics.Type) cache.V1 {
	ag := &airGapCache{c: c, cm: nil}
	ag.cm, _ = c.(batchedCache)
	return cache.MetricsForCache(ag, stats)
}

func (a *airGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := a.c.Get(ctx, key)
	if errors.Is(err, ErrKeyNotFound) {
		err = component.ErrKeyNotFound
	}
	return b, err
}

func (a *airGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return a.c.Set(ctx, key, value, ttl)
}

func (a *airGapCache) SetMulti(ctx context.Context, keyValues map[string]cache.TTLItem) error {
	if a.cm != nil {
		items := make([]CacheItem, 0, len(keyValues))
		for k, v := range keyValues {
			items = append(items, CacheItem{
				Key:   k,
				Value: v.Value,
				TTL:   v.TTL,
			})
		}
		return a.cm.SetMulti(ctx, items...)
	}
	for k, v := range keyValues {
		if err := a.c.Set(ctx, k, v.Value, v.TTL); err != nil {
			return err
		}
	}
	return nil
}

func (a *airGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	err := a.c.Add(ctx, key, value, ttl)
	if errors.Is(err, ErrKeyAlreadyExists) {
		err = component.ErrKeyAlreadyExists
	}
	return err
}

func (a *airGapCache) Delete(ctx context.Context, key string) error {
	return a.c.Delete(ctx, key)
}

func (a *airGapCache) Close(ctx context.Context) error {
	return a.c.Close(ctx)
}

//------------------------------------------------------------------------------

// Implements Cache around a types.Cache.
type reverseAirGapCache struct {
	c cache.V1
}

func newReverseAirGapCache(c cache.V1) *reverseAirGapCache {
	return &reverseAirGapCache{c}
}

func (r *reverseAirGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := r.c.Get(ctx, key)
	if errors.Is(err, component.ErrKeyNotFound) {
		err = ErrKeyNotFound
	}
	return b, err
}

func (r *reverseAirGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return r.c.Set(ctx, key, value, ttl)
}

func (r *reverseAirGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) (err error) {
	if err = r.c.Add(ctx, key, value, ttl); errors.Is(err, component.ErrKeyAlreadyExists) {
		err = ErrKeyAlreadyExists
	}
	return
}

func (r *reverseAirGapCache) Delete(ctx context.Context, key string) error {
	return r.c.Delete(ctx, key)
}

func (r *reverseAirGapCache) Close(ctx context.Context) error {
	return r.c.Close(ctx)
}
