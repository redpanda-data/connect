package service

import (
	"context"
	"errors"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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

//------------------------------------------------------------------------------

// Implements types.Cache
type airGapCache struct {
	c Cache

	sig *shutdown.Signaller
}

func newAirGapCache(c Cache, stats metrics.Type) types.Cache {
	return cache.NewV2ToV1Cache(&airGapCache{c, shutdown.NewSignaller()}, stats)
}

func (a *airGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := a.c.Get(ctx, key)
	if errors.Is(err, types.ErrKeyNotFound) {
		err = ErrKeyNotFound
	}
	return b, err
}

func (a *airGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return a.c.Set(ctx, key, value, ttl)
}

func (a *airGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	err := a.c.Add(ctx, key, value, ttl)
	if errors.Is(err, types.ErrKeyAlreadyExists) {
		err = ErrKeyAlreadyExists
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

// Implements Cache around a types.Cache
type reverseAirGapCache struct {
	c types.Cache
}

func newReverseAirGapCache(c types.Cache) *reverseAirGapCache {
	return &reverseAirGapCache{c}
}

func (r *reverseAirGapCache) Get(ctx context.Context, key string) ([]byte, error) {
	return r.c.Get(key)
}

func (r *reverseAirGapCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if cttl, ok := r.c.(types.CacheWithTTL); ok {
		return cttl.SetWithTTL(key, value, ttl)
	}
	return r.c.Set(key, value)
}

func (r *reverseAirGapCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if cttl, ok := r.c.(types.CacheWithTTL); ok {
		return cttl.AddWithTTL(key, value, ttl)
	}
	return r.c.Add(key, value)
}

func (r *reverseAirGapCache) Delete(ctx context.Context, key string) error {
	return r.c.Delete(key)
}

func (r *reverseAirGapCache) Close(ctx context.Context) error {
	r.c.CloseAsync()
	for {
		// Gross but will do for now until we replace these with context params.
		if err := r.c.WaitForClose(time.Millisecond * 100); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}
