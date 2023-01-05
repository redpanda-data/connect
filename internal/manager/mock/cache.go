package mock

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
)

// CacheItem represents a cached key/ttl pair.
type CacheItem struct {
	Value string
	TTL   *time.Duration
}

// Cache provides a mock cache implementation.
type Cache struct {
	Values map[string]CacheItem
}

// Get a mock cache item.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	i, ok := c.Values[key]
	if !ok {
		return nil, component.ErrKeyNotFound
	}
	return []byte(i.Value), nil
}

// Set a mock cache item.
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	c.Values[key] = CacheItem{
		Value: string(value),
		TTL:   ttl,
	}
	return nil
}

// SetMulti sets multiple mock cache items.
func (c *Cache) SetMulti(ctx context.Context, kvs map[string]cache.TTLItem) error {
	for k, v := range kvs {
		c.Values[k] = CacheItem{
			Value: string(v.Value),
			TTL:   v.TTL,
		}
	}
	return nil
}

// Add a mock cache item.
func (c *Cache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if _, ok := c.Values[key]; ok {
		return component.ErrKeyAlreadyExists
	}
	c.Values[key] = CacheItem{
		Value: string(value),
		TTL:   ttl,
	}
	return nil
}

// Delete a mock cache item.
func (c *Cache) Delete(ctx context.Context, key string) error {
	delete(c.Values, key)
	return nil
}

// Close does nothing.
func (c *Cache) Close(ctx context.Context) error {
	return nil
}
