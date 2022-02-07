package mock

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.Cache = &Cache{}

// Cache provides a mock cache implementation
type Cache struct {
	Values map[string]string
}

// Get a mock cache item
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	i, ok := c.Values[key]
	if !ok {
		return nil, types.ErrKeyNotFound
	}
	return []byte(i), nil
}

// Set a mock cache item
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	c.Values[key] = string(value)
	return nil
}

// SetMulti sets multiple mock cache items
func (c *Cache) SetMulti(ctx context.Context, kvs map[string]types.CacheTTLItem) error {
	for k, v := range kvs {
		c.Values[k] = string(v.Value)
	}
	return nil
}

// Add a mock cache item
func (c *Cache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if _, ok := c.Values[key]; ok {
		return types.ErrKeyAlreadyExists
	}
	c.Values[key] = string(value)
	return nil

}

// Delete a mock cache item
func (c *Cache) Delete(ctx context.Context, key string) error {
	delete(c.Values, key)
	return nil
}

// Close does nothing
func (c *Cache) Close(ctx context.Context) error {
	return nil
}
