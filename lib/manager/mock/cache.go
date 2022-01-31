package mock

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
)

var _ types.Cache = &Cache{}

// Cache provides a mock cache implementation
type Cache struct {
	Values map[string]string
}

// Get a mock cache item
func (c *Cache) Get(key string) ([]byte, error) {
	i, ok := c.Values[key]
	if !ok {
		return nil, types.ErrKeyNotFound
	}
	return []byte(i), nil
}

// Set a mock cache item
func (c *Cache) Set(key string, value []byte) error {
	c.Values[key] = string(value)
	return nil
}

// SetMulti sets multiple mock cache items
func (c *Cache) SetMulti(kvs map[string][]byte) error {
	for k, v := range kvs {
		c.Values[k] = string(v)
	}
	return nil
}

// Add a mock cache item
func (c *Cache) Add(key string, value []byte) error {
	if _, ok := c.Values[key]; ok {
		return types.ErrKeyAlreadyExists
	}
	c.Values[key] = string(value)
	return nil

}

// Delete a mock cache item
func (c *Cache) Delete(key string) error {
	delete(c.Values, key)
	return nil
}

// CloseAsync does nothing
func (c *Cache) CloseAsync() {
}

// WaitForClose does nothing
func (c *Cache) WaitForClose(t time.Duration) error {
	return nil
}
