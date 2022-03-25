package cache

import (
	"context"
	"time"
)

// TTLItem contains a value to cache along with an optional TTL.
type TTLItem struct {
	Value []byte
	TTL   *time.Duration
}

// V1 Defines a common interface of cache implementations.
type V1 interface {
	// Get attempts to locate and return a cached value by its key, returns an
	// error if the key does not exist or if the command fails.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set attempts to set the value of a key, returns an error if the command
	// fails.
	Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// SetMulti attempts to set the value of multiple keys, returns an error if
	// any of the keys fail.
	SetMulti(ctx context.Context, items map[string]TTLItem) error

	// Add attempts to set the value of a key only if the key does not already
	// exist, returns an error if the key already exists or if the command
	// fails.
	Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error

	// Delete attempts to remove a key. Returns an error if a failure occurs.
	Delete(ctx context.Context, key string) error

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}
