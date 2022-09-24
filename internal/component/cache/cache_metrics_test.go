package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
)

type testCacheItem struct {
	b   []byte
	ttl *time.Duration
}

type closableCache struct {
	m      map[string]testCacheItem
	err    error
	closed bool
}

func (c *closableCache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	i, ok := c.m[key]
	if !ok {
		return nil, component.ErrKeyNotFound
	}
	return i.b, nil
}

func (c *closableCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if c.err != nil {
		return c.err
	}
	c.m[key] = testCacheItem{
		b: value, ttl: ttl,
	}
	return nil
}

func (c *closableCache) SetMulti(ctx context.Context, keyValues map[string]TTLItem) error {
	if c.err != nil {
		return c.err
	}
	for k, v := range keyValues {
		c.m[k] = testCacheItem{
			b: v.Value, ttl: v.TTL,
		}
	}
	return nil
}

func (c *closableCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	if c.err != nil {
		return c.err
	}
	if _, ok := c.m[key]; ok {
		return component.ErrKeyAlreadyExists
	}
	c.m[key] = testCacheItem{
		b: value, ttl: ttl,
	}
	return nil
}

func (c *closableCache) Delete(ctx context.Context, key string) error {
	if c.err != nil {
		return c.err
	}
	delete(c.m, key)
	return nil
}

func (c *closableCache) Close(ctx context.Context) error {
	c.closed = true
	return nil
}

func TestCacheAirGapShutdown(t *testing.T) {
	rl := &closableCache{}
	agrl := MetricsForCache(rl, metrics.Noop())

	err := agrl.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, rl.closed)
}

func TestCacheAirGapGet(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{
			"foo": {
				b: []byte("bar"),
			},
		},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	b, err := agrl.Get(ctx, "foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(b))

	_, err = agrl.Get(ctx, "not exist")
	assert.Equal(t, err, component.ErrKeyNotFound)
	assert.EqualError(t, err, "key does not exist")
}

func TestCacheAirGapSet(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	err := agrl.Set(ctx, "foo", []byte("bar"), nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: nil,
		},
	}, rl.m)

	err = agrl.Set(ctx, "foo", []byte("baz"), nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("baz"),
			ttl: nil,
		},
	}, rl.m)
}

func TestCacheAirGapSetMultiWithTTL(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	ctx := context.Background()
	agrl := MetricsForCache(rl, metrics.Noop())

	ttl1, ttl2 := time.Second, time.Millisecond

	err := agrl.SetMulti(ctx, map[string]TTLItem{
		"first": {
			Value: []byte("bar"),
			TTL:   &ttl1,
		},
		"second": {
			Value: []byte("baz"),
			TTL:   &ttl2,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"first": {
			b:   []byte("bar"),
			ttl: &ttl1,
		},
		"second": {
			b:   []byte("baz"),
			ttl: &ttl2,
		},
	}, rl.m)
}

func TestCacheAirGapSetWithTTL(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	ttl1, ttl2 := time.Second, time.Millisecond
	err := agrl.Set(ctx, "foo", []byte("bar"), &ttl1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: &ttl1,
		},
	}, rl.m)

	err = agrl.Set(ctx, "foo", []byte("baz"), &ttl2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("baz"),
			ttl: &ttl2,
		},
	}, rl.m)
}

func TestCacheAirGapAdd(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	err := agrl.Add(ctx, "foo", []byte("bar"), nil)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: nil,
		},
	}, rl.m)

	err = agrl.Add(ctx, "foo", []byte("baz"), nil)
	assert.Equal(t, err, component.ErrKeyAlreadyExists)
	assert.EqualError(t, err, "key already exists")
}

func TestCacheAirGapAddWithTTL(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	ttl := time.Second
	err := agrl.Add(ctx, "foo", []byte("bar"), &ttl)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: &ttl,
		},
	}, rl.m)

	err = agrl.Add(ctx, "foo", []byte("baz"), nil)
	assert.Equal(t, err, component.ErrKeyAlreadyExists)
	assert.EqualError(t, err, "key already exists")
}

func TestCacheAirGapDelete(t *testing.T) {
	ctx := context.Background()
	rl := &closableCache{
		m: map[string]testCacheItem{
			"foo": {
				b: []byte("bar"),
			},
		},
	}
	agrl := MetricsForCache(rl, metrics.Noop())

	err := agrl.Delete(ctx, "foo")
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{}, rl.m)
}
