package cache

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
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
		return nil, types.ErrKeyNotFound
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

func (c *closableCache) SetMulti(ctx context.Context, keyValues map[string]types.CacheTTLItem) error {
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
		return types.ErrKeyAlreadyExists
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
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	err := agrl.WaitForClose(time.Millisecond * 5)
	assert.EqualError(t, err, "action timed out")
	assert.False(t, rl.closed)

	agrl.CloseAsync()
	err = agrl.WaitForClose(time.Millisecond * 5)
	assert.NoError(t, err)
	assert.True(t, rl.closed)
}

func TestCacheAirGapGet(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{
			"foo": {
				b: []byte("bar"),
			},
		},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	b, err := agrl.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(b))

	_, err = agrl.Get("not exist")
	assert.Equal(t, err, types.ErrKeyNotFound)
	assert.EqualError(t, err, "key does not exist")
}

func TestCacheAirGapSet(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	err := agrl.Set("foo", []byte("bar"))
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: nil,
		},
	}, rl.m)

	err = agrl.Set("foo", []byte("baz"))
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("baz"),
			ttl: nil,
		},
	}, rl.m)
}

func TestCacheAirGapSetMulti(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	err := agrl.SetMulti(map[string][]byte{
		"first":  []byte("bar"),
		"second": []byte("baz"),
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"first": {
			b:   []byte("bar"),
			ttl: nil,
		},
		"second": {
			b:   []byte("baz"),
			ttl: nil,
		},
	}, rl.m)
}

func TestCacheAirGapSetMultiWithTTL(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop()).(types.CacheWithTTL)

	ttl1, ttl2 := time.Second, time.Millisecond

	err := agrl.SetMultiWithTTL(map[string]types.CacheTTLItem{
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
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop()).(types.CacheWithTTL)

	ttl1, ttl2 := time.Second, time.Millisecond
	err := agrl.SetWithTTL("foo", []byte("bar"), &ttl1)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: &ttl1,
		},
	}, rl.m)

	err = agrl.SetWithTTL("foo", []byte("baz"), &ttl2)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("baz"),
			ttl: &ttl2,
		},
	}, rl.m)
}

func TestCacheAirGapAdd(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	err := agrl.Add("foo", []byte("bar"))
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: nil,
		},
	}, rl.m)

	err = agrl.Add("foo", []byte("baz"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)
	assert.EqualError(t, err, "key already exists")
}

func TestCacheAirGapAddWithTTL(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop()).(types.CacheWithTTL)

	ttl := time.Second
	err := agrl.AddWithTTL("foo", []byte("bar"), &ttl)
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{
		"foo": {
			b:   []byte("bar"),
			ttl: &ttl,
		},
	}, rl.m)

	err = agrl.AddWithTTL("foo", []byte("baz"), nil)
	assert.Equal(t, err, types.ErrKeyAlreadyExists)
	assert.EqualError(t, err, "key already exists")
}

func TestCacheAirGapDelete(t *testing.T) {
	rl := &closableCache{
		m: map[string]testCacheItem{
			"foo": {
				b: []byte("bar"),
			},
		},
	}
	agrl := NewV2ToV1Cache(rl, metrics.Noop())

	err := agrl.Delete("foo")
	assert.NoError(t, err)
	assert.Equal(t, map[string]testCacheItem{}, rl.m)
}
