package pure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestMultilevelCacheErrors(t *testing.T) {
	strmBuilder := service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(`
input:
  generate:
    interval: 1ns
    count: 1
    mapping: 'root = "hello world"'

output:
  cache:
    target: testing

cache_resources:
  - label: testing
    multilevel: []

logger:
  level: NONE
`))

	_, err := strmBuilder.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected at least two cache levels")

	strmBuilder = service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(`
input:
  generate:
    interval: 1ns
    count: 1
    mapping: 'root = "hello world"'

output:
  cache:
    target: test

cache_resources:
  - label: test
    multilevel:
      - foo

logger:
  level: NONE
`))

	_, err = strmBuilder.Build()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected at least two cache levels")

	strmBuilder = service.NewStreamBuilder()
	require.NoError(t, strmBuilder.SetYAML(`
input:
  generate:
    interval: 1ns
    count: 1
    mapping: 'root = "hello world"'

output:
  cache:
    target: test

cache_resources:
  - label: test
    multilevel: [ foo, bar ]

  - label: foo
    memory: {}

  - label: bar
    memory: {}

logger:
  level: NONE
`))

	s, err := strmBuilder.Build()
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	require.NoError(t, s.Run(tCtx))
}

type mockCacheProv struct {
	caches map[string]service.Cache
}

func (m *mockCacheProv) AccessCache(ctx context.Context, name string, fn func(c service.Cache)) error {
	c, ok := m.caches[name]
	if !ok {
		return errors.New("cache not found")
	}
	fn(c)
	return nil
}

func TestMultilevelCacheGetting(t *testing.T) {
	memCache1 := newMemCache(time.Minute, 0, 1, nil)
	memCache2 := newMemCache(time.Minute, 0, 1, nil)
	p := &mockCacheProv{
		caches: map[string]service.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	c, err := newMultilevelCache([]string{"foo", "bar"}, p, nil)
	require.NoError(t, err)

	ctx := context.Background()

	_, err = c.Get(ctx, "not_exist")
	assert.Equal(t, err, service.ErrKeyNotFound)

	require.NoError(t, memCache2.Set(ctx, "foo", []byte("test value 1"), nil))

	val, err := c.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	require.NoError(t, memCache2.Delete(ctx, "foo"))

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	_, err = memCache2.Get(ctx, "foo")
	assert.Equal(t, err, service.ErrKeyNotFound)
}

func TestMultilevelCacheSet(t *testing.T) {
	memCache1 := newMemCache(time.Minute, 0, 1, nil)
	memCache2 := newMemCache(time.Minute, 0, 1, nil)
	p := &mockCacheProv{
		caches: map[string]service.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	c, err := newMultilevelCache([]string{"foo", "bar"}, p, nil)
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, c.Set(ctx, "foo", []byte("test value 1"), nil))

	val, err := memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Set(ctx, "foo", []byte("test value 2"), nil)
	require.NoError(t, err)
	require.NoError(t, err)

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 2"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 2"))
}

func TestMultilevelCacheDelete(t *testing.T) {
	memCache1 := newMemCache(time.Minute, 0, 1, nil)
	memCache2 := newMemCache(time.Minute, 0, 1, nil)
	p := &mockCacheProv{
		caches: map[string]service.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	c, err := newMultilevelCache([]string{"foo", "bar"}, p, nil)
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, memCache2.Set(ctx, "foo", []byte("test value 1"), nil))

	require.NoError(t, c.Delete(ctx, "foo"))

	_, err = memCache1.Get(ctx, "foo")
	assert.Equal(t, err, service.ErrKeyNotFound)

	_, err = memCache2.Get(ctx, "foo")
	assert.Equal(t, err, service.ErrKeyNotFound)

	require.NoError(t, memCache1.Set(ctx, "foo", []byte("test value 1"), nil))
	require.NoError(t, memCache2.Set(ctx, "foo", []byte("test value 2"), nil))

	err = c.Delete(ctx, "foo")
	require.NoError(t, err)

	_, err = memCache1.Get(ctx, "foo")
	assert.Equal(t, err, service.ErrKeyNotFound)

	_, err = memCache2.Get(ctx, "foo")
	assert.Equal(t, err, service.ErrKeyNotFound)
}

func TestMultilevelCacheAdd(t *testing.T) {
	memCache1 := newMemCache(time.Minute, 0, 1, nil)
	memCache2 := newMemCache(time.Minute, 0, 1, nil)
	p := &mockCacheProv{
		caches: map[string]service.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	c, err := newMultilevelCache([]string{"foo", "bar"}, p, nil)
	require.NoError(t, err)

	ctx := context.Background()

	err = c.Add(ctx, "foo", []byte("test value 1"), nil)
	require.NoError(t, err)

	val, err := memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Add(ctx, "foo", []byte("test value 2"), nil)
	assert.Equal(t, err, service.ErrKeyAlreadyExists)

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	err = memCache2.Delete(ctx, "foo")
	require.NoError(t, err)

	err = c.Add(ctx, "foo", []byte("test value 3"), nil)
	assert.Equal(t, err, service.ErrKeyAlreadyExists)

	err = memCache1.Delete(ctx, "foo")
	require.NoError(t, err)

	err = c.Add(ctx, "foo", []byte("test value 4"), nil)
	require.NoError(t, err)

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 4"))

	err = memCache1.Delete(ctx, "foo")
	require.NoError(t, err)

	err = c.Add(ctx, "foo", []byte("test value 5"), nil)
	assert.Equal(t, err, service.ErrKeyAlreadyExists)
}

func TestMultilevelCacheAddMoreCaches(t *testing.T) {
	memCache1 := newMemCache(time.Minute, 0, 1, nil)
	memCache2 := newMemCache(time.Minute, 0, 1, nil)
	memCache3 := newMemCache(time.Minute, 0, 1, nil)
	p := &mockCacheProv{
		caches: map[string]service.Cache{
			"foo": memCache1,
			"bar": memCache2,
			"baz": memCache3,
		},
	}

	c, err := newMultilevelCache([]string{"foo", "bar", "baz"}, p, nil)
	require.NoError(t, err)

	ctx := context.Background()

	err = c.Add(ctx, "foo", []byte("test value 1"), nil)
	require.NoError(t, err)

	val, err := memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache3.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Add(ctx, "foo", []byte("test value 2"), nil)
	assert.Equal(t, err, service.ErrKeyAlreadyExists)

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache3.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 1"))

	err = memCache1.Delete(ctx, "foo")
	require.NoError(t, err)

	err = memCache2.Delete(ctx, "foo")
	require.NoError(t, err)

	err = c.Add(ctx, "foo", []byte("test value 3"), nil)
	assert.Equal(t, err, service.ErrKeyAlreadyExists)

	err = memCache3.Delete(ctx, "foo")
	require.NoError(t, err)

	err = c.Add(ctx, "foo", []byte("test value 4"), nil)
	require.NoError(t, err)

	val, err = memCache1.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache2.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache3.Get(ctx, "foo")
	require.NoError(t, err)
	assert.Equal(t, val, []byte("test value 4"))
}
