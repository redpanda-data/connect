package cache

import (
	"net/http"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
)

//------------------------------------------------------------------------------

type fakeMgr struct {
	caches map[string]types.Cache
}

func (f *fakeMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeMgr) GetCache(name string) (types.Cache, error) {
	if c, exists := f.caches[name]; exists {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}
func (f *fakeMgr) GetRateLimit(name string) (types.RateLimit, error) {
	return nil, types.ErrRateLimitNotFound
}
func (f *fakeMgr) GetPlugin(name string) (interface{}, error) {
	return nil, types.ErrPluginNotFound
}
func (f *fakeMgr) GetPipe(name string) (<-chan types.Transaction, error) {
	return nil, types.ErrPipeNotFound
}
func (f *fakeMgr) SetPipe(name string, prod <-chan types.Transaction)   {}
func (f *fakeMgr) UnsetPipe(name string, prod <-chan types.Transaction) {}

//------------------------------------------------------------------------------

func TestMultilevelErrors(t *testing.T) {
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": nil,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel

	if _, err := New(conf, &mgr, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from empty levels")
	}

	conf.Multilevel = []string{"foo"}

	if _, err := New(conf, &mgr, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from only one level")
	}

	conf.Multilevel = []string{"foo", "bar"}

	if _, err := New(conf, &mgr, log.Noop(), metrics.Noop()); err == nil {
		t.Error("Expected error from not existing level")
	}
}

func TestMultilevelCacheGetting(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Get("not_exist")
	assert.Equal(t, err, types.ErrKeyNotFound)

	if err = memCache2.Set("foo", []byte("test value 1")); err != nil {
		t.Fatal(err)
	}

	val, err := c.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	if err = memCache2.Delete("foo"); err != nil {
		t.Fatal(err)
	}

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	_, err = memCache2.Get("foo")
	assert.Equal(t, err, types.ErrKeyNotFound)
}

func TestMultilevelCacheSet(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	err = c.Set("foo", []byte("test value 1"))
	assert.Equal(t, err, nil)

	val, err := memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Set("foo", []byte("test value 2"))
	assert.Equal(t, err, nil)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))
}

func TestMultilevelCacheMultiSet(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	err = c.SetMulti(map[string][]byte{
		"foo": []byte("test value 1"),
		"bar": []byte("test value 2"),
	})
	assert.Equal(t, err, nil)

	val, err := memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache1.Get("bar")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))

	val, err = memCache2.Get("bar")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))

	err = c.Set("foo", []byte("test value 3"))
	assert.Equal(t, err, nil)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 3"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 3"))

	val, err = memCache1.Get("bar")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))

	val, err = memCache2.Get("bar")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 2"))
}

func TestMultilevelCacheDelete(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = memCache2.Set("foo", []byte("test value 1")); err != nil {
		t.Fatal(err)
	}

	err = c.Delete("foo")
	assert.Equal(t, err, nil)

	_, err = memCache1.Get("foo")
	assert.Equal(t, err, types.ErrKeyNotFound)

	_, err = memCache2.Get("foo")
	assert.Equal(t, err, types.ErrKeyNotFound)

	if err = memCache1.Set("foo", []byte("test value 1")); err != nil {
		t.Fatal(err)
	}
	if err = memCache2.Set("foo", []byte("test value 2")); err != nil {
		t.Fatal(err)
	}

	err = c.Delete("foo")
	assert.Equal(t, err, nil)

	_, err = memCache1.Get("foo")
	assert.Equal(t, err, types.ErrKeyNotFound)

	_, err = memCache2.Get("foo")
	assert.Equal(t, err, types.ErrKeyNotFound)
}

func TestMultilevelCacheAdd(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	err = c.Add("foo", []byte("test value 1"))
	assert.Equal(t, err, nil)

	val, err := memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Add("foo", []byte("test value 2"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	err = memCache2.Delete("foo")
	assert.Equal(t, err, nil)

	err = c.Add("foo", []byte("test value 3"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)

	err = memCache1.Delete("foo")
	assert.Equal(t, err, nil)

	err = c.Add("foo", []byte("test value 4"))
	assert.Equal(t, err, nil)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 4"))

	err = memCache1.Delete("foo")
	assert.Equal(t, err, nil)

	err = c.Add("foo", []byte("test value 5"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)
}

func TestMultilevelCacheAddMoreCaches(t *testing.T) {
	memCache1, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache2, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	memCache3, err := NewMemory(NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := fakeMgr{
		caches: map[string]types.Cache{
			"foo": memCache1,
			"bar": memCache2,
			"baz": memCache3,
		},
	}

	conf := NewConfig()
	conf.Type = TypeMultilevel
	conf.Multilevel = []string{"foo", "bar", "baz"}

	c, err := New(conf, &mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	err = c.Add("foo", []byte("test value 1"))
	assert.Equal(t, err, nil)

	val, err := memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache3.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	err = c.Add("foo", []byte("test value 2"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	val, err = memCache3.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 1"))

	err = memCache1.Delete("foo")
	assert.Equal(t, err, nil)

	err = memCache2.Delete("foo")
	assert.Equal(t, err, nil)

	err = c.Add("foo", []byte("test value 3"))
	assert.Equal(t, err, types.ErrKeyAlreadyExists)

	err = memCache3.Delete("foo")
	assert.Equal(t, err, nil)

	err = c.Add("foo", []byte("test value 4"))
	assert.Equal(t, err, nil)

	val, err = memCache1.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache2.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 4"))

	val, err = memCache3.Get("foo")
	assert.Equal(t, err, nil)
	assert.Equal(t, val, []byte("test value 4"))
}

//------------------------------------------------------------------------------
