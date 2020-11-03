package writer

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheSingle(t *testing.T) {
	c := &basicCache{
		values: map[string]string{},
	}

	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": c,
		},
	}

	conf := NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.New([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	assert.Equal(t, map[string]string{
		"1": `{"id":"1","value":"first"}`,
	}, c.values)
}

func TestCacheBatch(t *testing.T) {
	c := &basicCache{
		values: map[string]string{},
	}

	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": c,
		},
	}

	conf := NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.New([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
		[]byte(`{"id":"2","value":"second"}`),
		[]byte(`{"id":"3","value":"third"}`),
		[]byte(`{"id":"4","value":"fourth"}`),
	})))

	assert.Equal(t, map[string]string{
		"1": `{"id":"1","value":"first"}`,
		"2": `{"id":"2","value":"second"}`,
		"3": `{"id":"3","value":"third"}`,
		"4": `{"id":"4","value":"fourth"}`,
	}, c.values)
}

func TestCacheSingleTTL(t *testing.T) {
	c := &ttlCache{
		values: map[string]ttlCacheItem{},
	}

	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": c,
		},
	}

	conf := NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.New([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	twosec := time.Second * 2

	assert.Equal(t, map[string]ttlCacheItem{
		"1": {
			value: `{"id":"1","value":"first"}`,
			ttl:   &twosec,
		},
	}, c.values)
}

func TestCacheBatchTTL(t *testing.T) {
	c := &ttlCache{
		values: map[string]ttlCacheItem{},
	}

	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": c,
		},
	}

	conf := NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.New([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
		[]byte(`{"id":"2","value":"second"}`),
		[]byte(`{"id":"3","value":"third"}`),
		[]byte(`{"id":"4","value":"fourth"}`),
	})))

	twosec := time.Second * 2

	assert.Equal(t, map[string]ttlCacheItem{
		"1": {
			value: `{"id":"1","value":"first"}`,
			ttl:   &twosec,
		},
		"2": {
			value: `{"id":"2","value":"second"}`,
			ttl:   &twosec,
		},
		"3": {
			value: `{"id":"3","value":"third"}`,
			ttl:   &twosec,
		},
		"4": {
			value: `{"id":"4","value":"fourth"}`,
			ttl:   &twosec,
		},
	}, c.values)
}

//------------------------------------------------------------------------------

type fakeMgr struct {
	caches     map[string]types.Cache
	ratelimits map[string]types.RateLimit
}

func (f *fakeMgr) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
}
func (f *fakeMgr) GetCache(name string) (types.Cache, error) {
	if c, exists := f.caches[name]; exists {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}
func (f *fakeMgr) GetCondition(name string) (types.Condition, error) {
	return nil, types.ErrConditionNotFound
}
func (f *fakeMgr) GetRateLimit(name string) (types.RateLimit, error) {
	if r, exists := f.ratelimits[name]; exists {
		return r, nil
	}
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

type basicCache struct {
	values map[string]string
}

func (b *basicCache) Get(key string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (b *basicCache) Set(key string, value []byte) error {
	b.values[key] = string(value)
	return nil
}

func (b *basicCache) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		b.values[k] = string(v)
	}
	return nil
}

func (b *basicCache) Add(key string, value []byte) error {
	return errors.New("not implemented")
}

func (b *basicCache) Delete(key string) error {
	return errors.New("not implemented")
}

func (b *basicCache) CloseAsync() {}

func (b *basicCache) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

type ttlCacheItem struct {
	value string
	ttl   *time.Duration
}

type ttlCache struct {
	values map[string]ttlCacheItem
}

func (t *ttlCache) Get(key string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (t *ttlCache) Set(key string, value []byte) error {
	t.values[key] = ttlCacheItem{
		value: string(value),
	}
	return nil
}

func (t *ttlCache) SetWithTTL(key string, value []byte, ttl *time.Duration) error {
	t.values[key] = ttlCacheItem{
		value: string(value),
		ttl:   ttl,
	}
	return nil
}

func (t *ttlCache) SetMulti(items map[string][]byte) error {
	for k, v := range items {
		t.values[k] = ttlCacheItem{
			value: string(v),
		}
	}
	return nil
}

func (t *ttlCache) SetMultiWithTTL(items map[string]types.CacheTTLItem) error {
	for k, v := range items {
		t.values[k] = ttlCacheItem{
			value: string(v.Value),
			ttl:   v.TTL,
		}
	}
	return nil
}

func (t *ttlCache) Add(key string, value []byte) error {
	return errors.New("not implemented")
}

func (t *ttlCache) AddWithTTL(key string, value []byte, ttl *time.Duration) error {
	return errors.New("not implemented")
}

func (t *ttlCache) Delete(key string) error {
	return errors.New("not implemented")
}

func (t *ttlCache) CloseAsync() {}

func (t *ttlCache) WaitForClose(time.Duration) error {
	return nil
}
