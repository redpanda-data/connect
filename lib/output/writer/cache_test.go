package writer_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestCacheSingle(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]string{}

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.New([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	assert.Equal(t, map[string]string{
		"1": `{"id":"1","value":"first"}`,
	}, mgr.Caches["foocache"])
}

func TestCacheBatch(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]string{}

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
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
	}, mgr.Caches["foocache"])
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

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
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

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
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

type ttlCacheItem struct {
	value string
	ttl   *time.Duration
}

type ttlCache struct {
	values map[string]ttlCacheItem
}

func (t *ttlCache) Get(ctx context.Context, key string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (t *ttlCache) Set(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	t.values[key] = ttlCacheItem{
		value: string(value),
		ttl:   ttl,
	}
	return nil
}

func (t *ttlCache) SetMulti(ctx context.Context, items map[string]types.CacheTTLItem) error {
	for k, v := range items {
		t.values[k] = ttlCacheItem{
			value: string(v.Value),
			ttl:   v.TTL,
		}
	}
	return nil
}

func (t *ttlCache) Add(ctx context.Context, key string, value []byte, ttl *time.Duration) error {
	return errors.New("not implemented")
}

func (t *ttlCache) Delete(ctx context.Context, key string) error {
	return errors.New("not implemented")
}

func (t *ttlCache) Close(context.Context) error { return nil }

func TestCacheBasic(t *testing.T) {
	mgrConf := manager.NewResourceConfig()

	fooCache := cache.NewConfig()
	fooCache.Label = "foo"

	mgrConf.ResourceCaches = append(mgrConf.ResourceCaches, fooCache)

	mgr, err := manager.NewV2(mgrConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	cacheConf := writer.NewCacheConfig()
	cacheConf.Target = "foo"
	cacheConf.Key = "${!json(\"key\")}"

	c, err := writer.NewCache(cacheConf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := map[string]string{}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf(`{"key":"%v","test":"hello world"}`, key)
		exp[key] = value
		if err := c.Write(message.New([][]byte{[]byte(value)})); err != nil {
			t.Fatal(err)
		}
	}

	memCache, err := mgr.GetCache("foo")
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range exp {
		res, err := memCache.Get(context.Background(), k)
		if err != nil {
			t.Errorf("Missing key '%v': %v", k, err)
		}
		if exp, act := v, string(res); exp != act {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}

func TestCacheBatches(t *testing.T) {
	mgrConf := manager.NewResourceConfig()

	fooCache := cache.NewConfig()
	fooCache.Label = "foo"

	mgrConf.ResourceCaches = append(mgrConf.ResourceCaches, fooCache)

	mgr, err := manager.NewV2(mgrConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	cacheConf := writer.NewCacheConfig()
	cacheConf.Target = "foo"
	cacheConf.Key = "${!json(\"key\")}"

	c, err := writer.NewCache(cacheConf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := map[string]string{}
	for i := 0; i < 10; i++ {
		msg := message.New(nil)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key%v", i*10+j)
			value := fmt.Sprintf(`{"key":"%v","test":"hello world"}`, key)
			exp[key] = value
			msg.Append(message.NewPart([]byte(value)))
		}
		if err := c.Write(msg); err != nil {
			t.Fatal(err)
		}
	}

	memCache, err := mgr.GetCache("foo")
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range exp {
		res, err := memCache.Get(context.Background(), k)
		if err != nil {
			t.Errorf("Missing key '%v': %v", k, err)
		}
		if exp, act := v, string(res); exp != act {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
