package writer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	icache "github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestCacheSingle(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.QuickBatch([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	assert.Equal(t, map[string]mock.CacheItem{
		"1": {Value: `{"id":"1","value":"first"}`},
	}, mgr.Caches["foocache"])
}

func TestCacheBatch(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.QuickBatch([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
		[]byte(`{"id":"2","value":"second"}`),
		[]byte(`{"id":"3","value":"third"}`),
		[]byte(`{"id":"4","value":"fourth"}`),
	})))

	assert.Equal(t, map[string]mock.CacheItem{
		"1": {Value: `{"id":"1","value":"first"}`},
		"2": {Value: `{"id":"2","value":"second"}`},
		"3": {Value: `{"id":"3","value":"third"}`},
		"4": {Value: `{"id":"4","value":"fourth"}`},
	}, mgr.Caches["foocache"])
}

func TestCacheSingleTTL(t *testing.T) {
	c := map[string]mock.CacheItem{}

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = c

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.QuickBatch([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	two := time.Second * 2
	assert.Equal(t, map[string]mock.CacheItem{
		"1": {Value: `{"id":"1","value":"first"}`, TTL: &two},
	}, c)
}

func TestCacheBatchTTL(t *testing.T) {
	c := map[string]mock.CacheItem{}

	mgr := mock.NewManager()
	mgr.Caches["foocache"] = c

	conf := writer.NewCacheConfig()
	conf.Key = `${!json("id")}`
	conf.Target = "foocache"
	conf.TTL = "2s"

	w, err := writer.NewCache(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	require.NoError(t, w.Write(message.QuickBatch([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
		[]byte(`{"id":"2","value":"second"}`),
		[]byte(`{"id":"3","value":"third"}`),
		[]byte(`{"id":"4","value":"fourth"}`),
	})))

	twosec := time.Second * 2

	assert.Equal(t, map[string]mock.CacheItem{
		"1": {
			Value: `{"id":"1","value":"first"}`,
			TTL:   &twosec,
		},
		"2": {
			Value: `{"id":"2","value":"second"}`,
			TTL:   &twosec,
		},
		"3": {
			Value: `{"id":"3","value":"third"}`,
			TTL:   &twosec,
		},
		"4": {
			Value: `{"id":"4","value":"fourth"}`,
			TTL:   &twosec,
		},
	}, c)
}

//------------------------------------------------------------------------------

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
		if err := c.Write(message.QuickBatch([][]byte{[]byte(value)})); err != nil {
			t.Fatal(err)
		}
	}

	var memCache icache.V1
	require.NoError(t, mgr.AccessCache(context.Background(), "foo", func(v icache.V1) {
		memCache = v
	}))

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
		msg := message.QuickBatch(nil)
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

	var memCache icache.V1
	require.NoError(t, mgr.AccessCache(context.Background(), "foo", func(v icache.V1) {
		memCache = v
	}))

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
