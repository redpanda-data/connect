package pure_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func testCacheOutput(tb testing.TB, res bundle.NewManagement, confPattern string, args ...any) *pure.CacheWriter {
	tb.Helper()

	pConf, err := pure.CacheOutputSpec().ParseYAML(fmt.Sprintf(confPattern, args...), nil)
	require.NoError(tb, err)

	w, err := pure.NewCacheWriter(pConf, res)
	require.NoError(tb, err)

	return w
}

func TestCacheSingle(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	w := testCacheOutput(t, mgr, `
key: ${!json("id")}
target: foocache
`)

	tCtx := context.Background()

	require.NoError(t, w.WriteBatch(tCtx, message.QuickBatch([][]byte{
		[]byte(`{"id":"1","value":"first"}`),
	})))

	assert.Equal(t, map[string]mock.CacheItem{
		"1": {Value: `{"id":"1","value":"first"}`},
	}, mgr.Caches["foocache"])
}

func TestCacheBatch(t *testing.T) {
	mgr := mock.NewManager()
	mgr.Caches["foocache"] = map[string]mock.CacheItem{}

	w := testCacheOutput(t, mgr, `
key: ${!json("id")}
target: foocache
`)

	tCtx := context.Background()

	require.NoError(t, w.WriteBatch(tCtx, message.QuickBatch([][]byte{
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

	w := testCacheOutput(t, mgr, `
key: ${!json("id")}
target: foocache
ttl: 2s
`)

	tCtx := context.Background()

	require.NoError(t, w.WriteBatch(tCtx, message.QuickBatch([][]byte{
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

	w := testCacheOutput(t, mgr, `
key: ${!json("id")}
target: foocache
ttl: 2s
`)

	tCtx := context.Background()

	require.NoError(t, w.WriteBatch(tCtx, message.QuickBatch([][]byte{
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
	mgrConf, err := testutil.ManagerFromYAML(`
cache_resources:
  - label: foo
    memory: {}
`)
	require.NoError(t, err)

	mgr, err := manager.New(mgrConf)
	require.NoError(t, err)

	c := testCacheOutput(t, mgr, `
key: ${!json("key")}
target: foo
`)

	tCtx := context.Background()

	exp := map[string]string{}
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%v", i)
		value := fmt.Sprintf(`{"key":"%v","test":"hello world"}`, key)
		exp[key] = value
		if err := c.WriteBatch(tCtx, message.QuickBatch([][]byte{[]byte(value)})); err != nil {
			t.Fatal(err)
		}
	}

	var memCache cache.V1
	require.NoError(t, mgr.AccessCache(context.Background(), "foo", func(v cache.V1) {
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
	mgrConf, err := testutil.ManagerFromYAML(`
cache_resources:
  - label: foo
    memory: {}
`)
	require.NoError(t, err)

	mgr, err := manager.New(mgrConf)
	require.NoError(t, err)

	c := testCacheOutput(t, mgr, `
key: ${!json("key")}
target: foo
`)

	tCtx := context.Background()

	exp := map[string]string{}
	for i := 0; i < 10; i++ {
		msg := message.QuickBatch(nil)
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("key%v", i*10+j)
			value := fmt.Sprintf(`{"key":"%v","test":"hello world"}`, key)
			exp[key] = value
			msg = append(msg, message.NewPart([]byte(value)))
		}
		if err := c.WriteBatch(tCtx, msg); err != nil {
			t.Fatal(err)
		}
	}

	var memCache cache.V1
	require.NoError(t, mgr.AccessCache(context.Background(), "foo", func(v cache.V1) {
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
