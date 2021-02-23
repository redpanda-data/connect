package tests

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

func TestCacheBasic(t *testing.T) {
	mgrConf := manager.NewConfig()
	mgrConf.Caches["foo"] = cache.NewConfig()

	mgr, err := manager.New(mgrConf, nil, log.Noop(), metrics.Noop())
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
		res, err := memCache.Get(k)
		if err != nil {
			t.Errorf("Missing key '%v': %v", k, err)
		}
		if exp, act := v, string(res); exp != act {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}

func TestCacheBatches(t *testing.T) {
	mgrConf := manager.NewConfig()
	mgrConf.Caches["foo"] = cache.NewConfig()

	mgr, err := manager.New(mgrConf, nil, log.Noop(), metrics.Noop())
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
		res, err := memCache.Get(k)
		if err != nil {
			t.Errorf("Missing key '%v': %v", k, err)
		}
		if exp, act := v, string(res); exp != act {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}
}
