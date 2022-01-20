package processor

import (
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestCacheSet(t *testing.T) {
	memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Cache.Key = "${!json(\"key\")}"
	conf.Cache.Value = "${!json(\"value\")}"
	conf.Cache.Resource = "foocache"
	proc, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	actBytes, err := memCache.Get("1")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 3", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	actBytes, err = memCache.Get("2")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 2", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestCacheSetParts(t *testing.T) {
	memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Cache.Key = "${!json(\"key\")}"
	conf.Cache.Value = "${!json(\"value\")}"
	conf.Cache.Resource = "foocache"
	conf.Cache.Parts = []int{0, 1}
	proc, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	actBytes, err := memCache.Get("1")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 1", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	actBytes, err = memCache.Get("2")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 2", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestCacheAdd(t *testing.T) {
	memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	conf := NewConfig()
	conf.Cache.Key = "${!json(\"key\")}"
	conf.Cache.Value = "${!json(\"value\")}"
	conf.Cache.Resource = "foocache"
	conf.Cache.Operator = "add"
	proc, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1","value":"foo 1"}`),
		[]byte(`{"key":"2","value":"foo 2"}`),
		[]byte(`{"key":"1","value":"foo 3"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := false, HasFailed(output[0].Get(0)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := false, HasFailed(output[0].Get(1)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := true, HasFailed(output[0].Get(2)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}

	actBytes, err := memCache.Get("1")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 1", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	actBytes, err = memCache.Get("2")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 2", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestCacheGet(t *testing.T) {
	memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	memCache.Set("1", []byte("foo 1"))
	memCache.Set("2", []byte("foo 2"))

	conf := NewConfig()
	conf.Cache.Key = "${!json(\"key\")}"
	conf.Cache.Resource = "foocache"
	conf.Cache.Operator = "get"
	proc, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1"}`),
		[]byte(`{"key":"2"}`),
		[]byte(`{"key":"3"}`),
	})
	expParts := [][]byte{
		[]byte(`foo 1`),
		[]byte(`foo 2`),
		[]byte(`{"key":"3"}`),
	}

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := expParts, message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := false, HasFailed(output[0].Get(0)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := false, HasFailed(output[0].Get(1)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := true, HasFailed(output[0].Get(2)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
}

func TestCacheDelete(t *testing.T) {
	memCache, err := cache.NewMemory(cache.NewConfig(), nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	mgr := &fakeMgr{
		caches: map[string]types.Cache{
			"foocache": memCache,
		},
	}

	memCache.Set("1", []byte("foo 1"))
	memCache.Set("2", []byte("foo 2"))
	memCache.Set("3", []byte("foo 3"))

	conf := NewConfig()
	conf.Cache.Key = "${!json(\"key\")}"
	conf.Cache.Resource = "foocache"
	conf.Cache.Operator = "delete"
	proc, err := NewCache(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	input := message.New([][]byte{
		[]byte(`{"key":"1"}`),
		[]byte(`{"key":"3"}`),
		[]byte(`{"key":"4"}`),
	})

	output, res := proc.ProcessMessage(input)
	if res != nil {
		t.Fatal(res.Error())
	}

	if len(output) != 1 {
		t.Fatalf("Wrong count of result messages: %v", len(output))
	}

	if exp, act := message.GetAllBytes(input), message.GetAllBytes(output[0]); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result messages: %s != %s", act, exp)
	}

	if exp, act := false, HasFailed(output[0].Get(0)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := false, HasFailed(output[0].Get(1)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}
	if exp, act := false, HasFailed(output[0].Get(2)); exp != act {
		t.Errorf("Wrong fail flag: %v != %v", act, exp)
	}

	_, err = memCache.Get("1")
	if err != types.ErrKeyNotFound {
		t.Errorf("Wrong result: %v != %v", err, types.ErrKeyNotFound)
	}

	actBytes, err := memCache.Get("2")
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := "foo 2", string(actBytes); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	_, err = memCache.Get("3")
	if err != types.ErrKeyNotFound {
		t.Errorf("Wrong result: %v != %v", err, types.ErrKeyNotFound)
	}
}
