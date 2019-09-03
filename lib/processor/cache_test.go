// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	conf.Cache.Key = "${!json_field:key}"
	conf.Cache.Value = "${!json_field:value}"
	conf.Cache.Cache = "foocache"
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
	conf.Cache.Key = "${!json_field:key}"
	conf.Cache.Value = "${!json_field:value}"
	conf.Cache.Cache = "foocache"
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
	conf.Cache.Key = "${!json_field:key}"
	conf.Cache.Value = "${!json_field:value}"
	conf.Cache.Cache = "foocache"
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
	conf.Cache.Key = "${!json_field:key}"
	conf.Cache.Cache = "foocache"
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
