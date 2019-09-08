// Copyright (c) 2018 Ashley Jeffs
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

package writer

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestCacheBasic(t *testing.T) {
	mgrConf := manager.NewConfig()
	mgrConf.Caches["foo"] = cache.NewConfig()

	mgr, err := manager.New(mgrConf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	cacheConf := NewCacheConfig()
	cacheConf.Target = "foo"
	cacheConf.Key = "${!json_field:key}"

	c, err := NewCache(cacheConf, mgr, log.Noop(), metrics.Noop())
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

	cacheConf := NewCacheConfig()
	cacheConf.Target = "foo"
	cacheConf.Key = "${!json_field:key}"

	c, err := NewCache(cacheConf, mgr, log.Noop(), metrics.Noop())
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
