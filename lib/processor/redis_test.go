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
	"fmt"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/go-redis/redis"
	"github.com/ory/dockertest"
)

func TestRedisIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urlStr := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))
	uri, err := url.Parse(urlStr)
	if err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr:    uri.Host,
		Network: uri.Scheme,
	})

	if err = pool.Retry(func() error {
		return client.Ping().Err()
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	defer client.Close()

	t.Run("testRedisSAdd", func(t *testing.T) {
		testRedisSAdd(t, client, urlStr)
	})
	t.Run("testRedisSCard", func(t *testing.T) {
		testRedisSCard(t, client, urlStr)
	})
}

func testRedisSAdd(t *testing.T, client *redis.Client, url string) {
	conf := NewConfig()
	conf.Type = TypeRedis
	conf.Redis.URL = url
	conf.Redis.Operator = "sadd"
	conf.Redis.Key = "${!metadata:key}"

	r, err := NewRedis(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
		[]byte(`bar`),
		[]byte(`baz`),
		[]byte(`buz`),
		[]byte(`bev`),
	})
	msg.Get(0).Metadata().Set("key", "foo1")
	msg.Get(1).Metadata().Set("key", "foo1")
	msg.Get(2).Metadata().Set("key", "foo1")
	msg.Get(3).Metadata().Set("key", "foo2")
	msg.Get(4).Metadata().Set("key", "foo2")
	msg.Get(5).Metadata().Set("key", "foo2")

	resMsgs, response := r.ProcessMessage(msg)
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}

	exp := [][]byte{
		[]byte(`1`),
		[]byte(`1`),
		[]byte(`0`),
		[]byte(`1`),
		[]byte(`1`),
		[]byte(`1`),
	}
	if act := message.GetAllBytes(resMsgs[0]); !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}

	res, err := client.SCard("foo1").Result()
	if exp, act := 2, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 1: %v != %v", act, exp)
	}
	res, err = client.SCard("foo2").Result()
	if exp, act := 3, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 2: %v != %v", act, exp)
	}
}

func testRedisSCard(t *testing.T, client *redis.Client, url string) {
	// WARNING: Relies on testRedisSAdd succeeding.
	conf := NewConfig()
	conf.Type = TypeRedis
	conf.Redis.URL = url
	conf.Redis.Operator = "scard"
	conf.Redis.Key = "${!content}"

	r, err := NewRedis(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.New([][]byte{
		[]byte(`doesntexist`),
		[]byte(`foo1`),
		[]byte(`foo2`),
	})

	resMsgs, response := r.ProcessMessage(msg)
	if response != nil {
		if response.Error() != nil {
			t.Fatal(response.Error())
		}
		t.Fatal("Expected nil response")
	}
	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}

	exp := [][]byte{
		[]byte(`0`),
		[]byte(`2`),
		[]byte(`3`),
	}
	if act := message.GetAllBytes(resMsgs[0]); !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}
}
