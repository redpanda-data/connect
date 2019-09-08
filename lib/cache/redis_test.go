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

package cache

import (
	"fmt"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))

	if err = pool.Retry(func() error {
		conf := NewConfig()
		conf.Redis.URL = url

		r, cErr := NewRedis(conf, nil, log.Noop(), metrics.Noop())
		if cErr != nil {
			return cErr
		}
		cErr = r.Set("benthos_test_redis_connect", []byte("foo bar"))
		return cErr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestRedisAddDuplicate", func(te *testing.T) {
		testRedisAddDuplicate(url, te)
	})
	t.Run("TestRedisGetAndSet", func(te *testing.T) {
		testRedisGetAndSet(url, te)
	})
}

func testRedisAddDuplicate(url string, t *testing.T) {
	conf := NewConfig()
	conf.Redis.URL = url

	c, err := NewRedis(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Redis server not available: %v", err)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}

	if err = c.Add("benthos_test_foo", []byte("bar")); err != nil {
		t.Error(err)
	}
	if err = c.Add("benthos_test_foo", []byte("baz")); err != types.ErrKeyAlreadyExists {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyAlreadyExists)
	}

	exp := "bar"
	var act []byte

	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}
}

func testRedisGetAndSet(url string, t *testing.T) {
	conf := NewConfig()
	conf.Redis.URL = url

	c, err := NewRedis(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Redis server not available: %v", err)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}

	if err = c.Set("benthos_test_foo", []byte("bar")); err != nil {
		t.Error(err)
	}

	exp := "bar"
	var act []byte

	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Set("benthos_test_foo", []byte("baz")); err != nil {
		t.Error(err)
	}

	exp = "baz"
	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}
}
