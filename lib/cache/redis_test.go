// +build integration

package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
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

	url := fmt.Sprintf("tcp://localhost:%v/1", resource.GetPort("6379/tcp"))

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
	t.Run("TestRedisGetAndSetWithTTL", func(te *testing.T) {
		testRedisGetAndSetWithTTL(url, te)
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

func testRedisGetAndSetWithTTL(url string, t *testing.T) {
	conf := NewConfig()
	conf.Redis.URL = url

	c, err := NewRedis(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if c, ok := c.(types.CacheWithTTL); ok {
		ttl := 300 * time.Millisecond
		if err = c.SetWithTTL("benthos_test_connection", []byte("hello world"), &ttl); err != nil {
			t.Skipf("Redis server not available: %v", err)
		}

		if err = c.Delete("benthos_test_foo"); err != nil {
			t.Error(err)
		}
		if err = c.SetWithTTL("benthos_test_foo", []byte("bar"), &ttl); err != nil {
			t.Error(err)
		}

		exp := "bar"
		var act []byte

		if act, err = c.Get("benthos_test_foo"); err != nil {
			t.Error(err)
		} else if string(act) != exp {
			t.Errorf("Wrong value returned: %v != %v", string(act), exp)
		}

		if err = c.SetWithTTL("benthos_test_bar", []byte("baz"), &ttl); err != nil {
			t.Error(err)
		}
		// wait to expire
		time.Sleep(500 * time.Millisecond)
		_, err = c.Get("benthos_test_bar")
		assert.Equal(t, types.ErrKeyNotFound, err)
	} else {
		assert.Fail(t, "redis should implement CacheWithTTL interface")
	}
}
