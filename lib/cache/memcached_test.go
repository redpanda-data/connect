// +build integration

package cache

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest/v3"
)

func TestMemcachedIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("memcached", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	addrs := []string{fmt.Sprintf("localhost:%v", resource.GetPort("11211/tcp"))}

	if err = pool.Retry(func() error {
		conf := NewConfig()
		conf.Memcached.Addresses = addrs

		testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
		_, cErr := NewMemcached(conf, nil, testLog, metrics.DudType{})
		return cErr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestMemcachedAddDuplicate", func(te *testing.T) {
		testMemcachedAddDuplicate(addrs, te)
	})
	t.Run("TestMemcachedGetAndSet", func(te *testing.T) {
		testMemcachedGetAndSet(addrs, te)
	})
	t.Run("TestMemcachedGetAndSetWithTTL", func(te *testing.T) {
		testMemcachedGetAndSetWithTTL(addrs, te)
	})
}

func testMemcachedAddDuplicate(addrs []string, t *testing.T) {
	conf := NewConfig()
	conf.Memcached.Addresses = addrs

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	c, err := NewMemcached(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Memcached server not available: %v", err)
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

func testMemcachedGetAndSet(addrs []string, t *testing.T) {
	conf := NewConfig()
	conf.Memcached.Addresses = addrs

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	c, err := NewMemcached(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Memcached server not available: %v", err)
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

func testMemcachedGetAndSetWithTTL(addrs []string, t *testing.T) {
	conf := NewConfig()
	conf.Memcached.Addresses = addrs

	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	c, err := NewMemcached(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if c, ok := c.(types.CacheWithTTL); ok {
		ttl := time.Second
		if err = c.SetWithTTL("benthos_test_connection", []byte("hello world"), &ttl); err != nil {
			t.Skipf("Memcached server not available: %v", err)
		}
		require.NoError(t, c.SetWithTTL("foo", []byte("1"), &ttl))
		// wait to expire
		time.Sleep(2 * time.Second)
		_, err = c.Get("foo")
		assert.Equal(t, types.ErrKeyNotFound, err)
	} else {
		assert.Fail(t, "memcached should implement CacheWithTTL interface")
	}
}
