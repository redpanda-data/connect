package cache

import (
	"fmt"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest"
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
