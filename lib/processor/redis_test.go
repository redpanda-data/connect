package processor

import (
	"flag"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/go-redis/redis/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedisIntegration(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

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

	t.Run("testRedisKeys", func(t *testing.T) {
		testRedisKeys(t, client, urlStr)
	})
	t.Run("testRedisSAdd", func(t *testing.T) {
		testRedisSAdd(t, client, urlStr)
	})
	t.Run("testRedisSCard", func(t *testing.T) {
		testRedisSCard(t, client, urlStr)
	})
	t.Run("testRedisIncrby", func(t *testing.T) {
		testRedisIncrby(t, client, urlStr)
	})
}

func testRedisKeys(t *testing.T, client *redis.Client, url string) {
	conf := NewConfig()
	conf.Type = TypeRedis
	conf.Redis.URL = url
	conf.Redis.Operator = "keys"
	conf.Redis.Key = "foo*"

	r, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	for _, key := range []string{
		"bar1", "bar2", "fooa", "foob", "baz1", "fooc",
	} {
		_, err := client.Set(key, "hello world", 0).Result()
		require.NoError(t, err)
	}

	msg := message.QuickBatch([][]byte{[]byte(`ignore me please`)})

	resMsgs, response := r.ProcessMessage(msg)
	require.NoError(t, response)

	require.Len(t, resMsgs, 1)
	require.Equal(t, 1, resMsgs[0].Len())

	exp := []string{"fooa", "foob", "fooc"}

	actI, err := resMsgs[0].Get(0).JSON()
	require.NoError(t, err)

	actS, ok := actI.([]interface{})
	require.True(t, ok)

	actStrs := make([]string, 0, len(actS))
	for _, v := range actS {
		actStrs = append(actStrs, v.(string))
	}
	sort.Strings(actStrs)

	assert.Equal(t, exp, actStrs)
}

func testRedisSAdd(t *testing.T, client *redis.Client, url string) {
	conf := NewConfig()
	conf.Type = TypeRedis
	conf.Redis.URL = url
	conf.Redis.Operator = "sadd"
	conf.Redis.Key = "${! meta(\"key\") }"

	r, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.QuickBatch([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
		[]byte(`bar`),
		[]byte(`baz`),
		[]byte(`buz`),
		[]byte(`bev`),
	})
	msg.Get(0).MetaSet("key", "foo1")
	msg.Get(1).MetaSet("key", "foo1")
	msg.Get(2).MetaSet("key", "foo1")
	msg.Get(3).MetaSet("key", "foo2")
	msg.Get(4).MetaSet("key", "foo2")
	msg.Get(5).MetaSet("key", "foo2")

	resMsgs, response := r.ProcessMessage(msg)
	require.NoError(t, response)

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
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 1: %v != %v", act, exp)
	}
	res, err = client.SCard("foo2").Result()
	if err != nil {
		t.Fatal(err)
	}
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
	conf.Redis.Key = "${!content()}"

	r, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.QuickBatch([][]byte{
		[]byte(`doesntexist`),
		[]byte(`foo1`),
		[]byte(`foo2`),
	})

	resMsgs, response := r.ProcessMessage(msg)
	require.NoError(t, response)

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

func testRedisIncrby(t *testing.T, client *redis.Client, url string) {
	conf := NewConfig()
	conf.Type = TypeRedis
	conf.Redis.URL = url
	conf.Redis.Operator = "incrby"
	conf.Redis.Key = "incrby"

	r, err := New(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	msg := message.QuickBatch([][]byte{
		[]byte(`2`),
		[]byte(`1`),
		[]byte(`5`),
		[]byte(`-10`),
		[]byte(`0`),
	})
	resMsgs, response := r.ProcessMessage(msg)
	require.NoError(t, response)

	exp := [][]byte{
		[]byte(`2`),
		[]byte(`3`),
		[]byte(`8`),
		[]byte(`-2`),
		[]byte(`-2`),
	}
	if act := message.GetAllBytes(resMsgs[0]); !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong result: %s != %s", act, exp)
	}

}
