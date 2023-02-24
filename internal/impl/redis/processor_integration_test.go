package redis

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestIntegrationRedisProcessor(t *testing.T) {
	integration.CheckSkip(t)

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

	ctx := context.Background()
	if err = pool.Retry(func() error {
		return client.Ping(ctx).Err()
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	defer client.Close()

	t.Run("testRedisScript", func(t *testing.T) {
		testRedisScript(t, client, urlStr)
	})
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

	require.NoError(t, client.FlushAll(ctx).Err())

	t.Run("testRedisDeprecatedKeys", func(t *testing.T) {
		testRedisDeprecatedKeys(t, client, urlStr)
	})
	t.Run("testRedisDeprecatedSAdd", func(t *testing.T) {
		testRedisDeprecatedSAdd(t, client, urlStr)
	})
	t.Run("testRedisDeprecatedSCard", func(t *testing.T) {
		testRedisDeprecatedSCard(t, client, urlStr)
	})
	t.Run("testRedisDeprecatedIncrby", func(t *testing.T) {
		testRedisDeprecatedIncrby(t, client, urlStr)
	})
}

func testRedisScript(t *testing.T, client *redis.Client, url string) {
	conf, err := redisScriptProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
script: "return KEYS[1] .. ': ' .. ARGV[1]"
args_mapping: 'root = [ "value" ]'
keys_mapping: 'root = [ "key" ]'
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisScriptProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`ignore`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	require.Len(t, resMsgs, 1)
	require.Len(t, resMsgs[0], 1)
	require.NoError(t, resMsgs[0][0].GetError())

	actI, err := resMsgs[0][0].AsStructured()
	require.NoError(t, err)

	assert.Equal(t, "key: value", actI)
}

func testRedisKeys(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
command: keys
args_mapping: 'root = [ "foo*" ]'
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	for _, key := range []string{
		"bar1", "bar2", "fooa", "foob", "baz1", "fooc",
	} {
		_, err := client.Set(ctx, key, "hello world", 0).Result()
		require.NoError(t, err)
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`ignore me please`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	require.Len(t, resMsgs, 1)
	require.Len(t, resMsgs[0], 1)
	require.NoError(t, resMsgs[0][0].GetError())

	exp := []string{"fooa", "foob", "fooc"}

	actI, err := resMsgs[0][0].AsStructured()
	require.NoError(t, err)

	actS, ok := actI.([]any)
	require.True(t, ok)

	actStrs := make([]string, 0, len(actS))
	for _, v := range actS {
		actStrs = append(actStrs, v.(string))
	}
	sort.Strings(actStrs)

	assert.Equal(t, exp, actStrs)
}

func testRedisSAdd(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
command: sadd
args_mapping: 'root = [ meta("key"), content().string() ]'
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`foo`)),
		service.NewMessage([]byte(`bar`)),
		service.NewMessage([]byte(`bar`)),
		service.NewMessage([]byte(`baz`)),
		service.NewMessage([]byte(`buz`)),
		service.NewMessage([]byte(`bev`)),
	}

	msg[0].MetaSet("key", "foo1")
	msg[1].MetaSet("key", "foo1")
	msg[2].MetaSet("key", "foo1")
	msg[3].MetaSet("key", "foo2")
	msg[4].MetaSet("key", "foo2")
	msg[5].MetaSet("key", "foo2")

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	exp := []string{
		`1`,
		`1`,
		`0`,
		`1`,
		`1`,
		`1`,
	}

	require.Len(t, resMsgs, 1)
	require.Len(t, resMsgs[0], len(exp))

	for i, e := range exp {
		require.NoError(t, resMsgs[0][i].GetError())
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}

	ctx := context.Background()
	res, err := client.SCard(ctx, "foo1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 1: %v != %v", act, exp)
	}
	res, err = client.SCard(ctx, "foo2").Result()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 3, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 2: %v != %v", act, exp)
	}
}

func testRedisSCard(t *testing.T, client *redis.Client, url string) {
	// WARNING: Relies on testRedisSAdd succeeding.
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
command: scard
args_mapping: 'root = [ content().string() ]'
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`doesntexist`)),
		service.NewMessage([]byte(`foo1`)),
		service.NewMessage([]byte(`foo2`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	exp := []string{
		`0`,
		`2`,
		`3`,
	}

	require.Len(t, resMsgs, 1)
	require.Len(t, resMsgs[0], len(exp))

	for i, e := range exp {
		require.NoError(t, resMsgs[0][i].GetError())
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}
}

func testRedisIncrby(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
command: incrby
args_mapping: 'root = [ "incrby", this.number() ]'
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`2`)),
		service.NewMessage([]byte(`1`)),
		service.NewMessage([]byte(`5`)),
		service.NewMessage([]byte(`-10`)),
		service.NewMessage([]byte(`0`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	exp := []string{
		`2`,
		`3`,
		`8`,
		`-2`,
		`-2`,
	}

	require.Len(t, resMsgs, 1)
	require.Len(t, resMsgs[0], len(exp))

	for i, e := range exp {
		require.NoError(t, resMsgs[0][i].GetError())
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}
}

func testRedisDeprecatedKeys(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
operator: keys
key: foo*
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	for _, key := range []string{
		"bar1", "bar2", "fooa", "foob", "baz1", "fooc",
	} {
		_, err := client.Set(ctx, key, "hello world", 0).Result()
		require.NoError(t, err)
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`ignore me please`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	require.Len(t, resMsgs, 1)
	require.Equal(t, 1, len(resMsgs[0]))

	exp := []string{"fooa", "foob", "fooc"}

	actI, err := resMsgs[0][0].AsStructured()
	require.NoError(t, err)

	actS, ok := actI.([]any)
	require.True(t, ok)

	actStrs := make([]string, 0, len(actS))
	for _, v := range actS {
		actStrs = append(actStrs, v.(string))
	}
	sort.Strings(actStrs)

	assert.Equal(t, exp, actStrs)
}

func testRedisDeprecatedSAdd(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
operator: sadd
key: "${! meta(\"key\") }"
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`foo`)),
		service.NewMessage([]byte(`bar`)),
		service.NewMessage([]byte(`bar`)),
		service.NewMessage([]byte(`baz`)),
		service.NewMessage([]byte(`buz`)),
		service.NewMessage([]byte(`bev`)),
	}

	msg[0].MetaSet("key", "foo1")
	msg[1].MetaSet("key", "foo1")
	msg[2].MetaSet("key", "foo1")
	msg[3].MetaSet("key", "foo2")
	msg[4].MetaSet("key", "foo2")
	msg[5].MetaSet("key", "foo2")

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}

	exp := []string{
		`1`,
		`1`,
		`0`,
		`1`,
		`1`,
		`1`,
	}
	for i, e := range exp {
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}

	ctx := context.Background()

	res, err := client.SCard(ctx, "foo1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 1: %v != %v", act, exp)
	}
	res, err = client.SCard(ctx, "foo2").Result()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 3, int(res); exp != act {
		t.Errorf("Wrong cardinality of set 2: %v != %v", act, exp)
	}
}

func testRedisDeprecatedSCard(t *testing.T, client *redis.Client, url string) {
	// WARNING: Relies on testRedisSAdd succeeding.
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
operator: scard
key: "${! content() }"
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`doesntexist`)),
		service.NewMessage([]byte(`foo1`)),
		service.NewMessage([]byte(`foo2`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	if len(resMsgs) != 1 {
		t.Fatalf("Wrong resulting msgs: %v != %v", len(resMsgs), 1)
	}

	exp := []string{
		`0`,
		`2`,
		`3`,
	}
	for i, e := range exp {
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}
}

func testRedisDeprecatedIncrby(t *testing.T, client *redis.Client, url string) {
	conf, err := redisProcConfig().ParseYAML(fmt.Sprintf(`
url: %v
operator: incrby
key: incrby
`, url), nil)
	require.NoError(t, err)

	r, err := newRedisProcFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`2`)),
		service.NewMessage([]byte(`1`)),
		service.NewMessage([]byte(`5`)),
		service.NewMessage([]byte(`-10`)),
		service.NewMessage([]byte(`0`)),
	}

	resMsgs, response := r.ProcessBatch(context.Background(), msg)
	require.NoError(t, response)

	exp := []string{
		`2`,
		`3`,
		`8`,
		`-2`,
		`-2`,
	}
	for i, e := range exp {
		act, err := resMsgs[0][i].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, e, string(act))
	}
}
