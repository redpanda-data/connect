// +build integration

package integration

import (
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
	"github.com/ory/dockertest"
)

func TestRedisHashIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))

	if err = pool.Retry(func() error {
		conf := writer.NewRedisHashConfig()
		conf.URL = url
		conf.Key = "foo"
		conf.WalkMetadata = true

		r, cErr := writer.NewRedisHash(conf, log.Noop(), metrics.Noop())
		if cErr != nil {
			return cErr
		}
		cErr = r.Connect()

		r.CloseAsync()
		return cErr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestRedisHashSinglePart", func(te *testing.T) {
		testRedisHashSinglePart(url, te)
	})
	t.Run("TestRedisHashOverrides", func(te *testing.T) {
		testRedisHashOverrides(url, te)
	})
	t.Run("TestRedisHashMultiplePart", func(te *testing.T) {
		testRedisHashMultiplePart(url, te)
	})
	t.Run("TestRedisHashParallelWrites", func(te *testing.T) {
		testRedisHashParallelWrites(url, te)
	})
}

func testRedisHashSinglePart(surl string, t *testing.T) {
	outConf := writer.NewRedisHashConfig()
	outConf.URL = surl
	outConf.Key = "${!metadata:key}"
	outConf.Fields["example_key"] = "${!metadata:example_key}"

	mOutput, err := writer.NewRedisHash(outConf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = mOutput.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	purl, err := url.Parse(surl)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:    purl.Host,
		Network: purl.Scheme,
	})

	if _, err = client.Ping().Result(); err != nil {
		t.Fatal(err)
	}

	N := 10
	testIds := map[string]struct{}{}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("id%v", i)
		testIds[id] = struct{}{}
		msg := message.New([][]byte{
			[]byte("not this content"),
		})
		msg.Get(0).Metadata().Set("key", id)
		msg.Get(0).Metadata().Set("example_key", "test-"+id)
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for k := range testIds {
		res, err := client.HGet(k, "example_key").Result()
		if err != nil {
			t.Error(err)
			continue
		}
		if exp := "test-" + k; exp != res {
			t.Errorf("Wrong result: %v != %v", res, exp)
		}
	}
}

func testRedisHashOverrides(surl string, t *testing.T) {
	outConf := writer.NewRedisHashConfig()
	outConf.URL = surl
	outConf.Key = "${!metadata:key}"
	outConf.WalkMetadata = true
	outConf.WalkJSONObject = true
	outConf.Fields["baz"] = "true baz"

	mOutput, err := writer.NewRedisHash(outConf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = mOutput.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	purl, err := url.Parse(surl)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:    purl.Host,
		Network: purl.Scheme,
	})

	if _, err = client.Ping().Result(); err != nil {
		t.Fatal(err)
	}

	N := 10
	testIds := map[string]struct{}{}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("idoverrides%v", i)
		testIds[id] = struct{}{}
		msg := message.New([][]byte{
			[]byte(fmt.Sprintf(`{"bar":"%v","baz":"%v"}`, id+"- json obj bar", id+"- json obj baz")),
		})
		msg.Get(0).Metadata().Set("key", id)
		msg.Get(0).Metadata().Set("foo", id+"- metadata foo")
		msg.Get(0).Metadata().Set("bar", id+"- metadata bar")
		msg.Get(0).Metadata().Set("baz", id+"- metadata baz")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for k := range testIds {
		res, err := client.HGetAll(k).Result()
		if err != nil {
			t.Error(err)
			continue
		}
		if exp := map[string]string{
			"key": k,
			"foo": k + "- metadata foo",
			"bar": k + "- json obj bar",
			"baz": "true baz",
		}; !reflect.DeepEqual(exp, res) {
			t.Errorf("Wrong result: %v != %v", res, exp)
		}
	}
}

func testRedisHashMultiplePart(surl string, t *testing.T) {
	outConf := writer.NewRedisHashConfig()
	outConf.URL = surl
	outConf.Key = "${!metadata:key}"
	outConf.Fields["example_key"] = "${!metadata:example_key}"

	mOutput, err := writer.NewRedisHash(outConf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = mOutput.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	purl, err := url.Parse(surl)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:    purl.Host,
		Network: purl.Scheme,
	})

	if _, err = client.Ping().Result(); err != nil {
		t.Fatal(err)
	}

	N := 10
	M := 5
	testIds := map[string]struct{}{}
	for i := 0; i < N; i++ {
		msg := message.New(nil)
		for j := 0; j < M; j++ {
			id := fmt.Sprintf("idmulti%v", i*M+j)
			testIds[id] = struct{}{}
			part := message.NewPart([]byte("not this content"))
			part.Metadata().Set("key", id)
			part.Metadata().Set("example_key", "test-"+id)
			msg.Append(part)
		}
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for k := range testIds {
		res, err := client.HGet(k, "example_key").Result()
		if err != nil {
			t.Error(err)
			continue
		}
		if exp := "test-" + k; exp != res {
			t.Errorf("Wrong result: %v != %v", res, exp)
		}
	}
}

func testRedisHashParallelWrites(surl string, t *testing.T) {
	outConf := writer.NewRedisHashConfig()
	outConf.URL = surl
	outConf.Key = "${!metadata:key}"
	outConf.Fields["example_key"] = "${!metadata:example_key}"

	mOutput, err := writer.NewRedisHash(outConf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = mOutput.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	purl, err := url.Parse(surl)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:    purl.Host,
		Network: purl.Scheme,
	})

	if _, err = client.Ping().Result(); err != nil {
		t.Fatal(err)
	}

	N := 10
	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N)

	testIds := map[string]struct{}{}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("id%v", i)
		testIds[id] = struct{}{}
		msg := message.New([][]byte{
			[]byte("not this content"),
		})
		msg.Get(0).Metadata().Set("key", id)
		msg.Get(0).Metadata().Set("example_key", "test-"+id)
		go func(m types.Message) {
			<-startChan
			if gerr := mOutput.Write(m); gerr != nil {
				t.Fatal(gerr)
			}
			wg.Done()
		}(msg)
	}

	close(startChan)

	for k := range testIds {
		res, err := client.HGet(k, "example_key").Result()
		if err != nil {
			t.Error(err)
			continue
		}
		if exp := "test-" + k; exp != res {
			t.Errorf("Wrong result: %v != %v", res, exp)
		}
	}

	wg.Wait()
}
