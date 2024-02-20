package opensearch_test

import (
	"context"
	"encoding/json"
	"io"

	"github.com/benthosdev/benthos/v4/internal/impl/opensearch"
	"github.com/benthosdev/benthos/v4/public/service"

	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	os "github.com/opensearch-project/opensearch-go/v2"
	osapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func outputFromConf(t testing.TB, confStr string, args ...any) *opensearch.Output {
	t.Helper()

	pConf, err := opensearch.OutputSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(t, err)

	o, err := opensearch.OutputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)

	return o
}

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 60

	resource, err := pool.Run("opensearchproject/opensearch", "latest", []string{
		"discovery.type=single-node",
		"DISABLE_SECURITY_PLUGIN=true",
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urls := []string{fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("9200/tcp"))}

	var client *os.Client

	if err = pool.Retry(func() error {
		opts := os.Config{Addresses: urls,
			Transport: http.DefaultTransport,
		}

		var cerr error
		client, cerr = os.NewClient(opts)

		if cerr == nil {
			index := `{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"properties": {
			"user":{
				"type":"keyword"
			},
			"message":{
				"type":"text",
				"store": true,
				"fielddata": true
			}
		}
	}
}`
			_, cerr = osapi.IndicesCreateRequest{
				Index:   "test_conn_index",
				Body:    strings.NewReader(index),
				Timeout: time.Second * 20,
			}.Do(context.Background(), client)
			if cerr == nil {
				_, cerr = osapi.IndicesCreateRequest{
					Index:   "test_conn_index_2",
					Body:    strings.NewReader(index),
					Timeout: time.Second * 20,
				}.Do(context.Background(), client)
			}

		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestOpenSearchNoIndex", func(te *testing.T) {
		testOpenSearchNoIndex(urls, client, te)
	})

	t.Run("TestOpenSearchParallelWrites", func(te *testing.T) {
		testOpenSearchParallelWrites(urls, client, te)
	})

	t.Run("TestOpenSearchErrorHandling", func(te *testing.T) {
		testOpenSearchErrorHandling(urls, client, te)
	})

	t.Run("TestOpenSearchConnect", func(te *testing.T) {
		testOpenSearchConnect(urls, client, te)
	})

	t.Run("TestOpenSearchIndexInterpolation", func(te *testing.T) {
		testOpenSearchIndexInterpolation(urls, client, te)
	})

	t.Run("TestOpenSearchBatch", func(te *testing.T) {
		testOpenSearchBatch(urls, client, te)
	})

	t.Run("TestOpenSearchBatchDelete", func(te *testing.T) {
		testOpenSearchBatchDelete(urls, client, te)
	})

	t.Run("TestOpenSearchBatchIDCollision", func(te *testing.T) {
		testOpenSearchBatchIDCollision(urls, client, te)
	})

}

func testOpenSearchNoIndex(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: does_not_exist
id: 'foo-${!count("noIndexTest")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"1"}`)),
	}))

	require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"hello world","user":"2"}`)),
		service.NewMessage([]byte(`{"message":"hello world","user":"3"}`)),
	}))

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "does_not_exist",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())
	}
}

func testOpenSearchParallelWrites(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: new_index_parallel_writes
id: '${!json("key")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N)

	docs := map[string]string{}

	for i := 0; i < N; i++ {
		str := fmt.Sprintf(`{"key":"doc-%v","message":"foobar"}`, i)
		docs[fmt.Sprintf("doc-%v", i)] = str
		go func(content string) {
			<-startChan
			assert.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
				service.NewMessage([]byte(content)),
			}))
			wg.Done()
		}(str)
	}

	close(startChan)
	wg.Wait()

	for id, exp := range docs {
		get, err := osapi.GetRequest{
			Index:      "new_index_parallel_writes",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, exp, string(rawBytes), id)
	}
}

func testOpenSearchErrorHandling(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index?
id: 'foo-static'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	require.Error(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":true}`)),
	}))

	require.Error(t, m.WriteBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"message":"foo"}`)),
		service.NewMessage([]byte(`{"message":"bar"}`)),
	}))
}

func testOpenSearchConnect(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index
id: 'foo-${!count("foo")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsgs [][]byte
	for i := 0; i < N; i++ {
		testData := []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i))
		testMsgs = append(testMsgs, testData)
	}
	for i := 0; i < N; i++ {
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage(testMsgs[i]),
		}))
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, string(testMsgs[i]), string(rawBytes))
	}
}

func testOpenSearchIndexInterpolation(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-${!count("bar")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	testMsgs := [][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
	}
	for i := 0; i < N; i++ {
		msg := service.NewMessage(testMsgs[i])
		msg.MetaSetMut("index", "test_conn_index")
		require.NoError(t, m.WriteBatch(ctx, service.MessageBatch{msg}))
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, string(testMsgs[i]), string(rawBytes))
	}
}

func testOpenSearchBatch(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'baz-${!count("baz")}'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsg [][]byte
	var testBatch service.MessageBatch
	for i := 0; i < N; i++ {
		testMsg = append(testMsg, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("index", "test_conn_index")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("baz-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(rawBytes))
	}
}

func testOpenSearchBatchDelete(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'buz-${!count("elasticBatchDeleteMessages")}'
urls: %v
action: ${! @elastic_action }
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	N := 10

	var testMsg [][]byte
	var testBatch service.MessageBatch
	for i := 0; i < N; i++ {
		testMsg = append(testMsg, []byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)))
		testBatch = append(testBatch, service.NewMessage(testMsg[i]))
		testBatch[i].MetaSetMut("index", "test_conn_index")
		testBatch[i].MetaSetMut("elastic_action", "index")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(rawBytes))
	}

	// Set elastic_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		testBatch[i].MetaSetMut("elastic_action", "delete")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(ctx, client)
		require.NoError(t, err, id)

		partAction, _ := testBatch[i].MetaGet("elastic_action")
		if partAction == "deleted" {
			assert.True(t, get.IsError())
		} else {
			assert.False(t, get.IsError())
			rawBytes, err := io.ReadAll(get.Body)
			require.NoError(t, err)

			assert.Equal(t, string(testMsg[i]), string(rawBytes))
		}
	}
}

func testOpenSearchBatchIDCollision(urls []string, client *os.Client, t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-id'
urls: %v
action: index
`, urls)

	require.NoError(t, m.Connect(ctx))
	defer func() {
		require.NoError(t, m.Close(ctx))
	}()

	testMsg := [][]byte{
		[]byte(`{"message":"hello world","user":"0"}`),
		[]byte(`{"message":"hello world","user":"1"}`),
	}
	testBatch := service.MessageBatch{
		service.NewMessage(testMsg[0]),
		service.NewMessage(testMsg[1]),
	}

	testBatch[0].MetaSetMut("index", "test_conn_index")
	testBatch[1].MetaSetMut("index", "test_conn_index_2")

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < 2; i++ {
		index, _ := testBatch[i].MetaGet("index")

		get, err := osapi.GetRequest{
			Index:      index,
			DocumentID: "bar-id",
		}.Do(ctx, client)
		require.NoError(t, err)
		assert.False(t, get.IsError())

		rawBytes, err := io.ReadAll(get.Body)
		require.NoError(t, err)

		assert.Equal(t, string(testMsg[i]), string(rawBytes))
	}

	// testing sequential updates to a document created above
	m2 := outputFromConf(t, `
index: test_conn_index
id: 'bar-id'
urls: %v
action: update
`, urls)

	require.NoError(t, m2.Connect(ctx))
	defer func() {
		require.NoError(t, m2.Close(ctx))
	}()

	testBatch = service.MessageBatch{
		service.NewMessage([]byte(`{"message":"goodbye"}`)),
		service.NewMessage([]byte(`{"user": "updated"}`)),
	}
	require.NoError(t, m2.WriteBatch(ctx, testBatch))

	get, err := osapi.GetRequest{
		Index:      "test_conn_index",
		DocumentID: "bar-id",
	}.Do(ctx, client)
	require.NoError(t, err)
	assert.False(t, get.IsError())

	rawBytes, err := io.ReadAll(get.Body)
	require.NoError(t, err)

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}
	require.NoError(t, json.Unmarshal(rawBytes, &doc))
	assert.Equal(t, "updated", doc.User)
	assert.Equal(t, "goodbye", doc.Message)
}
