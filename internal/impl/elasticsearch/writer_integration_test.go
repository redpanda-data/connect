package elasticsearch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/elasticsearch"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
)

func outputFromConf(tb testing.TB, confStr string, args ...any) *elasticsearch.Output {
	tb.Helper()

	pConf, err := elasticsearch.OutputSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(tb, err)

	o, err := elasticsearch.OutputFromParsed(pConf, service.MockResources())
	require.NoError(tb, err)

	return o
}

func TestIntegrationWriter(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Minute * 3

	resource, err := pool.Run("elasticsearch", "7.17.0", []string{
		"discovery.type=single-node",
		"ES_JAVA_OPTS=-Xms512m -Xmx512m", // By default ES immediately gobbles half the available RAM, what a psychopath.
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urls := []string{fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("9200/tcp"))}

	var client *elastic.Client

	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(urls...),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		client, cerr = elastic.NewClient(opts...)

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
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(index).
				Do(context.Background())
			if cerr == nil {
				_, cerr = client.
					CreateIndex("test_conn_index_2").
					Timeout("20s").
					Body(index).
					Do(context.Background())
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

	t.Run("TestElasticNoIndex", func(t *testing.T) {
		testElasticNoIndex(t, urls, client)
	})

	t.Run("TestElasticParallelWrites", func(t *testing.T) {
		testElasticParallelWrites(t, urls, client)
	})

	t.Run("TestElasticErrorHandling", func(t *testing.T) {
		testElasticErrorHandling(t, urls, client)
	})

	t.Run("TestElasticConnect", func(te *testing.T) {
		testElasticConnect(te, urls, client)
	})

	t.Run("TestElasticIndexInterpolation", func(te *testing.T) {
		testElasticIndexInterpolation(te, urls, client)
	})

	t.Run("TestElasticBatch", func(t *testing.T) {
		testElasticBatch(t, urls, client)
	})

	t.Run("TestElasticBatchDelete", func(t *testing.T) {
		testElasticBatchDelete(t, urls, client)
	})

	t.Run("TestElasticBatchIDCollision", func(t *testing.T) {
		testElasticBatchIDCollision(t, urls, client)
	})
}

func testElasticNoIndex(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: does_not_exist
id: 'foo-${!count("noIndexTest")}'
urls: %v
max_retries: 1
backoff:
  max_elapsed_time: 1s
sniff: false
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

		get, err := client.Get().
			Index("does_not_exist").
			Id(id).
			Do(ctx)
		require.NoError(t, err, id)
		assert.True(t, get.Found, id)
	}
}

func testElasticParallelWrites(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: new_index_parallel_writes
id: '${!json("key")}'
urls: %v
max_retries: 1
backoff:
  max_elapsed_time: 1s
sniff: false
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
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("new_index_parallel_writes").
			Type("_doc").
			Id(id).
			Do(ctx)
		require.NoError(t, err, id)
		require.True(t, get.Found, id)

		rawBytes, err := get.Source.MarshalJSON()
		require.NoError(t, err)

		assert.Equal(t, exp, string(rawBytes), id)
	}
}

func testElasticErrorHandling(t *testing.T, urls []string, _ *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index?
id: 'foo-static'
urls: %v
backoff:
  max_elapsed_time: 1s
sniff: false
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

func testElasticConnect(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: test_conn_index
id: 'foo-${!count("foo")}'
urls: %v
type: _doc
sniff: false
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
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(ctx)
		require.NoError(t, err)
		assert.True(t, get.Found)

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, string(testMsgs[i]), string(sourceBytes))
	}
}

func testElasticIndexInterpolation(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-${!count("bar")}'
urls: %v
type: _doc
sniff: false
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
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(ctx)
		require.NoError(t, err)
		assert.True(t, get.Found)

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, string(testMsgs[i]), string(sourceBytes))
	}
}

func testElasticBatch(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'baz-${!count("baz")}'
urls: %v
type: _doc
sniff: false
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
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(ctx)
		require.NoError(t, err)
		assert.True(t, get.Found)

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}
}

func testElasticBatchDelete(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'buz-${!count("elasticBatchDeleteMessages")}'
urls: %v
action: ${! @elastic_action }
type: _doc
sniff: false
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
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(ctx)

		require.NoError(t, err)
		assert.True(t, get.Found)

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	// Set elastic_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		testBatch[i].MetaSetMut("elastic_action", "delete")
	}

	require.NoError(t, m.WriteBatch(ctx, testBatch))

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("buz-%v", i+1)
		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(ctx)
		require.NoError(t, err)

		partAction, _ := testBatch[i].MetaGet("elastic_action")
		if partAction == "deleted" && get.Found {
			t.Errorf("document %v found when it should have been deleted", i)
		} else if partAction != "deleted" && !get.Found {
			t.Errorf("document %v was not found", i)
		}
	}
}

func testElasticBatchIDCollision(t *testing.T, urls []string, client *elastic.Client) {
	t.Helper()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	m := outputFromConf(t, `
index: ${! @index }
id: 'bar-id'
urls: %v
type: _doc
sniff: false
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

		//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index(index).
			Type("_doc").
			Id("bar-id").
			Do(ctx)
		require.NoError(t, err)
		require.True(t, get.Found)

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		require.NoError(t, err)
		assert.Equal(t, string(testMsg[i]), string(sourceBytes))
	}

	// testing sequential updates to a document created above
	m2 := outputFromConf(t, `
index: test_conn_index
id: 'bar-id'
urls: %v
action: update
type: _doc
sniff: false
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

	//nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
	get, err := client.Get().
		Index("test_conn_index").
		Type("_doc").
		Id("bar-id").
		Do(ctx)
	require.NoError(t, err)
	assert.True(t, get.Found)

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}
	require.NoError(t, json.Unmarshal(get.Source, &doc))
	assert.Equal(t, "updated", doc.User)
	assert.Equal(t, "goodbye", doc.Message)
}
