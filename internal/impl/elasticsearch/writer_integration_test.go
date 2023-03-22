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
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/elasticsearch"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

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

	t.Run("TestElasticNoIndex", func(te *testing.T) {
		testElasticNoIndex(urls, client, te)
	})

	t.Run("TestElasticParallelWrites", func(te *testing.T) {
		testElasticParallelWrites(urls, client, te)
	})

	t.Run("TestElasticErrorHandling", func(te *testing.T) {
		testElasticErrorHandling(urls, client, te)
	})

	t.Run("TestElasticConnect", func(te *testing.T) {
		testElasticConnect(urls, client, te)
	})

	t.Run("TestElasticIndexInterpolation", func(te *testing.T) {
		testElasticIndexInterpolation(urls, client, te)
	})

	t.Run("TestElasticBatch", func(te *testing.T) {
		testElasticBatch(urls, client, te)
	})

	t.Run("TestElasticBatchDelete", func(te *testing.T) {
		testElasticBatchDelete(urls, client, te)
	})

	t.Run("TestElasticBatchIDCollision", func(te *testing.T) {
		testElasticBatchIDCollision(urls, client, te)
	})
}

func testElasticNoIndex(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "does_not_exist"
	conf.ID = "foo-${!count(\"noIndexTest\")}"
	conf.URLs = urls
	conf.MaxRetries = 1
	conf.Backoff.MaxElapsedTime = "1s"
	conf.Sniff = false

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Error(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	if err = m.Write(message.QuickBatch([][]byte{[]byte(`{"message":"hello world","user":"1"}`)})); err != nil {
		t.Error(err)
	}

	if err = m.Write(message.QuickBatch([][]byte{
		[]byte(`{"message":"hello world","user":"2"}`),
		[]byte(`{"message":"hello world","user":"3"}`),
	})); err != nil {
		t.Error(err)
	}

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("foo-%v", i+1)

		get, err := client.Get().
			Index("does_not_exist").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}
	}
}

func testElasticParallelWrites(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "new_index_parallel_writes"
	conf.ID = "${!json(\"key\")}"
	conf.URLs = urls
	conf.MaxRetries = 1
	conf.Backoff.MaxElapsedTime = "1s"
	conf.Sniff = false

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Error(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
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
			if lerr := m.Write(message.QuickBatch([][]byte{[]byte(content)})); lerr != nil {
				t.Error(lerr)
			}
			wg.Done()
		}(str)
	}

	close(startChan)
	wg.Wait()

	for id, exp := range docs {
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("new_index_parallel_writes").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", id)
		} else {
			rawBytes, err := get.Source.MarshalJSON()
			if err != nil {
				t.Error(err)
			} else if act := string(rawBytes); act != exp {
				t.Errorf("Wrong result: %v != %v", act, exp)
			}
		}
	}
}

func testElasticErrorHandling(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "test_conn_index?"
	conf.ID = "foo-static"
	conf.URLs = urls
	conf.Backoff.MaxInterval = "1s"
	conf.Sniff = false

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	if err = m.Write(message.QuickBatch([][]byte{[]byte(`{"message":true}`)})); err == nil {
		t.Error("Expected error")
	}

	if err = m.Write(message.QuickBatch([][]byte{[]byte(`{"message":"foo"}`), []byte(`{"message":"bar"}`)})); err == nil {
		t.Error("Expected error")
	}
}

func testElasticConnect(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "test_conn_index"
	conf.ID = "foo-${!count(\"foo\")}"
	conf.URLs = urls
	conf.Type = "_doc"
	conf.Sniff = false

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	N := 10

	testMsgs := [][][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		})
	}
	for i := 0; i < N; i++ {
		if err = m.Write(message.QuickBatch(testMsgs[i])); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsgs[i][0]), string(sourceBytes); exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testElasticIndexInterpolation(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"bar\")}"
	conf.URLs = urls
	conf.Type = "_doc"
	conf.Sniff = false

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	N := 10

	testMsgs := [][][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		})
	}
	for i := 0; i < N; i++ {
		msg := message.QuickBatch(testMsgs[i])
		msg.Get(0).MetaSetMut("index", "test_conn_index")
		if err = m.Write(msg); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsgs[i][0]), string(sourceBytes); exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testElasticBatch(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"bar\")}"
	conf.URLs = urls
	conf.Sniff = false
	conf.Type = "_doc"

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	N := 10

	testMsg := [][]byte{}
	for i := 0; i < N; i++ {
		testMsg = append(testMsg,
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		)
	}
	msg := message.QuickBatch(testMsg)
	for i := 0; i < N; i++ {
		msg.Get(i).MetaSetMut("index", "test_conn_index")
	}
	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), string(sourceBytes); exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testElasticBatchDelete(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"elasticBatchDeleteMessages\")}"
	conf.Action = "${!meta(\"elastic_action\")}"
	conf.URLs = urls
	conf.Sniff = false
	conf.Type = "_doc"

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	N := 10

	testMsg := [][]byte{}
	for i := 0; i < N; i++ {
		testMsg = append(testMsg,
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		)
	}
	msg := message.QuickBatch(testMsg)
	for i := 0; i < N; i++ {
		msg.Get(i).MetaSetMut("index", "test_conn_index")
		msg.Get(i).MetaSetMut("elastic_action", "index")
	}
	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), string(sourceBytes); exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}

	// Set elastic_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		msg.Get(i).MetaSetMut("elastic_action", "delete")
	}

	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index("test_conn_index").
			Type("_doc").
			Id(id).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		partAction := msg.Get(i).MetaGetStr("elastic_action")
		if partAction == "deleted" && get.Found {
			t.Errorf("document %v found when it should have been deleted", i)
		} else if partAction != "deleted" && !get.Found {
			t.Errorf("document %v was not found", i)
		}
	}
}

func testElasticBatchIDCollision(urls []string, client *elastic.Client, t *testing.T) {
	conf := output.NewElasticsearchConfig()
	conf.Index = `${!meta("index")}`
	conf.ID = "bar-id"
	conf.URLs = urls
	conf.Sniff = false
	conf.Type = "_doc"

	m, err := elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	N := 2

	testMsg := [][]byte{}
	for i := 0; i < N; i++ {
		testMsg = append(testMsg,
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		)
	}

	msg := message.QuickBatch(testMsg)
	msg.Get(0).MetaSetMut("index", "test_conn_index")
	msg.Get(1).MetaSetMut("index", "test_conn_index_2")

	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
		get, err := client.Get().
			Index(msg.Get(i).MetaGetStr("index")).
			Type("_doc").
			Id(conf.ID).
			Do(context.Background())
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", conf.ID, err)
		}
		if !get.Found {
			t.Errorf("document %v not found", i)
		}

		var sourceBytes []byte
		sourceBytes, err = get.Source.MarshalJSON()
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), string(sourceBytes); exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}

	// testing sequential updates to a document created above
	conf.Action = "update"
	conf.Index = "test_conn_index"
	conf.ID = "bar-id"

	m, err = elasticsearch.NewElasticsearchV2(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	defer func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, m.Close(ctx))
		done()
	}()

	testMsg = [][]byte{
		[]byte(`{"message":"goodbye"}`),
		[]byte(`{"user": "updated"}`),
	}
	msg = message.QuickBatch(testMsg)
	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}

	// nolint:staticcheck // Ignore SA1019 Type is deprecated warning for .Index()
	get, err := client.Get().
		Index("test_conn_index").
		Type("_doc").
		Id(conf.ID).
		Do(context.Background())
	if err != nil {
		t.Fatalf("Failed to get doc '%v': %v", conf.ID, err)
	}
	if !get.Found {
		t.Errorf("document not found")
	}

	var doc struct {
		Message string `json:"message"`
		User    string `json:"user"`
	}
	err = json.Unmarshal(get.Source, &doc)
	if err != nil {
		t.Error(err)
	} else if doc.User != "updated" {
		t.Errorf("wrong user field returned: %v != %v", doc.User, "updated")
	} else if doc.Message != "goodbye" {
		t.Errorf("wrong message field returned: %v != %v", doc.Message, "goodbye")
	}
}
