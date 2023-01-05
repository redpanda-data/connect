package opensearch_test

import (
	"bytes"
	"context"
	"io"

	"github.com/tidwall/gjson"

	"github.com/benthosdev/benthos/v4/internal/impl/opensearch"

	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	os "github.com/opensearch-project/opensearch-go/v2"
	osapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
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
	pool.MaxWait = time.Second * 60

	resource, err := pool.Run("opensearchproject/opensearch", "2.2.0", []string{
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
		testOpenSearchErrorHandling(urls, te)
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
	conf := output.NewOpenSearchConfig()
	conf.Index = "does_not_exist"
	conf.ID = "foo-${!count(\"noIndexTest\")}"
	conf.URLs = urls
	conf.MaxRetries = 1
	conf.Backoff.MaxElapsedTime = "1s"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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

	if err = m.Write(context.Background(), message.QuickBatch([][]byte{[]byte(`{"message":"hello world","user":"1"}`)})); err != nil {
		t.Error(err)
	}

	if err = m.Write(context.Background(), message.QuickBatch([][]byte{
		[]byte(`{"message":"hello world","user":"2"}`),
		[]byte(`{"message":"hello world","user":"3"}`),
	})); err != nil {
		t.Error(err)
	}

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := osapi.IndicesExistsRequest{
			Index: []string{"does_not_exist"},
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}
	}
}

func testOpenSearchParallelWrites(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "new_index_parallel_writes"
	conf.ID = "${!json(\"key\")}"
	conf.URLs = urls
	conf.MaxRetries = 1
	conf.Backoff.MaxElapsedTime = "1s"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
			if lerr := m.Write(context.Background(), message.QuickBatch([][]byte{[]byte(content)})); lerr != nil {
				t.Error(lerr)
			}
			wg.Done()
		}(str)
	}

	close(startChan)
	wg.Wait()

	for id, exp := range docs {
		get, err := osapi.GetRequest{
			Index:      "new_index_parallel_writes",
			DocumentID: id,
		}.Do(context.Background(), client)
		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", id)
		} else {
			response := read(get.Body)
			source := gjson.Get(response, "_source")
			if act := source.Raw; act != exp {
				t.Errorf("Wrong result: %v != %v", act, exp)
			}
		}
	}
}

func testOpenSearchErrorHandling(urls []string, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "test_conn_index?"
	conf.ID = "foo-static"
	conf.URLs = urls
	conf.Backoff.MaxInterval = "1s"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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

	if err = m.Write(context.Background(), message.QuickBatch([][]byte{[]byte(`{"message":invalid json`)})); err == nil {
		t.Error("Expected error")
	}

	if err = m.Write(context.Background(), message.QuickBatch([][]byte{[]byte(`{"message":"foo}`), []byte(`{"message":"bar"}`)})); err == nil {
		t.Error("Expected error")
	}
}

func testOpenSearchConnect(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "test_conn_index"
	conf.ID = "foo-${!count(\"foo\")}"
	conf.URLs = urls
	conf.Type = "_doc"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
		if err = m.Write(context.Background(), message.QuickBatch(testMsgs[i])); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("foo-%v", i+1)

		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}

		response := read(get.Body)
		source := gjson.Get(response, "_source")
		if exp, act := string(testMsgs[i][0]), source.Raw; exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testOpenSearchIndexInterpolation(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"bar\")}"
	conf.URLs = urls
	conf.Type = "_doc"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
		if err = m.Write(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)

		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}

		response := read(get.Body)
		source := gjson.Get(response, "_source")
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsgs[i][0]), source.Raw; exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testOpenSearchBatch(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"bar\")}"
	conf.URLs = urls

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
	if err = m.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)

		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}

		response := read(get.Body)
		source := gjson.Get(response, "_source")
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), source.Raw; exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}
}

func testOpenSearchBatchDelete(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = "${!meta(\"index\")}"
	conf.ID = "bar-${!count(\"openSearchBatchDeleteMessages\")}"
	conf.Action = "${!meta(\"opensearch_action\")}"
	conf.URLs = urls

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
		msg.Get(i).MetaSetMut("opensearch_action", "index")
	}
	if err = m.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}

		response := read(get.Body)
		source := gjson.Get(response, "_source")
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), source.Raw; exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}

	// Set opensearch_action to deleted for some message parts
	for i := N / 2; i < N; i++ {
		msg.Get(i).MetaSetMut("opensearch_action", "delete")
	}

	if err = m.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := osapi.GetRequest{
			Index:      "test_conn_index",
			DocumentID: id,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", id, err)
		}
		partAction := msg.Get(i).MetaGetStr("opensearch_action")
		if partAction == "deleted" && get.StatusCode == 200 {
			t.Errorf("document %v found when it should have been deleted", i)
		} else if partAction != "deleted" && get.StatusCode != 200 {
			t.Errorf("document %v was not found", i)
		}
	}
}

func testOpenSearchBatchIDCollision(urls []string, client *os.Client, t *testing.T) {
	conf := output.NewOpenSearchConfig()
	conf.Index = `${!meta("index")}`
	conf.ID = "bar-id"
	conf.URLs = urls
	conf.Type = "_doc"

	m, err := opensearch.NewOpenSearchV2(conf, mock.NewManager())
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

	var testMsg [][]byte
	for i := 0; i < N; i++ {
		testMsg = append(testMsg,
			[]byte(fmt.Sprintf(`{"message":"hello world","user":"%v"}`, i)),
		)
	}

	msg := message.QuickBatch(testMsg)
	msg.Get(0).MetaSetMut("index", "test_conn_index")
	msg.Get(1).MetaSetMut("index", "test_conn_index_2")

	if err = m.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		get, err := osapi.GetRequest{
			Index:      msg.Get(i).MetaGetStr("index"),
			DocumentID: conf.ID,
		}.Do(context.Background(), client)

		if err != nil {
			t.Fatalf("Failed to get doc '%v': %v", conf.ID, err)
		}
		if get.StatusCode != 200 {
			t.Errorf("document %v not found", i)
		}

		response := read(get.Body)
		source := gjson.Get(response, "_source")
		if err != nil {
			t.Error(err)
		} else if exp, act := string(testMsg[i]), source.Raw; exp != act {
			t.Errorf("wrong user field returned: %v != %v", act, exp)
		}
	}

	// testing sequential updates to a document created above
	conf.Action = "update"
	conf.Index = "test_conn_index"
	conf.ID = "bar-id"

	m, err = opensearch.NewOpenSearchV2(conf, mock.NewManager())
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
	if err = m.Write(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	get, err := osapi.GetRequest{
		Index:      "test_conn_index",
		DocumentID: conf.ID,
	}.Do(context.Background(), client)

	if err != nil {
		t.Fatalf("Failed to get doc '%v': %v", conf.ID, err)
	}
	if get.StatusCode != 200 {
		t.Errorf("document not found")
	}
	response := read(get.Body)
	srcMessage := gjson.Get(response, "_source.message").String()
	user := gjson.Get(response, "_source.user").String()

	if err != nil {
		t.Error(err)
	} else if user != "updated" {
		t.Errorf("wrong user field returned: %v != %v", user, "updated")
	} else if srcMessage != "goodbye" {
		t.Errorf("wrong message field returned: %v != %v", srcMessage, "goodbye")
	}
}

// Read method to read the content from io.reader to string
func read(r io.Reader) string {
	var b bytes.Buffer
	_, err := b.ReadFrom(r)
	if err != nil {
		return ""
	}
	return b.String()
}
