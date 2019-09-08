// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package writer

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/olivere/elastic"
	"github.com/ory/dockertest"
)

func TestElasticIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("elasticsearch", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urls := []string{fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))}

	var client *elastic.Client

	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(urls...),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
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
		"doc":{
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
	}
}`
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(index).
				Do(context.Background())
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
}

func testElasticNoIndex(urls []string, client *elastic.Client, t *testing.T) {
	conf := NewElasticsearchConfig()
	conf.Index = "does_not_exist"
	conf.ID = "foo-${!count:noIndexTest}"
	conf.URLs = urls
	conf.MaxRetries = 1
	conf.Backoff.MaxElapsedTime = "1s"

	m, err := NewElasticsearch(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Error(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	if err = m.Write(message.New([][]byte{[]byte(`{"user":"1","message":"hello world"}`)})); err != nil {
		t.Error(err)
	}

	if err = m.Write(message.New([][]byte{
		[]byte(`{"user":"2","message":"hello world"}`),
		[]byte(`{"user":"3","message":"hello world"}`),
	})); err != nil {
		t.Error(err)
	}

	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := client.Get().
			Index("does_not_exist").
			Type("doc").
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

func testElasticErrorHandling(urls []string, client *elastic.Client, t *testing.T) {
	conf := NewElasticsearchConfig()
	conf.Index = "test_conn_index?"
	conf.ID = "foo-static"
	conf.URLs = urls
	conf.Backoff.MaxInterval = "1s"

	m, err := NewElasticsearch(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	if err = m.Write(message.New([][]byte{[]byte(`{"message":true}`)})); err == nil {
		t.Error("Expected error")
	}

	if err = m.Write(message.New([][]byte{[]byte(`{"message":"foo"}`), []byte(`{"message":"bar"}`)})); err == nil {
		t.Error("Expected error")
	}
}

func testElasticConnect(urls []string, client *elastic.Client, t *testing.T) {
	conf := NewElasticsearchConfig()
	conf.Index = "test_conn_index"
	conf.ID = "foo-${!count:foo}"
	conf.URLs = urls

	m, err := NewElasticsearch(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	testMsgs := [][][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"user":"%v","message":"hello world"}`, i)),
		})
	}
	for i := 0; i < N; i++ {
		if err = m.Write(message.New(testMsgs[i])); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("foo-%v", i+1)
		get, err := client.Get().
			Index("test_conn_index").
			Type("doc").
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
		} else {
			if exp, act := string(testMsgs[i][0]), string(sourceBytes); exp != act {
				t.Errorf("wrong user field returned: %v != %v", act, exp)
			}
		}
	}
}

func testElasticIndexInterpolation(urls []string, client *elastic.Client, t *testing.T) {
	conf := NewElasticsearchConfig()
	conf.Index = "${!metadata:index}"
	conf.ID = "bar-${!count:bar}"
	conf.URLs = urls

	m, err := NewElasticsearch(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	testMsgs := [][][]byte{}
	for i := 0; i < N; i++ {
		testMsgs = append(testMsgs, [][]byte{
			[]byte(fmt.Sprintf(`{"user":"%v","message":"hello world"}`, i)),
		})
	}
	for i := 0; i < N; i++ {
		msg := message.New(testMsgs[i])
		msg.Get(0).Metadata().Set("index", "test_conn_index")
		if err = m.Write(msg); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := client.Get().
			Index("test_conn_index").
			Type("doc").
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
		} else {
			if exp, act := string(testMsgs[i][0]), string(sourceBytes); exp != act {
				t.Errorf("wrong user field returned: %v != %v", act, exp)
			}
		}
	}
}

func testElasticBatch(urls []string, client *elastic.Client, t *testing.T) {
	conf := NewElasticsearchConfig()
	conf.Index = "${!metadata:index}"
	conf.ID = "bar-${!count:bar}"
	conf.URLs = urls

	m, err := NewElasticsearch(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	testMsg := [][]byte{}
	for i := 0; i < N; i++ {
		testMsg = append(testMsg,
			[]byte(fmt.Sprintf(`{"user":"%v","message":"hello world"}`, i)),
		)
	}
	msg := message.New(testMsg)
	for i := 0; i < N; i++ {
		msg.Get(i).Metadata().Set("index", "test_conn_index")
	}
	if err = m.Write(msg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		id := fmt.Sprintf("bar-%v", i+1)
		get, err := client.Get().
			Index("test_conn_index").
			Type("doc").
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
		} else {
			if exp, act := string(testMsg[i]), string(sourceBytes); exp != act {
				t.Errorf("wrong user field returned: %v != %v", act, exp)
			}
		}
	}
}
