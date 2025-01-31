// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package elasticsearchv8

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationElasticsearch(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctx := context.Background()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 60

	resource, err := pool.Run("docker.elastic.co/elasticsearch/elasticsearch", "8.17.1", []string{
		"discovery.type=single-node",
		"xpack.security.enabled=false",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	})

	url := fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("9200/tcp"))

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := client.Ping().Do(ctx)
		return err == nil && ok
	}, time.Second*30, time.Millisecond*500)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddOutputYAML(fmt.Sprintf(`
elasticsearch_v8:
  urls: ['%s']
  index: "things"
  action: ${! meta("action") }
  id: ${! meta("id") }
`, url)))

	inFunc, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	go func() {
		require.NoError(t, stream.Run(ctx))
	}()
	t.Cleanup(func() {
		err := stream.StopWithin(time.Second * 3)
		require.NoError(t, err)
	})

	t.Run("index", func(t *testing.T) {
		msgBytes := []byte(`{"message":"blobfish are cool","likes":1}`)
		msg := service.NewMessage(msgBytes)
		msg.MetaSet("action", "index")
		msg.MetaSet("id", "1")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)

		require.Equal(t, string(msgBytes), string(resp.Source_))
	})

	t.Run("update", func(t *testing.T) {
		msgBytes, err := json.Marshal(map[string]any{
			"script": map[string]any{
				"source": "ctx._source.likes += 1",
				"lang":   "painless",
			},
		})
		require.NoError(t, err)

		msg := service.NewMessage(msgBytes)
		msg.MetaSet("id", "1")
		msg.MetaSet("action", "update")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)

		require.Equal(t, `{"message":"blobfish are cool","likes":2}`, string(resp.Source_))
	})

	t.Run("delete", func(t *testing.T) {
		msg := service.NewMessage([]byte("{}"))
		msg.MetaSet("id", "1")
		msg.MetaSet("action", "delete")
		err = inFunc(ctx, msg)
		require.NoError(t, err)

		resp, err := client.Get("things", "1").Do(ctx)
		require.NoError(t, err)
		require.False(t, resp.Found)
	})
}
