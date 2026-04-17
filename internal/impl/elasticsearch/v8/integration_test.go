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
package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationElasticsearch(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()

	ctr, err := testcontainers.Run(t.Context(), "docker.elastic.co/elasticsearch/elasticsearch:8.17.1",
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node",
			"cluster.routing.allocation.disk.threshold_enabled": "false",
			"xpack.security.enabled":                            "false",
			"ES_JAVA_OPTS":                                      "-Xms256m -Xmx256m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9200/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("http://127.0.0.1:%v", mappedPort.Port())

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
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	defer func() {
		err := stream.StopWithin(time.Second * 3)
		require.NoError(t, err)
	}()

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

	t.Run("create", func(t *testing.T) {
		// Create a new document
		createMsgBytes := []byte(`{"message":"mantis shrimp are epic","likes":10}`)
		createMsg := service.NewMessage(createMsgBytes)
		createMsg.MetaSet("action", "create")
		createMsg.MetaSet("id", "2")
		err = inFunc(ctx, createMsg)
		require.NoError(t, err)

		resp, err := client.Get("things", "2").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(createMsgBytes), string(resp.Source_))

		// Attempt to create the same document again (should fail)
		err = inFunc(ctx, createMsg)
		require.Error(t, err) // Expecting an error here

		// Verify the document was not overwritten
		resp, err = client.Get("things", "2").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(createMsgBytes), string(resp.Source_))
	})

	t.Run("upsert", func(t *testing.T) {
		// Upsert a new document
		upsertNewMsgBytes := []byte(`{"message":"dragonflies are ancient","likes":5}`)
		upsertNewMsg := service.NewMessage(upsertNewMsgBytes)
		upsertNewMsg.MetaSet("action", "upsert")
		upsertNewMsg.MetaSet("id", "3")
		err = inFunc(ctx, upsertNewMsg)
		require.NoError(t, err)

		resp, err := client.Get("things", "3").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(upsertNewMsgBytes), string(resp.Source_))

		// Upsert an existing document (update)
		upsertUpdateMsgBytes := []byte(`{"message":"dragonflies are truly ancient","likes":6}`)
		upsertUpdateMsg := service.NewMessage(upsertUpdateMsgBytes)
		upsertUpdateMsg.MetaSet("action", "upsert")
		upsertUpdateMsg.MetaSet("id", "3")
		err = inFunc(ctx, upsertUpdateMsg)
		require.NoError(t, err)

		resp, err = client.Get("things", "3").Do(ctx)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.Equal(t, string(upsertUpdateMsgBytes), string(resp.Source_))
	})
}

func TestElasticsearchV8ConnectionTestIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()

	ctr, err := testcontainers.Run(t.Context(), "docker.elastic.co/elasticsearch/elasticsearch:8.17.1",
		testcontainers.WithExposedPorts("9200/tcp"),
		testcontainers.WithEnv(map[string]string{
			"discovery.type": "single-node",
			"cluster.routing.allocation.disk.threshold_enabled": "false",
			"xpack.security.enabled":                            "false",
			"ES_JAVA_OPTS":                                      "-Xms256m -Xmx256m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/").WithPort("9200/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "9200/tcp")
	require.NoError(t, err)
	url := fmt.Sprintf("http://127.0.0.1:%v", mappedPort.Port())

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses: []string{url},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		ok, err := client.Ping().Do(ctx)
		return err == nil && ok
	}, time.Second*30, time.Millisecond*500)

	t.Run("output_valid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(fmt.Sprintf(`
label: test_output
elasticsearch_v8:
  urls: ['%s']
  index: test-index
  action: index
  id: ${! counter() }
`, url)))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.NoError(t, connResults[0].Err)
		}))
	})

	t.Run("output_invalid", func(t *testing.T) {
		resBuilder := service.NewResourceBuilder()

		require.NoError(t, resBuilder.AddOutputYAML(`
label: test_output
elasticsearch_v8:
  urls: ['http://localhost:11111']
  index: test-index
  action: index
  id: ${! counter() }
`))

		resources, _, err := resBuilder.BuildSuspended()
		require.NoError(t, err)

		require.NoError(t, resources.AccessOutput(t.Context(), "test_output", func(o *service.ResourceOutput) {
			connResults := o.ConnectionTest(t.Context())
			require.Len(t, connResults, 1)
			require.Error(t, connResults[0].Err)
		}))
	})
}
