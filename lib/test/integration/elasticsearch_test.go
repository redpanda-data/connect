package integration

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var elasticIndex = `{
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
				"type": "text",
				"store": true,
				"fielddata": true
			}
		}
	}
}`

var _ = registerIntegrationTest("elasticsearch", func(t *testing.T) {
	t.Skip("This uses a ton of memory so we don't run it by default")

	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("elasticsearch", "7.13.4", []string{
		"discovery.type=single-node",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    type: doc
    sniff: false
`
	queryGetFn := func(env *testEnvironment, id string) (string, []string, error) {
		res, err := client.Get().
			Index(env.configVars.id).
			Id(id).
			Do(env.ctx)
		if err != nil {
			return "", nil, err
		}

		if !res.Found {
			return "", nil, fmt.Errorf("document %v not found", id)
		}

		resBytes, err := res.Source.MarshalJSON()
		if err != nil {
			return "", nil, err
		}
		return string(resBytes), nil, nil
	}

	suite := integrationTests(
		integrationTestOutputOnlySendSequential(10, queryGetFn),
		integrationTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		testOptPort(resource.GetPort("9200/tcp")),
	)
})

var _ = registerIntegrationBench("elasticsearch", func(b *testing.B) {
	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("elasticsearch", "7.13.4", []string{
		"discovery.type=single-node",
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))),
			elastic.SetHttpClient(&http.Client{
				Timeout: time.Second,
			}),
			elastic.SetSniff(false),
		}

		var cerr error
		if client, cerr = elastic.NewClient(opts...); cerr == nil {
			_, cerr = client.
				CreateIndex("test_conn_index").
				Timeout("20s").
				Body(elasticIndex).
				Do(context.Background())
		}
		return cerr
	}); err != nil {
		b.Fatalf("Could not connect to docker resource: %s", err)
	}

	resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    type: doc
    sniff: false
`
	suite := integrationBenchs(
		integrationBenchWrite(20),
		integrationBenchWrite(10),
		integrationBenchWrite(1),
	)
	suite.Run(
		b, template,
		testOptPort(resource.GetPort("9200/tcp")),
	)
})
