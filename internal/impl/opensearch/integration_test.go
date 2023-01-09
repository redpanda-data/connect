package opensearch_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	os "github.com/opensearch-project/opensearch-go/v2"
	osapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/tidwall/gjson"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

var openSearchIndex = `{
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

func TestIntegrationOpenSearch(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("opensearchproject/opensearch", "2.4.0", []string{
		"discovery.type=single-node",
		"DISABLE_SECURITY_PLUGIN=true",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var client *os.Client
	if err = pool.Retry(func() error {
		opts := os.Config{
			Addresses: []string{fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))},
			Transport: http.DefaultTransport,
		}

		var cerr error
		if client, cerr = os.NewClient(opts); cerr == nil {
			_, cerr = osapi.IndicesCreateRequest{
				Index:   "test_conn_index",
				Body:    strings.NewReader(openSearchIndex),
				Timeout: time.Second * 20,
			}.Do(context.Background(), client)
		}
		return cerr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  opensearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		res, err := osapi.GetRequest{
			Index:      testID,
			DocumentID: messageID,
		}.Do(ctx, client)
		if err != nil {
			return "", nil, err
		}

		if res.StatusCode != 200 {
			return "", nil, fmt.Errorf("document %v not found", messageID)
		}

		response := read(res.Body)
		source := gjson.Get(response, "_source")

		if err != nil {
			return "", nil, err
		}
		return source.Raw, nil, nil
	}

	suite := integration.StreamTests(
		integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("9200/tcp")),
	)
}

func BenchmarkIntegrationOpenSearch(b *testing.B) {
	integration.CheckSkip(b)

	pool, err := dockertest.NewPool("")
	require.NoError(b, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("opensearchproject/opensearch", "2.4.0", []string{
		"discovery.type=single-node",
		"DISABLE_SECURITY_PLUGIN=true",
	})
	require.NoError(b, err)
	b.Cleanup(func() {
		assert.NoError(b, pool.Purge(resource))
	})

	var client *os.Client
	if err = pool.Retry(func() error {
		opts := os.Config{
			Addresses: []string{fmt.Sprintf("http://localhost:%v", resource.GetPort("9200/tcp"))},
			Transport: http.DefaultTransport,
		}

		var cerr error
		if client, cerr = os.NewClient(opts); cerr == nil {
			_, cerr = osapi.IndicesCreateRequest{
				Index:   "test_conn_index",
				Body:    strings.NewReader(openSearchIndex),
				Timeout: time.Second * 20,
			}.Do(context.Background(), client)
		}
		return cerr
	}); err != nil {
		b.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  opensearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
`
	suite := integration.StreamBenchs(
		integration.StreamBenchWrite(20),
		integration.StreamBenchWrite(10),
		integration.StreamBenchWrite(1),
	)
	suite.Run(
		b, template,
		integration.StreamTestOptPort(resource.GetPort("9200/tcp")),
	)
}
