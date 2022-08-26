package aws_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	os "github.com/opensearch-project/opensearch-go/v2"
	osapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	"github.com/tidwall/gjson"

	"github.com/benthosdev/benthos/v4/internal/impl/opensearch"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
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

func TestIntegrationOpenSearchAWS(t *testing.T) {
	t.Skip("Struggling to get localstack opensearch to work, maybe one day")

	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack-full",
		ExposedPorts: []string{"4566/tcp"},
		Env:          []string{"SERVICES=es"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	servicePort := resource.GetPort("4566/tcp")

	aConf := output.NewOpenSearchConfig()
	aConf.AWS.Enabled = true
	aConf.AWS.Endpoint = "http://localhost:" + servicePort
	aConf.AWS.Region = "eu-west-1"
	aConf.AWS.Credentials.ID = "xxxxx"
	aConf.AWS.Credentials.Secret = "xxxxx"
	aConf.AWS.Credentials.Token = "xxxxx"

	require.NoError(t, err)

	var client *os.Client
	if err = pool.Retry(func() error {
		opts := os.Config{
			Addresses: []string{fmt.Sprintf("http://localhost:%v", servicePort)},
			Transport: http.DefaultTransport,
		}
		opts.Transport, _ = opensearch.AWSOptFn(opts.Transport, aConf)

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
    aws:
      enabled: true
      endpoint: http://localhost:$PORT
      region: eu-west-1
      credentials:
        id: xxxxx
        secret: xxxxx
        token: xxxxx
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
		integration.StreamTestOptPort(servicePort),
	)
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
