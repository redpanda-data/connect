package aws_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/elasticsearch"
	"github.com/benthosdev/benthos/v4/internal/integration"

	_ "github.com/benthosdev/benthos/v4/internal/impl/elasticsearch/aws"
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

func TestIntegrationElasticsearchAWS(t *testing.T) {
	t.Skip("Struggling to get localstack es to work, maybe one day")

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

	aConf := output.NewElasticsearchConfig()
	aConf.AWS.Enabled = true
	aConf.AWS.Endpoint = "http://localhost:" + servicePort
	aConf.AWS.Region = "eu-west-1"
	aConf.AWS.Credentials.ID = "xxxxx"
	aConf.AWS.Credentials.Secret = "xxxxx"
	aConf.AWS.Credentials.Token = "xxxxx"

	awsOpts, err := elasticsearch.AWSOptFn(aConf)
	require.NoError(t, err)

	var client *elastic.Client
	if err = pool.Retry(func() error {
		opts := []elastic.ClientOptionFunc{
			elastic.SetURL(fmt.Sprintf("http://localhost:%v", servicePort)),
			elastic.SetSniff(false),
			elastic.SetHealthcheck(false),
		}
		opts = append(opts, awsOpts...)

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

	_ = resource.Expire(900)

	template := `
output:
  elasticsearch:
    urls:
      - http://localhost:$PORT
    index: $ID
    id: ${!json("id")}
    sniff: false
    healthcheck: false
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
		res, err := client.Get().
			Index(testID).
			Id(messageID).
			Do(ctx)
		if err != nil {
			return "", nil, err
		}

		if !res.Found {
			return "", nil, fmt.Errorf("document %v not found", messageID)
		}

		resBytes, err := res.Source.MarshalJSON()
		if err != nil {
			return "", nil, err
		}
		return string(resBytes), nil, nil
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
