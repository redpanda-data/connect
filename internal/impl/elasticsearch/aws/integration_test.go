// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/elasticsearch"

	_ "github.com/redpanda-data/connect/v4/internal/impl/elasticsearch/aws"
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

	// integration.CheckSkip(t)
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

	awsConf, err := service.NewConfigSpec().Field(elasticsearch.AWSField()).ParseYAML(fmt.Sprintf(`
aws:
  enabled: true
  endpoint: "http://localhost:%v"
  region: eu-west-1
  credentials:
    id: xxxxx
    secret: xxxxx
    token: xxxxx
`, servicePort), nil)
	require.NoError(t, err)

	awsOpts, err := elasticsearch.AWSOptFn(awsConf)
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
