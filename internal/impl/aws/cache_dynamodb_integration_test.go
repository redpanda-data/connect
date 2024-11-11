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

package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func createTable(ctx context.Context, t testing.TB, dynamoPort, id string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	table := id
	hashKey := "id"

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	ta, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		var derr *types.ResourceNotFoundException
		if !errors.As(err, &derr) {
			return err
		}
	}

	if ta != nil && ta.Table != nil && ta.Table.TableStatus == types.TableStatusActive {
		return nil
	}

	intPtr := func(i int64) *int64 {
		return &i
	}

	t.Logf("Creating table: %v\n", table)
	_, _ = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: &hashKey,
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: &hashKey,
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  intPtr(5),
			WriteCapacityUnits: intPtr(5),
		},
		TableName: &table,
	})

	waiter := dynamodb.NewTableExistsWaiter(client)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: &table,
	}, time.Minute)
}

func TestIntegrationDynamoDBCache(t *testing.T) {
	// integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "amazon/dynamodb-local",
		ExposedPorts: []string{"8000/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createTable(context.Background(), t, resource.GetPort("8000/tcp"), "poketable")
	}))

	template := `
cache_resources:
  - label: testcache
    aws_dynamodb:
      endpoint: http://localhost:$PORT
      region: us-east-1
      consistent_read: true
      data_key: data
      hash_key: id
      table: $ID
      credentials:
        id: xxxxx
        secret: xxxxx
        token: xxxxx
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(resource.GetPort("8000/tcp")),
		integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
			require.NoError(t, createTable(ctx, t, resource.GetPort("8000/tcp"), vars.ID))
		}),
	)
}
