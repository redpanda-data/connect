package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func createTable(ctx context.Context, t testing.TB, dynamoPort, id string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	table := id
	hashKey := "id"

	client := dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("us-east-1"),
	})))

	ta, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != dynamodb.ErrCodeResourceNotFoundException {
			return err
		}
	}

	if ta != nil && ta.Table != nil && ta.Table.TableStatus != nil && *ta.Table.TableStatus == dynamodb.TableStatusActive {
		return nil
	}

	t.Logf("Creating table: %v\n", table)
	_, _ = client.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(table),
	})

	// wait for table to exist
	return client.WaitUntilTableExistsWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
}

func TestIntegrationDynamoDBCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "peopleperhour/dynamodb",
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
		integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
			require.NoError(t, createTable(ctx, t, resource.GetPort("8000/tcp"), testID))
		}),
	)
}
