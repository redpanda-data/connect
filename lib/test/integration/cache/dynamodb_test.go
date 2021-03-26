package cache

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
)

func createTable(ctx context.Context, t *testing.T, dynamoPort, id string) error {
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
	client.CreateTable(&dynamodb.CreateTableInput{
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

var _ = registerIntegrationTest("dynamodb", func(t *testing.T) {
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

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createTable(context.Background(), t, resource.GetPort("8000/tcp"), "poketable")
	}))

	template := `
resource_caches:
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
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMissingKey(),
		integrationTestDoubleAdd(),
		integrationTestDelete(),
		integrationTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		testOptPort(resource.GetPort("8000/tcp")),
		testOptPreTest(func(t *testing.T, env *testEnvironment) {
			require.NoError(t, createTable(env.ctx, t, resource.GetPort("8000/tcp"), env.configVars.id))
		}),
	)
})
