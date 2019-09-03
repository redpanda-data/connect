package cache

import (
	"fmt"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ory/dockertest"
)

func TestDynamoDBIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "peopleperhour/dynamodb",
		ExposedPorts: []string{"8000/tcp"},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	endpoint := fmt.Sprintf("http://localhost:%v", resource.GetPort("8000/tcp"))
	table := "mycache"
	hashKey := "id"
	dataKey := "data"

	client := dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("us-east-1"),
	})))
	if err = pool.Retry(func() error {
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
		// attempt to create test table
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
		return client.WaitUntilTableExists(&dynamodb.DescribeTableInput{
			TableName: aws.String(table),
		})
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	conf := NewConfig()
	conf.DynamoDB.Endpoint = endpoint
	conf.DynamoDB.ConsistentRead = true
	conf.DynamoDB.Credentials.ID = "xxxxx"
	conf.DynamoDB.Credentials.Secret = "xxxxx"
	conf.DynamoDB.Credentials.Token = "xxxxx"
	conf.DynamoDB.DataKey = dataKey
	conf.DynamoDB.HashKey = hashKey
	conf.DynamoDB.Region = "us-east-1"
	conf.DynamoDB.Table = table

	t.Run("testDynamodbGetAndSet", func(t *testing.T) {
		testDynamodbGetAndSet(t, conf)
	})

	t.Run("testDynamodbGetAndSetMulti", func(t *testing.T) {
		testDynamodbGetAndSetMulti(t, conf)
	})

	t.Run("testDynamodbAddAndDelete", func(t *testing.T) {
		testDynamodbAddAndDelete(t, conf)
	})
}

func testDynamodbGetAndSet(t *testing.T, conf Config) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	c, err := NewDynamoDB(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	exp := []byte(`{"foo":"bar"}`)
	if err := c.Set("foo", exp); err != nil {
		t.Error(err)
	}
	defer func() {
		if err := c.Delete("foo"); err != nil {
			t.Error(err)
		}
	}()

	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != string(exp) {
		t.Errorf("Expected key 'foo' to have value %s, got %s", string(exp), string(act))
	}
}

func testDynamodbGetAndSetMulti(t *testing.T, conf Config) {
	c, err := NewDynamoDB(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	exp := map[string][]byte{
		"foo": []byte(`{"test":"foo"}`),
		"bar": []byte(`{"test":"bar"}`),
		"baz": []byte(`{"test":"baz"}`),
	}

	if err := c.SetMulti(exp); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for k := range exp {
			if err := c.Delete(k); err != nil {
				t.Error(err)
			}
		}
	}()

	for k, v := range exp {
		if act, err := c.Get(k); err != nil {
			t.Error(err)
		} else if string(act) != string(v) {
			t.Errorf("Expected key '%v' to have value %s, got %s", k, string(v), string(act))
		}
	}
}

func testDynamodbAddAndDelete(t *testing.T, conf Config) {
	testLog := log.New(os.Stdout, log.Config{LogLevel: "NONE"})
	c, err := NewDynamoDB(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Add("foo", []byte(`{"foo":"bar"}`)); err != nil {
		t.Error(err)
	}
	defer func() {
		if err := c.Delete("foo"); err != nil {
			t.Error(err)
		}
	}()

	exp := []byte(`{"foo":"baz"}`)
	if err := c.Add("foo", exp); err != types.ErrKeyAlreadyExists {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyAlreadyExists)
	}

	if err := c.Delete("foo"); err != nil {
		t.Error(err)
	}

	if err := c.Add("foo", exp); err != nil {
		t.Error(err)
	}

	if act, err := c.Get("foo"); err != nil {
		t.Error(err)
	} else if string(act) != string(exp) {
		t.Errorf("Expected key 'foo' to have value %s, got %s", string(exp), string(act))
	}
}
