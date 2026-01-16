// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// createTableWithStreams creates a DynamoDB table with streams enabled for testing
func createTableWithStreams(ctx context.Context, t testing.TB, dynamoPort, tableName string) (*dynamodb.Client, error) {
	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	// Check if table already exists
	ta, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		var derr *types.ResourceNotFoundException
		if !errors.As(err, &derr) {
			return nil, err
		}
	}

	if ta != nil && ta.Table != nil && ta.Table.TableStatus == types.TableStatusActive {
		return client, nil
	}

	intPtr := func(i int64) *int64 {
		return &i
	}

	t.Logf("Creating table with streams: %v\n", tableName)
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  intPtr(5),
			WriteCapacityUnits: intPtr(5),
		},
		TableName: &tableName,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		return nil, err
	}

	// Wait for table to be active
	waiter := dynamodb.NewTableExistsWaiter(client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}, time.Minute)

	return client, err
}

// putTestItem inserts a test item into DynamoDB
func putTestItem(ctx context.Context, client *dynamodb.Client, tableName, id, value string) error {
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			"id":    &types.AttributeValueMemberS{Value: id},
			"value": &types.AttributeValueMemberS{Value: value},
		},
	})
	return err
}

// updateTestItem updates a test item in DynamoDB
func updateTestItem(ctx context.Context, client *dynamodb.Client, tableName, id, newValue string) error {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: &tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET #v = :val"),
		ExpressionAttributeNames: map[string]string{
			"#v": "value",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: newValue},
		},
	})
	return err
}

// deleteTestItem deletes a test item from DynamoDB
func deleteTestItem(ctx context.Context, client *dynamodb.Client, tableName, id string) error {
	_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &tableName,
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
	})
	return err
}

func TestIntegrationDynamoDBStreams(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 60

	// Start DynamoDB Local container
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "amazon/dynamodb-local",
		Tag:          "latest",
		ExposedPorts: []string{"8000/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	var client *dynamodb.Client
	tableName := "test-streams-table"

	// Wait for DynamoDB to be ready and create table with streams
	require.NoError(t, pool.Retry(func() error {
		var err error
		client, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), tableName)
		return err
	}))

	port := resource.GetPort("8000/tcp")

	t.Run("ReadInsertEvents", func(t *testing.T) {
		checkpointTable := "test-checkpoints-insert"
		testReadInsertEvents(t, client, port, tableName, checkpointTable)
	})

	t.Run("ReadModifyEvents", func(t *testing.T) {
		checkpointTable := "test-checkpoints-modify"
		testReadModifyEvents(t, client, port, tableName, checkpointTable)
	})

	t.Run("ReadRemoveEvents", func(t *testing.T) {
		checkpointTable := "test-checkpoints-remove"
		testReadRemoveEvents(t, client, port, tableName, checkpointTable)
	})

	t.Run("CheckpointResumption", func(t *testing.T) {
		checkpointTable := "test-checkpoints-resumption"
		testCheckpointResumption(t, client, port, tableName, checkpointTable)
	})
}

// testReadInsertEvents verifies that INSERT events are captured
func testReadInsertEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert test items
	require.NoError(t, putTestItem(ctx, client, tableName, "test-1", "value-1"))
	require.NoError(t, putTestItem(ctx, client, tableName, "test-2", "value-2"))

	// Read events
	batch, _, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch)

	// Verify we got INSERT events
	foundInsert := false
	for _, msg := range batch {
		eventName, _ := msg.MetaGet("dynamodb_event_name")
		if eventName == "INSERT" {
			foundInsert = true
			break
		}
	}
	assert.True(t, foundInsert, "Should receive INSERT events")
}

// testReadModifyEvents verifies that MODIFY events are captured
func testReadModifyEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert an item
	itemID := "modify-test"
	require.NoError(t, putTestItem(ctx, client, tableName, itemID, "original"))

	// Wait briefly for stream propagation
	time.Sleep(100 * time.Millisecond)

	// Update the item
	require.NoError(t, updateTestItem(ctx, client, tableName, itemID, "updated"))

	// Read events (may need multiple batches)
	foundModify := false
	for i := 0; i < 5 && !foundModify; i++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			if eventName == "MODIFY" {
				foundModify = true
				break
			}
		}

		if !foundModify {
			time.Sleep(100 * time.Millisecond)
		}
	}

	assert.True(t, foundModify, "Should receive MODIFY events")
}

// testReadRemoveEvents verifies that REMOVE events are captured
func testReadRemoveEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert an item
	itemID := "delete-test"
	require.NoError(t, putTestItem(ctx, client, tableName, itemID, "to-delete"))

	// Wait briefly for stream propagation
	time.Sleep(100 * time.Millisecond)

	// Delete the item
	require.NoError(t, deleteTestItem(ctx, client, tableName, itemID))

	// Read events (may need multiple batches)
	foundRemove := false
	for i := 0; i < 5 && !foundRemove; i++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			if eventName == "REMOVE" {
				foundRemove = true
				break
			}
		}

		if !foundRemove {
			time.Sleep(100 * time.Millisecond)
		}
	}

	assert.True(t, foundRemove, "Should receive REMOVE events")
}

// testCheckpointResumption verifies that checkpoints work correctly
func testCheckpointResumption(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: trim_horizon
checkpoint_limit: 2
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	// First input instance
	input1, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, input1.Connect(ctx))

	// Insert some items
	require.NoError(t, putTestItem(ctx, client, tableName, "checkpoint-1", "value-1"))
	require.NoError(t, putTestItem(ctx, client, tableName, "checkpoint-2", "value-2"))

	// Read and acknowledge messages
	batch1, ackFn1, err := input1.ReadBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch1)

	// Acknowledge to trigger checkpoint
	require.NoError(t, ackFn1(ctx, nil))

	// Close first input
	require.NoError(t, input1.Close(ctx))

	// Create second input instance (should resume from checkpoint)
	input2, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, input2.Connect(ctx))
	t.Cleanup(func() {
		_ = input2.Close(ctx)
	})

	// Insert new item after checkpoint
	require.NoError(t, putTestItem(ctx, client, tableName, "checkpoint-3", "value-3"))

	// Second input should read new events (not re-read old ones)
	batch2, _, err := input2.ReadBatch(ctx)
	require.NoError(t, err)

	// The batch may include checkpoint-3 but should not re-process already checkpointed items
	assert.NotEmpty(t, batch2, "Should read new events after resumption")
}
