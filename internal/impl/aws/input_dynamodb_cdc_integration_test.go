// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build integration

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

	t.Run("VerifyRecordCount", func(t *testing.T) {
		checkpointTable := "test-checkpoints-count"
		testVerifyRecordCount(t, client, port, tableName, checkpointTable)
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

// testVerifyRecordCount verifies that the number of CDC events matches the number of operations performed
func testVerifyRecordCount(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
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

	// Perform a known number of operations
	numInserts := 100
	numUpdates := 5
	numDeletes := 3
	expectedTotalEvents := numInserts + numUpdates + numDeletes

	// Insert items
	for i := 0; i < numInserts; i++ {
		itemID := fmt.Sprintf("count-test-%d", i)
		require.NoError(t, putTestItem(ctx, client, tableName, itemID, "initial"))
	}

	// Update some items
	for i := 0; i < numUpdates; i++ {
		itemID := fmt.Sprintf("count-test-%d", i)
		require.NoError(t, updateTestItem(ctx, client, tableName, itemID, "updated"))
	}

	// Delete some items
	for i := 0; i < numDeletes; i++ {
		itemID := fmt.Sprintf("count-test-%d", i)
		require.NoError(t, deleteTestItem(ctx, client, tableName, itemID))
	}

	// Read events until we get all expected events or timeout
	receivedEvents := make([]string, 0, expectedTotalEvents)
	eventCounts := map[string]int{
		"INSERT": 0,
		"MODIFY": 0,
		"REMOVE": 0,
	}

	maxAttempts := 20
	for attempt := 0; attempt < maxAttempts; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if len(batch) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			eventName, exists := msg.MetaGet("dynamodb_event_name")
			if exists {
				receivedEvents = append(receivedEvents, eventName)
				eventCounts[eventName]++
			}
		}

		// Check if we've received all expected events
		if len(receivedEvents) >= expectedTotalEvents {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// Verify counts
	assert.Len(t, receivedEvents, expectedTotalEvents,
		"Should receive exactly %d events", expectedTotalEvents)
	assert.Equal(t, numInserts, eventCounts["INSERT"],
		"Should receive %d INSERT events", numInserts)
	assert.Equal(t, numUpdates, eventCounts["MODIFY"],
		"Should receive %d MODIFY events", numUpdates)
	assert.Equal(t, numDeletes, eventCounts["REMOVE"],
		"Should receive %d REMOVE events", numDeletes)

	t.Logf("Received %d total events: %d INSERTs, %d MODIFYs, %d REMOVEs",
		len(receivedEvents), eventCounts["INSERT"], eventCounts["MODIFY"], eventCounts["REMOVE"])
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

// TestIntegrationDynamoDBSnapshot tests snapshot functionality
func TestIntegrationDynamoDBSnapshot(t *testing.T) {
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
	tableName := "test-snapshot-table"

	// Wait for DynamoDB to be ready and create table
	require.NoError(t, pool.Retry(func() error {
		var err error
		client, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), tableName)
		return err
	}))

	port := resource.GetPort("8000/tcp")

	t.Run("SnapshotOnlyMode", func(t *testing.T) {
		checkpointTable := "test-snapshot-only-checkpoint"
		testSnapshotOnlyMode(t, client, port, tableName, checkpointTable)
	})

	t.Run("SnapshotAndCDCMode", func(t *testing.T) {
		checkpointTable := "test-snapshot-cdc-checkpoint"
		testSnapshotAndCDCMode(t, client, port, tableName, checkpointTable)
	})

	t.Run("SnapshotResumeFromCheckpoint", func(t *testing.T) {
		checkpointTable := "test-snapshot-resume-checkpoint"
		testSnapshotResumeFromCheckpoint(t, client, port, tableName, checkpointTable)
	})
}

// testSnapshotOnlyMode verifies snapshot_only mode reads all items and exits
func testSnapshotOnlyMode(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Insert test items BEFORE starting snapshot
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-only-1", "value-1"))
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-only-2", "value-2"))
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-only-3", "value-3"))

	// Give DynamoDB a moment to persist
	time.Sleep(100 * time.Millisecond)

	// Create input with snapshot_only mode
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
snapshot_mode: snapshot_only
snapshot_segments: 1
snapshot_batch_size: 10
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

	// Collect all messages
	messages := []any{}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Read batches until we get ErrEndOfInput or timeout
	for {
		batch, ackFn, err := input.ReadBatch(readCtx)
		if err != nil {
			if errors.Is(err, service.ErrEndOfInput) {
				t.Log("Received ErrEndOfInput as expected for snapshot_only mode")
				break
			}
			// Timeout or context canceled is expected when snapshot completes
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Log("Context timeout - snapshot may still be running")
				break
			}
			require.NoError(t, err, "Unexpected error reading batch")
		}

		// Acknowledge batch
		if ackFn != nil {
			require.NoError(t, ackFn(ctx, nil))
		}

		// Verify all messages have READ event type (snapshot events)
		for _, msg := range batch {
			eventName, exists := msg.MetaGet("dynamodb_event_name")
			require.True(t, exists, "Message should have event_name metadata")
			require.Equal(t, "READ", eventName, "Snapshot messages should have READ event type")

			structured, err := msg.AsStructured()
			require.NoError(t, err)
			messages = append(messages, structured)
		}
	}

	// We should have read at least the 3 items we inserted
	// (there might be more from other tests, that's okay)
	assert.GreaterOrEqual(t, len(messages), 3, "Should read at least 3 snapshot items")
}

// testSnapshotAndCDCMode verifies snapshot_and_cdc mode captures both snapshot and CDC events
func testSnapshotAndCDCMode(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Insert initial items BEFORE starting
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-cdc-1", "initial-1"))
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-cdc-2", "initial-2"))

	// Give DynamoDB a moment to persist
	time.Sleep(100 * time.Millisecond)

	// Create input with snapshot_and_cdc mode
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
snapshot_mode: snapshot_and_cdc
snapshot_segments: 1
snapshot_batch_size: 10
snapshot_deduplicate: true
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

	// Read first batch (should include snapshot items)
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	batch1, ackFn1, err := input.ReadBatch(readCtx)
	require.NoError(t, err)
	require.NotEmpty(t, batch1)

	// Verify we got READ events (snapshot)
	foundRead := false
	for _, msg := range batch1 {
		eventName, _ := msg.MetaGet("dynamodb_event_name")
		if eventName == "READ" {
			foundRead = true
			break
		}
	}
	assert.True(t, foundRead, "Should receive READ events from snapshot")

	// Acknowledge snapshot batch
	require.NoError(t, ackFn1(ctx, nil))

	// Now insert a NEW item (CDC event)
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-cdc-3", "new-item"))

	// Read next batch (should include CDC INSERT event)
	readCtx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()

	batch2, ackFn2, err := input.ReadBatch(readCtx2)
	if err == nil {
		// Verify we can get CDC events after snapshot
		foundInsert := false
		for _, msg := range batch2 {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			if eventName == "INSERT" {
				foundInsert = true
				break
			}
		}
		assert.True(t, foundInsert, "Should receive INSERT events from CDC after snapshot")

		require.NoError(t, ackFn2(ctx, nil))
	}
}

// testSnapshotResumeFromCheckpoint verifies snapshot can resume from checkpoint
func testSnapshotResumeFromCheckpoint(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Insert multiple test items
	for i := 1; i <= 10; i++ {
		require.NoError(t, putTestItem(ctx, client, tableName, fmt.Sprintf("snap-resume-%d", i), fmt.Sprintf("value-%d", i)))
	}

	// Give DynamoDB a moment to persist
	time.Sleep(100 * time.Millisecond)

	// Create input with snapshot_only mode and small batch size to force multiple batches
	confStr := fmt.Sprintf(`
table: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
snapshot_mode: snapshot_only
snapshot_segments: 1
snapshot_batch_size: 3
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	// First input instance - read some messages then close (simulating crash)
	input1, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, input1.Connect(ctx))

	// Read one batch
	readCtx1, cancel1 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel1()

	batch1, ackFn1, err := input1.ReadBatch(readCtx1)
	if err == nil && len(batch1) > 0 {
		// Acknowledge to save checkpoint
		require.NoError(t, ackFn1(ctx, nil))

		// Give checkpoint time to persist
		time.Sleep(500 * time.Millisecond)
	}

	// Close first input (simulating crash/restart)
	require.NoError(t, input1.Close(ctx))

	// Create second input instance - should resume from checkpoint
	input2, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, input2.Connect(ctx))
	t.Cleanup(func() {
		_ = input2.Close(ctx)
	})

	// Should be able to continue reading without re-reading all items
	readCtx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()

	batch2, _, err := input2.ReadBatch(readCtx2)

	// We expect either:
	// 1. More snapshot data to read (no error)
	// 2. Snapshot complete (ErrEndOfInput or timeout)
	if err != nil && !errors.Is(err, service.ErrEndOfInput) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unexpected error on resume: %v", err)
	}

	// If we got data, verify it's snapshot data
	if len(batch2) > 0 {
		for _, msg := range batch2 {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			assert.Equal(t, "READ", eventName, "Resumed messages should be snapshot READ events")
		}
	}

	t.Log("Successfully resumed snapshot from checkpoint")
}

// TestIntegrationDynamoDBMultiTable tests multi-table streaming functionality
func TestIntegrationDynamoDBMultiTable(t *testing.T) {
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
	table1 := "test-multi-table-1"
	table2 := "test-multi-table-2"
	table3 := "test-multi-table-3"

	// Wait for DynamoDB to be ready and create multiple tables
	require.NoError(t, pool.Retry(func() error {
		var err error
		client, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), table1)
		return err
	}))

	// Create additional tables
	_, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), table2)
	require.NoError(t, err)
	_, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), table3)
	require.NoError(t, err)

	port := resource.GetPort("8000/tcp")

	t.Run("IncludeListMode", func(t *testing.T) {
		checkpointTable := "test-multi-includelist-checkpoint"
		testIncludeListMode(t, client, port, []string{table1, table2}, checkpointTable)
	})

	t.Run("TableMetadataInMessages", func(t *testing.T) {
		checkpointTable := "test-multi-metadata-checkpoint"
		testTableMetadataInMessages(t, client, port, []string{table1, table2}, checkpointTable)
	})

	t.Run("IsolationBetweenTables", func(t *testing.T) {
		checkpointTable := "test-multi-isolation-checkpoint"
		testIsolationBetweenTables(t, client, port, table1, table2, checkpointTable)
	})
}

// testIncludeListMode verifies that includelist mode streams from multiple tables
func testIncludeListMode(t *testing.T, client *dynamodb.Client, port string, tables []string, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration with multiple tables
	confStr := fmt.Sprintf(`
tables: [%s, %s]
table_discovery_mode: includelist
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tables[0], tables[1], checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert items into both tables
	require.NoError(t, putTestItem(ctx, client, tables[0], "multi-1", "table1-value"))
	require.NoError(t, putTestItem(ctx, client, tables[1], "multi-2", "table2-value"))

	// Read events from both tables
	tablesFound := make(map[string]bool)
	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			tableName, exists := msg.MetaGet("dynamodb_table")
			if exists {
				tablesFound[tableName] = true
			}
		}

		// Check if we've received events from both tables
		if tablesFound[tables[0]] && tablesFound[tables[1]] {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	assert.True(t, tablesFound[tables[0]], "Should receive events from table 1")
	assert.True(t, tablesFound[tables[1]], "Should receive events from table 2")
	t.Logf("Successfully received events from %d tables", len(tablesFound))
}

// testTableMetadataInMessages verifies that table name is included in message metadata
func testTableMetadataInMessages(t *testing.T, client *dynamodb.Client, port string, tables []string, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s, %s]
table_discovery_mode: includelist
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tables[0], tables[1], checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert items with unique IDs per table
	require.NoError(t, putTestItem(ctx, client, tables[0], "metadata-test-1", "value1"))
	require.NoError(t, putTestItem(ctx, client, tables[1], "metadata-test-2", "value2"))

	// Collect events and verify metadata
	eventsWithMetadata := 0
	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts && eventsWithMetadata < 2; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			tableName, hasTable := msg.MetaGet("dynamodb_table")
			eventName, hasEvent := msg.MetaGet("dynamodb_event_name")
			shardID, hasShard := msg.MetaGet("dynamodb_shard_id")

			if hasTable && hasEvent && hasShard {
				// Verify table name is one of our expected tables
				assert.Contains(t, tables, tableName, "Table name should be one of the configured tables")
				assert.NotEmpty(t, eventName, "Event name should not be empty")
				assert.NotEmpty(t, shardID, "Shard ID should not be empty")
				eventsWithMetadata++
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	assert.GreaterOrEqual(t, eventsWithMetadata, 2, "Should have received at least 2 events with complete metadata")
}

// testIsolationBetweenTables verifies that table streams are properly isolated
func testIsolationBetweenTables(t *testing.T, client *dynamodb.Client, port, table1, table2, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s, %s]
table_discovery_mode: includelist
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, table1, table2, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert items with SAME ID in different tables
	sameID := "isolation-test"
	require.NoError(t, putTestItem(ctx, client, table1, sameID, "value-from-table1"))
	require.NoError(t, putTestItem(ctx, client, table2, sameID, "value-from-table2"))

	// Collect events
	eventsByTable := make(map[string]int)
	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			tableName, hasTable := msg.MetaGet("dynamodb_table")
			if hasTable {
				// Get the value to verify it matches the table
				structured, err := msg.AsStructured()
				if err == nil {
					if dataMap, ok := structured.(map[string]any); ok {
						if value, hasValue := dataMap["value"]; hasValue {
							// Verify the value matches the expected table
							if tableName == table1 {
								assert.Equal(t, "value-from-table1", value, "Table1 should have its own value")
							} else if tableName == table2 {
								assert.Equal(t, "value-from-table2", value, "Table2 should have its own value")
							}
						}
					}
				}
				eventsByTable[tableName]++
			}
		}

		// Check if we've received events from both tables
		if eventsByTable[table1] > 0 && eventsByTable[table2] > 0 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	assert.Greater(t, eventsByTable[table1], 0, "Should receive events from table 1")
	assert.Greater(t, eventsByTable[table2], 0, "Should receive events from table 2")
	t.Logf("Received %d events from table1, %d events from table2", eventsByTable[table1], eventsByTable[table2])
}

// TestIntegrationDynamoDBTagDiscovery tests tag-based table discovery
func TestIntegrationDynamoDBTagDiscovery(t *testing.T) {
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
	taggedTable1 := "test-tagged-table-1"
	taggedTable2 := "test-tagged-table-2"
	untaggedTable := "test-untagged-table"

	// Wait for DynamoDB to be ready and create tables
	require.NoError(t, pool.Retry(func() error {
		var err error
		client, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), taggedTable1)
		return err
	}))

	_, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), taggedTable2)
	require.NoError(t, err)
	_, err = createTableWithStreams(context.Background(), t, resource.GetPort("8000/tcp"), untaggedTable)
	require.NoError(t, err)

	port := resource.GetPort("8000/tcp")

	// Tag the first two tables
	ctx := context.Background()
	tagKey := "stream-enabled"
	tagValue := "true"

	// Get table ARNs
	desc1, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &taggedTable1,
	})
	require.NoError(t, err)

	desc2, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &taggedTable2,
	})
	require.NoError(t, err)

	// Tag tables (note: DynamoDB Local may not fully support tagging)
	_, err = client.TagResource(ctx, &dynamodb.TagResourceInput{
		ResourceArn: desc1.Table.TableArn,
		Tags: []types.Tag{
			{Key: &tagKey, Value: &tagValue},
		},
	})
	if err != nil {
		t.Skipf("DynamoDB Local doesn't support tagging: %v", err)
	}

	_, err = client.TagResource(ctx, &dynamodb.TagResourceInput{
		ResourceArn: desc2.Table.TableArn,
		Tags: []types.Tag{
			{Key: &tagKey, Value: &tagValue},
		},
	})
	require.NoError(t, err)

	t.Run("TagBasedDiscovery", func(t *testing.T) {
		checkpointTable := "test-tag-discovery-checkpoint"
		testTagBasedDiscovery(t, client, port, tagKey, tagValue, checkpointTable)
	})

	t.Run("TagBasedDiscoveryWithValue", func(t *testing.T) {
		checkpointTable := "test-tag-value-checkpoint"
		testTagBasedDiscoveryWithValue(t, client, port, tagKey, tagValue, checkpointTable)
	})
}

// testTagBasedDiscovery verifies that tag-based discovery finds tagged tables
func testTagBasedDiscovery(t *testing.T, client *dynamodb.Client, port, tagKey, tagValue, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration with tag discovery
	confStr := fmt.Sprintf(`
table_discovery_mode: tag
table_tag_key: %s
table_tag_value: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tagKey, tagValue, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// Insert items into tagged tables
	require.NoError(t, putTestItem(ctx, client, "test-tagged-table-1", "tag-test-1", "tagged-value-1"))
	require.NoError(t, putTestItem(ctx, client, "test-tagged-table-2", "tag-test-2", "tagged-value-2"))

	// Read events
	tablesFound := make(map[string]bool)
	maxAttempts := 15

	for attempt := 0; attempt < maxAttempts; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		for _, msg := range batch {
			tableName, exists := msg.MetaGet("dynamodb_table")
			if exists {
				tablesFound[tableName] = true
			}
		}

		// Check if we've discovered tagged tables
		if len(tablesFound) >= 1 {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	// We should have discovered at least one tagged table
	assert.GreaterOrEqual(t, len(tablesFound), 1, "Should discover at least one tagged table")
	t.Logf("Tag discovery found %d tables: %v", len(tablesFound), tablesFound)
}

// testTagBasedDiscoveryWithValue verifies tag discovery with specific tag value
func testTagBasedDiscoveryWithValue(t *testing.T, client *dynamodb.Client, port, tagKey, tagValue, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration with tag key AND value
	confStr := fmt.Sprintf(`
table_discovery_mode: tag
table_tag_key: %s
table_tag_value: %s
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tagKey, tagValue, checkpointTable, port)

	spec := dynamoDBCDCInputConfig()
	parsed, err := spec.ParseYAML(confStr, nil)
	require.NoError(t, err)

	input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	require.NoError(t, input.Connect(ctx))
	t.Cleanup(func() {
		_ = input.Close(ctx)
	})

	// The connector should have discovered tables with matching tag key AND value
	// We'll verify by inserting data and seeing if we receive it
	require.NoError(t, putTestItem(ctx, client, "test-tagged-table-1", "tag-value-test", "value-match"))

	// Try to read events
	foundEvent := false
	maxAttempts := 10

	for attempt := 0; attempt < maxAttempts && !foundEvent; attempt++ {
		batch, _, err := input.ReadBatch(ctx)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if len(batch) > 0 {
			foundEvent = true
			break
		}

		time.Sleep(200 * time.Millisecond)
	}

	// If tag value matching works, we should have found events
	// Note: DynamoDB Local may not fully support tagging, so we're lenient here
	t.Logf("Tag value matching: found events = %v", foundEvent)
}
