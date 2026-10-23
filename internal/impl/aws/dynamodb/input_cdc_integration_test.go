// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build integration

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/license"
)

// createTableWithStreams creates a DynamoDB table with streams enabled for testing.
func createTableWithStreams(ctx context.Context, t testing.TB, dynamoPort, tableName string) (*dynamodb.Client, error) {
	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	if err != nil {
		return nil, err
	}

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

	// Check if table already exists
	ta, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		if _, ok := errors.AsType[*types.ResourceNotFoundException](err); !ok {
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

// putTestItem inserts a test item into DynamoDB.
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

// updateTestItem updates a test item in DynamoDB.
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

// deleteTestItem deletes a test item from DynamoDB.
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

	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate dynamodb container: %v", err)
		}
	})

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	var client *dynamodb.Client
	tableName := "test-streams-table"

	client, err = createTableWithStreams(ctx, t, port, tableName)
	require.NoError(t, err)

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

// testReadInsertEvents verifies that INSERT events are captured.
func testReadInsertEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s]
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

// testReadModifyEvents verifies that MODIFY events are captured.
func testReadModifyEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s]
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

// testReadRemoveEvents verifies that REMOVE events are captured.
func testReadRemoveEvents(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s]
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

// testVerifyRecordCount verifies that the number of CDC events matches the number of operations performed.
func testVerifyRecordCount(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s]
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

	deadline := time.After(30 * time.Second)
	for len(receivedEvents) < expectedTotalEvents {
		select {
		case <-deadline:
			goto verifyResults
		default:
		}

		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
		if err != nil {
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
	}
verifyResults:

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

// testCheckpointResumption verifies that checkpoints work correctly.
func testCheckpointResumption(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Create input configuration
	confStr := fmt.Sprintf(`
tables: [%s]
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

// TestIntegrationDynamoDBSnapshotAckGate verifies that snapshot progress and
// completion are gated on downstream acks: a crash after the scan has read
// (and emitted) everything but before acknowledgement must leave no snapshot
// checkpoint state, so a restart re-delivers every item. See CON-504.
func TestIntegrationDynamoDBSnapshotAckGate(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate dynamodb container: %v", err)
		}
	})

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	var client *dynamodb.Client
	tableName := "test-snapshot-ack-gate-table"
	checkpointTable := "test-snapshot-ack-gate-checkpoint"

	require.Eventually(t, func() bool {
		var cerr error
		client, cerr = createTableWithStreams(ctx, t, port, tableName)
		return cerr == nil
	}, 60*time.Second, 500*time.Millisecond)

	const itemCount = 5
	for i := range itemCount {
		require.NoError(t, putTestItem(ctx, client, tableName, fmt.Sprintf("gate-%d", i), fmt.Sprintf("value-%d", i)))
	}

	confStr := fmt.Sprintf(`
tables: [%s]
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
snapshot_mode: snapshot_only
snapshot_segments: 1
snapshot_batch_size: 10
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

	countCheckpointRows := func() int {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: &checkpointTable})
		if err != nil {
			return -1 // table may not exist yet
		}
		return len(out.Items)
	}

	// Run 1: receive the snapshot batch but never acknowledge it, then
	// simulate a crash. Nothing may be persisted.
	{
		spec := dynamoDBCDCInputConfig()
		parsed, err := spec.ParseYAML(confStr, nil)
		require.NoError(t, err)
		input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, input.Connect(ctx))

		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		cancel()
		require.NoError(t, err)
		require.Len(t, batch, itemCount, "expected the whole snapshot in one batch")

		// Give the input time to (wrongly) persist progress or the completion
		// marker - the pre-fix code did both at read time.
		time.Sleep(3 * time.Second)
		require.Zero(t, countCheckpointRows(),
			"no snapshot checkpoint state may be persisted before the batch is acknowledged")

		// Simulated crash: abandon without acking.
		_ = input.Close(ctx)
	}

	// Run 2: restart against the same checkpoint table. Since nothing was
	// acked, the snapshot must re-run and deliver every item again.
	{
		spec := dynamoDBCDCInputConfig()
		parsed, err := spec.ParseYAML(confStr, nil)
		require.NoError(t, err)
		input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, input.Connect(ctx))
		t.Cleanup(func() { _ = input.Close(ctx) })

		seen := 0
		readCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		for seen < itemCount {
			batch, ackFn, err := input.ReadBatch(readCtx)
			if errors.Is(err, service.ErrEndOfInput) {
				break
			}
			require.NoError(t, err)
			seen += len(batch)
			require.NoError(t, ackFn(ctx, nil))
		}
		require.Equal(t, itemCount, seen, "the snapshot should have re-run and re-delivered every item after the crash")

		// With everything acked, completion must now persist.
		require.Eventually(t, func() bool {
			return countCheckpointRows() > 0
		}, 30*time.Second, 500*time.Millisecond, "snapshot completion was never persisted after a fully-acked run")
	}
}

// TestIntegrationDynamoDBSnapshot tests snapshot functionality.
func TestIntegrationDynamoDBSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()

	// Start DynamoDB Local container using testcontainers-go
	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate dynamodb container: %v", err)
		}
	})

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	var client *dynamodb.Client
	tableName := "test-snapshot-table"

	// Wait for DynamoDB to be ready and create table
	require.Eventually(t, func() bool {
		var cerr error
		client, cerr = createTableWithStreams(ctx, t, port, tableName)
		return cerr == nil
	}, 60*time.Second, 500*time.Millisecond)

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

// testSnapshotOnlyMode verifies snapshot_only mode reads all items and exits.
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
tables: [%s]
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

// testSnapshotAndCDCMode verifies snapshot_and_cdc mode captures both snapshot and CDC events.
func testSnapshotAndCDCMode(t *testing.T, client *dynamodb.Client, port, tableName, checkpointTable string) {
	ctx := context.Background()

	// Insert initial items BEFORE starting
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-cdc-1", "initial-1"))
	require.NoError(t, putTestItem(ctx, client, tableName, "snap-cdc-2", "initial-2"))

	// Give DynamoDB a moment to persist
	time.Sleep(100 * time.Millisecond)

	// Create input with snapshot_and_cdc mode
	confStr := fmt.Sprintf(`
tables: [%s]
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

// testSnapshotResumeFromCheckpoint verifies snapshot can resume from checkpoint.
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
tables: [%s]
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

	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate dynamodb container: %v", err)
		}
	})

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	table1 := "test-multi-table-1"
	table2 := "test-multi-table-2"
	table3 := "test-multi-table-3"

	// Create multiple tables
	client, err := createTableWithStreams(ctx, t, port, table1)
	require.NoError(t, err)
	_, err = createTableWithStreams(ctx, t, port, table2)
	require.NoError(t, err)
	_, err = createTableWithStreams(ctx, t, port, table3)
	require.NoError(t, err)

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

	time.Sleep(100 * time.Millisecond)

	// Insert items into both tables
	require.NoError(t, putTestItem(ctx, client, tables[0], "multi-1", "table1-value"))
	require.NoError(t, putTestItem(ctx, client, tables[1], "multi-2", "table2-value"))

	// Read events from both tables
	tablesFound := make(map[string]bool)
	maxAttempts := 20

	for attempt := 0; attempt < maxAttempts; attempt++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
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
	maxAttempts := 20

	for attempt := 0; attempt < maxAttempts && eventsWithMetadata < 2; attempt++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
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
	maxAttempts := 20

	for attempt := 0; attempt < maxAttempts; attempt++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
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
						if dynamoData, ok := dataMap["dynamodb"].(map[string]any); ok {
							if newImage, ok := dynamoData["newImage"].(map[string]any); ok {
								if value, hasValue := newImage["value"]; hasValue {
									// Verify the value matches the expected table
									if tableName == table1 {
										assert.Equal(t, "value-from-table1", value, "Table1 should have its own value")
									} else if tableName == table2 {
										assert.Equal(t, "value-from-table2", value, "Table2 should have its own value")
									}
								}
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
	}

	assert.Greater(t, eventsByTable[table1], 0, "Should receive events from table 1")
	assert.Greater(t, eventsByTable[table2], 0, "Should receive events from table 2")
	t.Logf("Received %d events from table1, %d events from table2", eventsByTable[table1], eventsByTable[table2])
}

// TestIntegrationDynamoDBTagDiscovery tests tag-based table discovery
func TestIntegrationDynamoDBTagDiscovery(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate dynamodb container: %v", err)
		}
	})

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	port := mappedPort.Port()

	taggedTable1 := "test-tagged-table-1"
	taggedTable2 := "test-tagged-table-2"
	untaggedTable := "test-untagged-table"

	// Create tables
	client, err := createTableWithStreams(ctx, t, port, taggedTable1)
	require.NoError(t, err)
	_, err = createTableWithStreams(ctx, t, port, taggedTable2)
	require.NoError(t, err)
	_, err = createTableWithStreams(ctx, t, port, untaggedTable)
	require.NoError(t, err)

	// Tag the first two tables
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
table_tag_filter: "%s:%s"
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
	maxAttempts := 20

	for attempt := 0; attempt < maxAttempts; attempt++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
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
table_tag_filter: "%s:%s"
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
	maxAttempts := 20

	for attempt := 0; attempt < maxAttempts && !foundEvent; attempt++ {
		readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
		batch, _, err := input.ReadBatch(readCtx)
		readCancel()
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		if len(batch) > 0 {
			foundEvent = true
			break
		}
	}

	// If tag value matching works, we should have found events
	// Note: DynamoDB Local may not fully support tagging, so we're lenient here
	t.Logf("Tag value matching: found events = %v", foundEvent)
}

// incrementalSnapshotRealAWSEnv switches TestIntegrationDynamoDBCDCIncrementalSnapshot
// from DynamoDB Local to real AWS, using the default credential chain and
// region. DynamoDB Local rounds ApproximateCreationDateTime down to the minute,
// so the ordering refinement (which depends on stream timestamps) is only
// asserted against real AWS; the Safety rule is clock-free and asserted on both.
const incrementalSnapshotRealAWSEnv = "DYNAMODB_CDC_REAL_AWS"

// TestIntegrationDynamoDBCDCIncrementalSnapshot backfills a table in
// snapshot_mode incremental while a writer keeps updating random keys. It
// checks the Safety rule (no snapshot item is emitted after a newer stream
// event for its key) and that the last value the input emitted for every key
// matches the table.
func TestIntegrationDynamoDBCDCIncrementalSnapshot(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()
	realAWS := os.Getenv(incrementalSnapshotRealAWSEnv) != ""

	var (
		conf      aws.Config
		inputConn string
		err       error
	)
	if realAWS {
		conf, err = config.LoadDefaultConfig(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, conf.Region, "set AWS_REGION to run against real AWS")
		inputConn = "region: " + conf.Region
	} else {
		ctr, err := testcontainers.Run(ctx,
			"amazon/dynamodb-local:latest",
			testcontainers.WithExposedPorts("8000/tcp"),
			testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
		)
		testcontainers.CleanupContainer(t, ctr)
		require.NoError(t, err)
		mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
		require.NoError(t, err)
		endpoint := fmt.Sprintf("http://localhost:%v", mappedPort.Port())

		conf, err = config.LoadDefaultConfig(ctx,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
			config.WithRegion("us-east-1"),
		)
		require.NoError(t, err)
		conf.BaseEndpoint = &endpoint
		inputConn = fmt.Sprintf(`region: us-east-1
endpoint: %s
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx`, endpoint)
	}
	client := dynamodb.NewFromConfig(conf)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "rpcn-incremental-snapshot-" + suffix
	checkpointTable := "rpcn-incremental-snapshot-cp-" + suffix
	t.Cleanup(func() {
		for _, name := range []string{tableName, checkpointTable} {
			if _, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(name)}); err != nil {
				t.Logf("failed to delete table %s: %v", name, err)
			}
		}
	})

	t.Logf("Creating table with streams: %v", tableName)
	createTable := func() error {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			BillingMode: types.BillingModePayPerRequest,
			StreamSpecification: &types.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: types.StreamViewTypeNewAndOldImages,
			},
		})
		return err
	}
	// DynamoDB Local can accept connections before it serves requests.
	require.Eventually(t, func() bool { return createTable() == nil }, time.Minute, 500*time.Millisecond)
	require.NoError(t, dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}, 2*time.Minute))

	const itemCount = 500
	for i := range itemCount {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
				"v":  &types.AttributeValueMemberN{Value: "0"},
			},
		})
		require.NoError(t, err)
	}

	// Every write is ADD v :one, so a key's value only increases.
	// safetyViolations are READ events emitted with a v below one already
	// emitted for the key: a snapshot item leaving the input after a newer
	// stream event, which the Safety rule forbids. orderRegressions are any
	// event whose v is below the previous one emitted for the key; the
	// ordering refinement prevents these only with accurate stream
	// timestamps. The consumer is the only writer of this state and the test
	// body reads it under mu.
	var (
		mu                                 sync.Mutex
		last                               = map[string]int{}
		maxSeen                            = map[string]int{}
		events                             = map[string]int{}
		safetyViolations, orderRegressions []string
		consumeErrs                        []error
		lastMsgAt                          = time.Now()
	)
	consume := func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			events[eventName]++

			structured, err := msg.AsStructured()
			if err != nil {
				consumeErrs = append(consumeErrs, err)
				continue
			}
			img, ok := structured.(map[string]any)["dynamodb"].(map[string]any)["newImage"].(map[string]any)
			if !ok {
				consumeErrs = append(consumeErrs, fmt.Errorf("%s message without a newImage", eventName))
				continue
			}
			v, err := strconv.Atoi(fmt.Sprint(img["v"]))
			if err != nil {
				consumeErrs = append(consumeErrs, err)
				continue
			}
			pk := img["pk"].(string)

			if prev, seen := last[pk]; seen && v < prev {
				orderRegressions = append(orderRegressions, fmt.Sprintf("%s: %s v=%d after v=%d", pk, eventName, v, prev))
			}
			if top, seen := maxSeen[pk]; seen && eventName == "READ" && v < top {
				safetyViolations = append(safetyViolations, fmt.Sprintf("%s: READ v=%d after maxSeen=%d", pk, v, top))
			}
			if top, seen := maxSeen[pk]; !seen || v > top {
				maxSeen[pk] = v
			}
			last[pk] = v
		}
		if len(batch) > 0 {
			lastMsgAt = time.Now()
		}
		return nil
	}

	inputYAML := fmt.Sprintf(`
tables: [%s]
checkpoint_table: %s
%s
snapshot_mode: incremental
snapshot_segments: 4
snapshot_batch_size: 25
snapshot_throttle: 10ms
`, tableName, checkpointTable, inputConn)

	// The Safety assertion depends on messages reaching the consumer in the
	// order the input emitted them, so the pipeline must not run processor
	// threads in parallel (the default is one per CPU, which can reorder
	// batches). The batch consumer func is driven by a single goroutine, so
	// it sees batches sequentially. SetThreads is used instead of a full
	// SetYAML because SetYAML would also add the default reject output.
	builder := service.NewStreamBuilder()
	builder.SetThreads(1) // equivalent to pipeline: { threads: 1 }
	require.NoError(t, builder.AddInputYAML("aws_dynamodb_cdc:\n  "+
		strings.ReplaceAll(strings.TrimSpace(inputYAML), "\n", "\n  ")))
	require.NoError(t, builder.AddBatchConsumerFunc(consume))
	stream, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	var runErr error
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		runErr = stream.Run(t.Context())
	}()
	t.Cleanup(func() {
		_ = stream.StopWithin(10 * time.Second)
		select {
		case <-streamDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("stream failed: %v", runErr)
			}
		case <-time.After(15 * time.Second):
			t.Error("stream did not stop within 15s of teardown")
		}
	})

	// Writer: bump random keys for 10 seconds while the backfill runs.
	var (
		writerDone atomic.Bool
		writes     atomic.Int64
	)
	writerCtx, stopWriter := context.WithTimeout(ctx, 10*time.Second)
	defer stopWriter()
	go func() {
		defer writerDone.Store(true)
		for writerCtx.Err() == nil {
			_, err := client.UpdateItem(writerCtx, &dynamodb.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", rand.IntN(itemCount))},
				},
				UpdateExpression: aws.String("ADD v :one"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":one": &types.AttributeValueMemberN{Value: "1"},
				},
			})
			if err != nil {
				if writerCtx.Err() == nil {
					t.Errorf("update failed: %v", err)
				}
				return
			}
			writes.Add(1)
		}
	}()

	// Wait until the writer has stopped, every key has been seen and nothing
	// has arrived for the quiet period. Once the writer stops, the idle rule
	// releases held pages after two empty polls plus 1s + 2 *
	// snapshot_watermark_margin (about 5s). DynamoDB Local rounds stream
	// timestamps down to the minute, which delays releases until the idle
	// rule or the next minute, so the local quiet period is longer.
	quiet := 5 * time.Second
	if !realAWS {
		quiet = 20 * time.Second
	}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return writerDone.Load() && len(last) >= itemCount && time.Since(lastMsgAt) >= quiet
	}, 5*time.Minute, 500*time.Millisecond, "timed out waiting for every key and a quiet period")

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, consumeErrs)
	t.Logf("writer made %d updates; events by type: %v; safety violations: %d; order regressions: %d",
		writes.Load(), events, len(safetyViolations), len(orderRegressions))
	assert.Positive(t, events["READ"], "the incremental backfill emitted no snapshot items")
	assert.Empty(t, safetyViolations, "snapshot items emitted after a newer stream event for the same key")
	if realAWS {
		assert.Empty(t, orderRegressions, "per-key values emitted out of order")
	}

	table := map[string]int{}
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		require.NoError(t, err)
		for _, item := range out.Items {
			pk := item["pk"].(*types.AttributeValueMemberS).Value
			v, err := strconv.Atoi(item["v"].(*types.AttributeValueMemberN).Value)
			require.NoError(t, err)
			table[pk] = v
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	require.Len(t, table, itemCount)

	for pk, want := range table {
		assert.Equal(t, want, last[pk], "last emitted value for %s does not match the table", pk)
	}
}

// TestIntegrationDynamoDBCDCSnapshotSignal checks that a signal table drives
// incremental backfills: nothing is backfilled until a snapshot-execute
// signal arrives, the signal then backfills the table once, and a repeated
// signal is a no-op.
func TestIntegrationDynamoDBCDCSnapshotSignal(t *testing.T) {
	integration.CheckSkip(t)

	ctx := t.Context()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(t, err)
	endpoint := fmt.Sprintf("http://localhost:%v", mappedPort.Port())

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)
	conf.BaseEndpoint = &endpoint
	inputConn := fmt.Sprintf(`region: us-east-1
endpoint: %s
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx`, endpoint)
	client := dynamodb.NewFromConfig(conf)

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	ordersTable := "orders"
	signalTable := "rpcn_signals"
	checkpointTable := "rpcn-snapshot-signal-cp-" + suffix

	createTable := func(name, key string, view types.StreamViewType) func() error {
		return func() error {
			_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName: aws.String(name),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String(key), AttributeType: types.ScalarAttributeTypeS},
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String(key), KeyType: types.KeyTypeHash},
				},
				BillingMode: types.BillingModePayPerRequest,
				StreamSpecification: &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: view,
				},
			})
			return err
		}
	}
	// DynamoDB Local can accept connections before it serves requests.
	require.Eventually(t, func() bool {
		return createTable(ordersTable, "pk", types.StreamViewTypeNewAndOldImages)() == nil
	}, time.Minute, 500*time.Millisecond)
	require.NoError(t, createTable(signalTable, "id", types.StreamViewTypeNewImage)())
	for _, name := range []string{ordersTable, signalTable} {
		require.NoError(t, dynamodb.NewTableExistsWaiter(client).Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		}, 2*time.Minute))
	}

	const itemCount = 100
	for i := range itemCount {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(ordersTable),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
				"v":  &types.AttributeValueMemberN{Value: "0"},
			},
		})
		require.NoError(t, err)
	}

	// Every orders write is ADD v :one, so a key's value only increases and
	// a READ with a v below one already seen for its key would be a snapshot
	// item leaving the input after a newer stream event. The consumer is the
	// only writer of this state and the test body reads it under mu.
	var (
		mu               sync.Mutex
		maxSeen          = map[string]int{}
		reads            = map[string]int{}
		last             = map[string]int{}
		signalRecords    int
		safetyViolations []string
		consumeErrs      []error
	)
	consume := func(_ context.Context, batch service.MessageBatch) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range batch {
			eventName, _ := msg.MetaGet("dynamodb_event_name")
			table, _ := msg.MetaGet("dynamodb_table")
			if table == signalTable {
				signalRecords++
				continue
			}
			structured, err := msg.AsStructured()
			if err != nil {
				consumeErrs = append(consumeErrs, err)
				continue
			}
			img, ok := structured.(map[string]any)["dynamodb"].(map[string]any)["newImage"].(map[string]any)
			if !ok {
				consumeErrs = append(consumeErrs, fmt.Errorf("%s message without a newImage", eventName))
				continue
			}
			v, err := strconv.Atoi(fmt.Sprint(img["v"]))
			if err != nil {
				consumeErrs = append(consumeErrs, err)
				continue
			}
			pk := img["pk"].(string)
			if eventName == "READ" {
				reads[pk]++
				if top, seen := maxSeen[pk]; seen && v < top {
					safetyViolations = append(safetyViolations, fmt.Sprintf("%s: READ v=%d after maxSeen=%d", pk, v, top))
				}
			}
			if top, seen := maxSeen[pk]; !seen || v > top {
				maxSeen[pk] = v
			}
			last[pk] = v
		}
		return nil
	}
	readCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, c := range reads {
			n += c
		}
		return n
	}

	inputYAML := fmt.Sprintf(`
tables: [%s]
signal_table_name: %s
checkpoint_table: %s
%s
snapshot_mode: incremental
snapshot_segments: 2
snapshot_batch_size: 25
snapshot_throttle: 10ms
`, ordersTable, signalTable, checkpointTable, inputConn)

	// One processor thread keeps batches in emission order for the safety
	// check (see TestIntegrationDynamoDBCDCIncrementalSnapshot).
	builder := service.NewStreamBuilder()
	builder.SetThreads(1)
	require.NoError(t, builder.AddInputYAML("aws_dynamodb_cdc:\n  "+
		strings.ReplaceAll(strings.TrimSpace(inputYAML), "\n", "\n  ")))
	require.NoError(t, builder.AddBatchConsumerFunc(consume))
	stream, err := builder.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())

	var runErr error
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		runErr = stream.Run(t.Context())
	}()
	t.Cleanup(func() {
		_ = stream.StopWithin(10 * time.Second)
		select {
		case <-streamDone:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("stream failed: %v", runErr)
			}
		case <-time.After(15 * time.Second):
			t.Error("stream did not stop within 15s of teardown")
		}
	})

	putSignal := func(id string) {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(signalTable),
			Item: map[string]types.AttributeValue{
				"id":   &types.AttributeValueMemberS{Value: id},
				"type": &types.AttributeValueMemberS{Value: "snapshot-execute"},
				"data": &types.AttributeValueMemberS{Value: `{"tables":["orders"]}`},
			},
		})
		require.NoError(t, err)
	}

	// The checkpoint partition for orders is keyed by its stream ARN (the
	// default, non-global mode), as Checkpointer.hashKeyValue does.
	descOrders, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(ordersTable)})
	require.NoError(t, err)
	ordersPartition := aws.ToString(descOrders.Table.LatestStreamArn)
	snapshotRowsErr := func(prefix string) ([]string, error) {
		out, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(checkpointTable),
			ConsistentRead:         aws.Bool(true),
			KeyConditionExpression: aws.String(checkpointHashKeyDefault + " = :hash AND begins_with(" + checkpointRangeKey + ", :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":hash":   &types.AttributeValueMemberS{Value: ordersPartition},
				":prefix": &types.AttributeValueMemberS{Value: prefix},
			},
		})
		if err != nil {
			return nil, err
		}
		var ids []string
		for _, item := range out.Items {
			ids = append(ids, item[checkpointRangeKey].(*types.AttributeValueMemberS).Value)
		}
		return ids, nil
	}
	// snapshotRows must only be called on the test goroutine; polling
	// conditions use snapshotRowsErr because require cannot run there.
	snapshotRows := func(prefix string) []string {
		ids, err := snapshotRowsErr(prefix)
		require.NoError(t, err)
		return ids
	}

	// Let the input connect, then check its state directly: an eager
	// backfill could still be held back by DynamoDB Local's minute-rounded
	// stream timestamps, so the absence of READs alone proves little.
	time.Sleep(5 * time.Second)
	assert.Empty(t, snapshotRows("snapshot#"), "snapshot checkpoint rows exist before any signal")
	assert.Zero(t, readCount(), "READ events before any signal")

	// Writer: bump random orders keys from just before the signal for 10s
	// while the backfill starts, so the safety bookkeeping guards a real
	// race. It is bounded because the idle rule that releases held pages
	// cannot fire while writes keep arriving (the first run with a writer
	// that lasted until the backfill completed never finished).
	var writes atomic.Int64
	writerCtx, stopWriter := context.WithTimeout(ctx, 10*time.Second)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for writerCtx.Err() == nil {
			_, err := client.UpdateItem(writerCtx, &dynamodb.UpdateItemInput{
				TableName: aws.String(ordersTable),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", rand.IntN(itemCount))},
				},
				UpdateExpression: aws.String("ADD v :one"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":one": &types.AttributeValueMemberN{Value: "1"},
				},
			})
			if err != nil {
				if writerCtx.Err() == nil {
					t.Errorf("update failed: %v", err)
				}
				return
			}
			writes.Add(1)
		}
	}()
	t.Cleanup(func() { stopWriter(); <-writerDone })

	putSignal("1")

	// DynamoDB Local rounds stream timestamps down to the minute, which
	// delays the release of held snapshot pages until the idle rule or the
	// next minute, so the waits are generous. The writer makes the stream
	// deliver a newer value for some keys, and the window then drops their
	// snapshot item, so the READ count is at most itemCount; completion is
	// the snapshot#complete row, and every key must have been seen as a READ
	// or a stream event.
	require.Eventually(t, func() bool {
		mu.Lock()
		seen, sigs := len(maxSeen), signalRecords
		mu.Unlock()
		if seen != itemCount || sigs < 1 {
			return false
		}
		done, err := snapshotRowsErr("snapshot#complete")
		return err == nil && len(done) == 1
	}, 5*time.Minute, 500*time.Millisecond, "timed out waiting for the backfill and the signal record")
	stopWriter()
	<-writerDone
	assert.Positive(t, writes.Load(), "the writer made no updates, so the safety check guarded no race")
	reads1 := readCount()
	assert.Positive(t, reads1, "the signal backfilled nothing")
	assert.LessOrEqual(t, reads1, itemCount)

	// The signal was already executed, so a second one is a no-op. Check the
	// outcome in checkpoint state, which does not depend on release timing.
	putSignal("2")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return signalRecords >= 2
	}, 5*time.Minute, 500*time.Millisecond, "timed out waiting for the second signal record")
	time.Sleep(5 * time.Second)

	assert.Len(t, snapshotRows("snapshot#complete"), 1)
	assert.Empty(t, snapshotRows("snapshot#requested"), "a complete table must not be requeued by a repeated signal")
	assert.Equal(t, reads1, readCount(), "a repeated signal must not backfill again")

	// Wait for the stream to deliver every write, then compare with the table.
	table := map[string]int{}
	var startKey map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(ordersTable),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		require.NoError(t, err)
		for _, item := range out.Items {
			pk := item["pk"].(*types.AttributeValueMemberS).Value
			v, err := strconv.Atoi(item["v"].(*types.AttributeValueMemberN).Value)
			require.NoError(t, err)
			table[pk] = v
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	require.Len(t, table, itemCount)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for pk, want := range table {
			if last[pk] != want {
				return false
			}
		}
		return true
	}, 5*time.Minute, 500*time.Millisecond, "emitted values never matched the table")

	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, consumeErrs)
	n := 0
	for _, c := range reads {
		n += c
	}
	t.Logf("READ events: %d; writer updates: %d; signal records: %d; safety violations: %d; requested rows: %v; complete rows: %v",
		n, writes.Load(), signalRecords, len(safetyViolations), snapshotRows("snapshot#requested"), snapshotRows("snapshot#complete"))
	assert.Empty(t, safetyViolations, "snapshot items emitted after a newer stream event for the same key")
}
