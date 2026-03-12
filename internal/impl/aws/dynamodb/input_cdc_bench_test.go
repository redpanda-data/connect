// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

var benchCounter atomic.Int64

// createBenchTable creates a DynamoDB table with streams enabled for benchmarking.
func createBenchTable(ctx context.Context, b *testing.B, dynamoPort, tableName string) *dynamodb.Client {
	b.Helper()

	endpoint := fmt.Sprintf("http://localhost:%v", dynamoPort)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(b, err)

	conf.BaseEndpoint = &endpoint
	client := dynamodb.NewFromConfig(conf)

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
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: &tableName,
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(b, err)

	waiter := dynamodb.NewTableExistsWaiter(client)
	require.NoError(b, waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	}, time.Minute))

	return client
}

func setupBenchContainer(b *testing.B) (string, func()) {
	b.Helper()
	ctx := context.Background()

	ctr, err := testcontainers.Run(ctx,
		"amazon/dynamodb-local:latest",
		testcontainers.WithExposedPorts("8000/tcp"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("8000/tcp")),
	)
	require.NoError(b, err)

	mappedPort, err := ctr.MappedPort(ctx, "8000/tcp")
	require.NoError(b, err)

	cleanup := func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			b.Logf("failed to terminate dynamodb container: %v", err)
		}
	}
	return mappedPort.Port(), cleanup
}

func bulkInsertItems(ctx context.Context, b *testing.B, client *dynamodb.Client, tableName string, count int) {
	b.Helper()
	const maxBatch = 25

	for i := 0; i < count; i += maxBatch {
		end := min(i+maxBatch, count)

		requests := make([]types.WriteRequest, 0, end-i)
		for j := i; j < end; j++ {
			requests = append(requests, types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":        &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", j)},
						"value":     &types.AttributeValueMemberS{Value: fmt.Sprintf("benchmark-payload-data-%d-padding-to-fill-space-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", j)},
						"timestamp": &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().UnixNano(), 10)},
						"index":     &types.AttributeValueMemberN{Value: strconv.Itoa(j)},
					},
				},
			})
		}

		_, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: requests,
			},
		})
		require.NoError(b, err)
	}
}

func benchName(size int) string {
	if size >= 1000 {
		return fmt.Sprintf("%dk", size/1000)
	}
	return fmt.Sprintf("%d", size)
}

func BenchmarkDynamoDBCDCThroughput(b *testing.B) {
	integration.CheckSkip(b)

	port, cleanup := setupBenchContainer(b)
	b.Cleanup(cleanup)

	ctx := context.Background()
	sizes := []int{100, 1000, 5000}

	for _, size := range sizes {
		tableName := fmt.Sprintf("bench-cdc-%d", size)
		client := createBenchTable(ctx, b, port, tableName)

		bulkInsertItems(ctx, b, client, tableName, size)
		time.Sleep(2 * time.Second)

		numItems := size
		b.Run(benchName(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				checkpointTable := fmt.Sprintf("bench-cdc-ckpt-%d", benchCounter.Add(1))

				confStr := fmt.Sprintf(`
tables: [%s]
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: trim_horizon
batch_size: 1000
poll_interval: 50ms
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

				spec := dynamoDBCDCInputConfig()
				parsed, err := spec.ParseYAML(confStr, nil)
				require.NoError(b, err)

				input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
				require.NoError(b, err)

				require.NoError(b, input.Connect(ctx))

				readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				totalEvents := 0
				emptyReads := 0
				for totalEvents < numItems && emptyReads < 15 {
					batch, ackFn, err := input.ReadBatch(readCtx)
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) {
							break
						}
						b.Fatalf("unexpected error: %v", err)
					}
					if ackFn != nil {
						_ = ackFn(ctx, nil)
					}
					if len(batch) == 0 {
						emptyReads++
						continue
					}
					emptyReads = 0
					totalEvents += len(batch)
				}
				cancel()
				_ = input.Close(ctx)
			}

			b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "events/sec")
		})
	}
}

func BenchmarkDynamoDBSnapshotThroughput(b *testing.B) {
	integration.CheckSkip(b)

	port, cleanup := setupBenchContainer(b)
	b.Cleanup(cleanup)

	ctx := context.Background()
	sizes := []int{100, 1000, 5000}

	for _, size := range sizes {
		tableName := fmt.Sprintf("bench-snap-%d", size)
		client := createBenchTable(ctx, b, port, tableName)

		bulkInsertItems(ctx, b, client, tableName, size)

		numItems := size
		b.Run(benchName(size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				checkpointTable := fmt.Sprintf("bench-snap-ckpt-%d", benchCounter.Add(1))

				confStr := fmt.Sprintf(`
tables: [%s]
checkpoint_table: %s
endpoint: http://localhost:%s
region: us-east-1
start_from: latest
snapshot_mode: snapshot_only
snapshot_segments: 1
snapshot_batch_size: 1000
snapshot_throttle: 1ms
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, tableName, checkpointTable, port)

				spec := dynamoDBCDCInputConfig()
				parsed, err := spec.ParseYAML(confStr, nil)
				require.NoError(b, err)

				input, err := newDynamoDBCDCInputFromConfig(parsed, service.MockResources())
				require.NoError(b, err)

				require.NoError(b, input.Connect(ctx))

				readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				totalEvents := 0
				for {
					batch, ackFn, err := input.ReadBatch(readCtx)
					if err != nil {
						if errors.Is(err, service.ErrEndOfInput) {
							break
						}
						if errors.Is(err, context.DeadlineExceeded) {
							break
						}
						b.Fatalf("unexpected error: %v", err)
					}
					if ackFn != nil {
						_ = ackFn(ctx, nil)
					}
					totalEvents += len(batch)
				}
				cancel()
				_ = input.Close(ctx)

				_ = totalEvents
			}

			b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "events/sec")
		})
	}
}
