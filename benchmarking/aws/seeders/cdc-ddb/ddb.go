// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
)

// dynamoBatchMax is DynamoDB's hard cap on items per BatchWriteItem call.
const dynamoBatchMax = 25

func newClient(ctx context.Context) (*dynamodb.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(os.Getenv("AWS_REGION")))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return dynamodb.NewFromConfig(cfg), nil
}

// seed bulk-loads initial rows. Scenarios that reset via drop+recreate set
// rows=0, making this a no-op. Kept for parity with the cdc-rows seeder and in
// case a future scenario wants pre-populated state.
func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	if rows == 0 {
		return nil
	}
	cli, err := newClient(ctx)
	if err != nil {
		return err
	}
	for _, table := range tables {
		if err := bulkWrite(ctx, cli, table, rows, rowSize); err != nil {
			return err
		}
	}
	return nil
}

func bulkWrite(ctx context.Context, cli *dynamodb.Client, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var written int64
			for written < rowsPerWorker {
				batch := buildBatch(table, dynamoBatchMax, rowSize)
				if err := writeBatch(ctx, cli, batch); err != nil {
					errCh <- err
					return
				}
				written += int64(len(batch[table]))
			}
			errCh <- nil
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("seeded %d rows into %s in %s\n", rows, table, time.Since(start))
	return nil
}

// workload drives sustained PutItem load via parallel BatchWriteItem workers.
// 16 workers handles 5000 PutItems/sec comfortably (each worker ~312/sec, well
// under the per-worker network ceiling). Bump the rate at the scenario layer
// when the connector is the bottleneck.
func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration) error {
	const workers = 16
	cli, err := newClient(ctx)
	if err != nil {
		return err
	}

	// Per-worker items per 100ms tick. Round up so the actual rate >= target.
	perWorkerPerTick := (rate/workers + 9) / 10
	if perWorkerPerTick < 1 {
		perWorkerPerTick = 1
	}
	if perWorkerPerTick > dynamoBatchMax {
		perWorkerPerTick = dynamoBatchMax
	}

	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := w
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			tIdx := workerIdx
			for {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						errCh <- nil
						return
					}
					table := tables[tIdx%len(tables)]
					tIdx++
					batch := buildBatch(table, perWorkerPerTick, rowSize)
					if err := writeBatch(ctx, cli, batch); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			return err
		}
	}
	return nil
}

// buildBatch returns a single-table BatchWriteItem RequestItems map with n
// PutRequests. UUID hash keys spread writes across DDB partitions so the
// bench probes connector throughput rather than per-partition WCU contention.
func buildBatch(table string, n, rowSize int) map[string][]ddbtypes.WriteRequest {
	reqs := make([]ddbtypes.WriteRequest, n)
	for i := 0; i < n; i++ {
		reqs[i] = ddbtypes.WriteRequest{
			PutRequest: &ddbtypes.PutRequest{
				Item: map[string]ddbtypes.AttributeValue{
					"id":      &ddbtypes.AttributeValueMemberS{Value: uuid.NewString()},
					"payload": &ddbtypes.AttributeValueMemberS{Value: randomPayload(rowSize)},
				},
			},
		}
	}
	return map[string][]ddbtypes.WriteRequest{table: reqs}
}

// writeBatch issues a BatchWriteItem with retry-on-unprocessed. DDB returns
// unprocessed items when the table hits its provisioned write throughput; we
// re-submit them rather than letting the worker stall.
func writeBatch(ctx context.Context, cli *dynamodb.Client, batch map[string][]ddbtypes.WriteRequest) error {
	for attempt := 0; attempt < 5 && len(batch) > 0; attempt++ {
		out, err := cli.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems:           batch,
			ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityNone,
		})
		if err != nil {
			return err
		}
		if len(out.UnprocessedItems) == 0 {
			return nil
		}
		batch = out.UnprocessedItems
		// Exponential-ish backoff: 50ms, 100ms, 200ms, 400ms.
		time.Sleep(time.Duration(50*(1<<attempt)) * time.Millisecond)
	}
	if len(batch) > 0 {
		return fmt.Errorf("batch write: %d items unprocessed after retries", len(batch))
	}
	return nil
}

func randomPayload(size int) string {
	b := make([]byte, (size*3)/4+1)
	_, _ = rand.Read(b)
	s := base64.StdEncoding.EncodeToString(b)
	if len(s) > size {
		s = s[:size]
	}
	return s
}
