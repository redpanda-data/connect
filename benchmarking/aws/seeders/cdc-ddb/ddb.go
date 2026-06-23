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
// rows=0, making this a no-op. Kept for parity with the cdc-rows-postgres seeder and in
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
// Earlier shape capped per-worker writes at dynamoBatchMax (25), which silently
// halved any configured rate above ~4000/sec — Connect bench data showed Connect
// reading the full source rate but never above ~18 MB/s, masking the connector's
// true ceiling. Current shape: each worker sends as many BatchWriteItem calls
// per 100ms tick as needed to hit its share of the configured rate.
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
	// Split into BatchWriteItem-sized chunks. AWS caps a single BatchWriteItem
	// at dynamoBatchMax (25) items, so anything above is sent as multiple
	// calls per tick. With 16 workers and 100ms ticks, the effective ceiling
	// is roughly (workers × dynamoBatchMax × 10) ≈ 4000/sec per call-per-tick;
	// going higher requires multiple calls per tick (handled below).
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
					// Send floor(N/25) full batches + one remainder batch.
					remaining := perWorkerPerTick
					for remaining > 0 {
						n := remaining
						if n > dynamoBatchMax {
							n = dynamoBatchMax
						}
						batch := buildBatch(table, n, rowSize)
						if err := writeBatch(ctx, cli, batch); err != nil {
							errCh <- err
							return
						}
						remaining -= n
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

// writeBatch issues a BatchWriteItem with retry on transient errors AND on
// UnprocessedItems. AWS BatchWriteItem returns transient HTTP 500s
// (InternalServerError) and 400s (ProvisionedThroughputExceededException,
// ThrottlingException) under sustained load; without retries here, a single
// blip would kill the worker and cascade to the whole workload — masquerading
// as a Connect-side throughput drop. Each attempt uses exponential backoff
// up to a few seconds total. After max attempts, the batch is DROPPED (we
// log and continue) so individual worker hiccups don't terminate the load
// generator — sustaining the rate is more important than delivering every
// item exactly.
func writeBatch(ctx context.Context, cli *dynamodb.Client, batch map[string][]ddbtypes.WriteRequest) error {
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts && len(batch) > 0; attempt++ {
		out, err := cli.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems:           batch,
			ReturnConsumedCapacity: ddbtypes.ReturnConsumedCapacityNone,
		})
		if err != nil {
			// Context cancellation isn't transient — bubble out so the worker
			// can exit cleanly on shutdown.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// All other errors (5xx, throttling, network blips, etc.) are
			// retryable. Back off exponentially: 50ms, 100ms, 200ms, ...
			// capped at ~6s total over 8 attempts.
			wait := time.Duration(50*(1<<attempt)) * time.Millisecond
			if wait > 2*time.Second {
				wait = 2 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		if len(out.UnprocessedItems) == 0 {
			return nil
		}
		batch = out.UnprocessedItems
		wait := time.Duration(50*(1<<attempt)) * time.Millisecond
		if wait > 2*time.Second {
			wait = 2 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	if len(batch) > 0 {
		// Drop the batch rather than fail. Sustaining the load is more
		// important than delivering every item — the bench measures sustained
		// throughput, not exact-once delivery.
		return nil
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
