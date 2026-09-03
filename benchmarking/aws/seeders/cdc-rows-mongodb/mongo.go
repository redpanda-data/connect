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
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// clientReadyTimeout / clientRetryDelay bound how long openClient waits for the
// self-hosted mongod to become reachable with an elected primary.
//
// The mongod host finishes cloud-init (package install + rs.initiate) a few
// minutes AFTER terraform reports the EC2 instance "running", but the seed
// phase starts as soon as `terraform apply` returns — so the first connection
// races cloud-init and sees "connection refused" (mongod not listening yet) or
// ReplicaSetNoPrimary (rs.initiate not settled). Retry until it is ready. The
// RDS-backed seeders (postgres/mysql/oracle) don't need this because RDS is
// already "available" when apply returns.
const (
	clientReadyTimeout = 8 * time.Minute
	clientRetryDelay   = 5 * time.Second
)

// openClient dials the MongoDB replica set. mongo.Connect in the v2 driver takes
// options only (no context); readiness is verified with a Ping, retried until
// mongod is up (see clientReadyTimeout).
func openClient(ctx context.Context, dsn string) (*mongo.Client, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(dsn).SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(clientReadyTimeout)
	var lastErr error
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		lastErr = client.Ping(pingCtx, nil)
		cancel()
		if lastErr == nil {
			return client, nil
		}
		if time.Now().After(deadline) {
			_ = client.Disconnect(ctx)
			return nil, fmt.Errorf("mongodb not ready after %s: %w", clientReadyTimeout, lastErr)
		}
		select {
		case <-ctx.Done():
			_ = client.Disconnect(ctx)
			return nil, ctx.Err()
		case <-time.After(clientRetryDelay):
		}
	}
}

// execDrop opens a short-lived client with an explicit DSN and truncates the
// named collection by dropping then recreating it. Used by the bench reset via
// the `exec` subcommand. The recreate matters: reset runs BEFORE the engine
// starts each sweep point, and a collection-level change stream opened on a
// missing collection is fragile across server/driver versions — recreating
// leaves an empty collection in place so mongodb_cdc / Debezium can watch it.
// A drop (rather than deleteMany) avoids emitting delete change events that
// would pollute the insert-only workload's throughput measurement.
func execDrop(ctx context.Context, dsn, database, collection string) error {
	if dsn == "" || collection == "" {
		return fmt.Errorf("exec requires both --dsn and --drop-collection")
	}
	client, err := openClient(ctx, dsn)
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	if err := ensureCollection(ctx, client.Database(database), collection); err != nil {
		return err
	}
	return nil
}

func seed(ctx context.Context, database string, tables []string, rows int64, rowSize int) error {
	client, err := openClient(ctx, os.Getenv("MONGODB_DSN"))
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	db := client.Database(database)

	for _, table := range tables {
		if err := ensureCollection(ctx, db, table); err != nil {
			return err
		}
	}
	if rows <= 0 {
		// Allow rows=0 (scenario.dataset.initial_rows: 0): ensureCollection already
		// ran so the collection exists but stays empty.
		return nil
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			errCh <- bulkInsert(ctx, db, t, rows, rowSize)
		}(table)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// ensureCollection drops the collection then recreates it. CreateCollection
// racing an existing namespace ("already exists") is tolerated.
func ensureCollection(ctx context.Context, db *mongo.Database, collection string) error {
	coll := db.Collection(collection)
	if err := coll.Drop(ctx); err != nil {
		return fmt.Errorf("drop collection %s: %w", collection, err)
	}
	if err := db.CreateCollection(ctx, collection); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
			return fmt.Errorf("create collection %s: %w", collection, err)
		}
	}
	return nil
}

func bulkInsert(ctx context.Context, db *mongo.Database, collection string, rows int64, rowSize int) error {
	const (
		workers   = 16
		batchSize = 1000
	)
	rowsPerWorker := rows / workers
	if rowsPerWorker == 0 {
		return nil
	}
	coll := db.Collection(collection)
	start := time.Now()

	// Distinct payloads (built once, not per doc) so the change events aren't
	// trivially compressible — see payloadPoolSize.
	pool := randomPayloadPool(rowSize, payloadPoolSize)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := 0
			done := int64(0)
			for done < rowsPerWorker {
				n := int64(batchSize)
				if rem := rowsPerWorker - done; rem < n {
					n = rem
				}
				docs := make([]any, n)
				for i := range docs {
					docs[i] = newDocument(pool[p%len(pool)])
					p++
				}
				if _, err := coll.InsertMany(ctx, docs); err != nil {
					errCh <- err
					return
				}
				done += n
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
	fmt.Printf("seeded %d documents into %s in %s\n", rows, collection, time.Since(start))
	return nil
}

func workload(ctx context.Context, database string, tables []string, rowSize, rate int, dur time.Duration) error {
	const workers = 16
	client, err := openClient(ctx, os.Getenv("MONGODB_DSN"))
	if err != nil {
		return err
	}
	defer client.Disconnect(ctx)
	db := client.Database(database)

	perWorkerPer100ms := rate / workers / 10
	if perWorkerPer100ms < 1 {
		perWorkerPer100ms = 1
	}
	deadline := time.Now().Add(dur)
	// Distinct payloads (built once) so change events aren't trivially
	// compressible — see payloadPoolSize.
	pool := randomPayloadPool(rowSize, payloadPoolSize)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := w
		go func() {
			defer wg.Done()
			p := 0
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
					coll := db.Collection(tables[tIdx%len(tables)])
					tIdx++
					docs := make([]any, perWorkerPer100ms)
					for i := range docs {
						docs[i] = newDocument(pool[p%len(pool)])
						p++
					}
					if _, err := coll.InsertMany(ctx, docs); err != nil {
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

// newDocument builds a CDC row document from a precomputed payload. _id is left
// unset so mongo assigns it; created_at varies per document.
func newDocument(payload string) bson.M {
	return bson.M{
		"created_at": time.Now(),
		"payload":    payload,
	}
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

// payloadPoolSize is the number of distinct random payloads cycled per worker.
// A single reused payload (cheap for the producer) is trivially compressible, so
// the Connect redpanda output — which compresses on the wire — sends far fewer
// bytes than Debezium's uncompressed producer, making the broker-derived
// head-to-head metric apples-to-oranges. Cycling a pool of distinct random
// payloads defeats batch compression for BOTH engines (a compression batch is
// ~1 MB ≈ 850 docs; a pool larger than that leaves few repeats per batch) while
// staying cheap — the pool is built once, not per document. 4096 comfortably
// exceeds a single compression batch (~1 MB ≈ 850 docs), so every batch is
// effectively unique and neither engine's producer can compress it — closing
// the residual self-report-vs-broker gap (calibration showed a 1024 pool still
// left Connect ~1.5x compressible while Debezium's producer doesn't compress).
const payloadPoolSize = 4096

// randomPayloadPool builds n distinct random payloads of ~size bytes.
func randomPayloadPool(size, n int) []string {
	pool := make([]string, n)
	for i := range pool {
		pool[i] = randomPayload(size)
	}
	return pool
}
