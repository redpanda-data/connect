// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

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

	"github.com/jackc/pgx/v5/pgxpool"
)

func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return err
	}
	defer pool.Close()

	for _, table := range tables {
		if err := ensureTable(ctx, pool, table, rowSize); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(tables))
	for _, table := range tables {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			errCh <- bulkInsert(ctx, pool, t, rows, rowSize)
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

func ensureTable(ctx context.Context, pool *pgxpool.Pool, table string, rowSize int) error {
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf(`CREATE TABLE %s (
			id          BIGSERIAL PRIMARY KEY,
			created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			payload     TEXT NOT NULL
		)`, table),
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_ = rowSize
	return nil
}

func bulkInsert(ctx context.Context, pgPool *pgxpool.Pool, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	pool := randomPayloadPool(rowSize, payloadPoolSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const batchSize = 1000
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, valuesList(batchSize))
			args := make([]any, batchSize)
			cursor := 0
			conn, err := pgPool.Acquire(ctx)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Release()
			done := int64(0)
			for done < rowsPerWorker {
				fillArgs(args, pool, &cursor)
				if _, err := conn.Exec(ctx, stmt, args...); err != nil {
					errCh <- err
					return
				}
				done += 1000
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

func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration) error {
	// A single goroutine driving large per-tick batches caps around 30-40K
	// inserts/sec on c8g.large because statement parsing + one network RTT
	// per tick eats the budget. Spread across workers, each with a smaller
	// batch, so the scenario's write_rate_per_sec is actually achievable.
	// 16 workers handles 150K writes/sec comfortably (each worker ~9.4K/sec,
	// well under the per-worker ceiling).
	const workers = 16
	cfg, err := pgxpool.ParseConfig(os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(workers)
	pgPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pgPool.Close()

	perWorkerPer100ms := rate / workers / 10
	if perWorkerPer100ms < 1 {
		perWorkerPer100ms = 1
	}
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		workerIdx := w
		go func() {
			defer wg.Done()
			// Distinct payloads (built once) so change events aren't trivially
			// compressible — see payloadPoolSize.
			pool := randomPayloadPool(rowSize, payloadPoolSize)
			cursor := 0
			batch := valuesList(perWorkerPer100ms)
			args := make([]any, perWorkerPer100ms)
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
					stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
					fillArgs(args, pool, &cursor)
					if _, err := pgPool.Exec(ctx, stmt, args...); err != nil {
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

// payloadPoolSize is the number of distinct random payloads cycled per worker.
//
// Ported from cdc-rows-mongodb, the only seeder that originally had this right.
// Reusing ONE identical payload for every row makes each producer batch
// trivially compressible, and that alone accounted for the 11-17x gap between
// Connect's self-reported throughput and the broker's byte counters across the
// postgres, mysql, oracle and sqlserver benches. Mongo's calibration note is the
// authority on the size: 4096 comfortably exceeds one compression batch, where
// 1024 still left ~1.5x compressible.
const payloadPoolSize = 4096

// valuesList builds a multi-row VALUES clause with ONE PLACEHOLDER PER ROW:
// (NOW(),$1),(NOW(),$2),...,(NOW(),$n)
//
// It used to repeat "$1" n times, which only worked because every row carried an
// identical payload. Distinct payloads need distinct placeholders. Postgres
// allows 65535 parameters per statement, so n=1000 is far inside the limit.
func valuesList(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "(NOW(),$%d)", i+1)
	}
	return sb.String()
}

// fillArgs refills args from the pool, advancing the cursor. Refilling every
// batch matters: filling once would make each batch byte-identical to the last,
// which compresses just as well as a single repeated payload did.
func fillArgs(args []any, pool []string, cursor *int) {
	for i := range args {
		args[i] = pool[*cursor%len(pool)]
		*cursor++
	}
}

// randomPayloadPool builds n distinct random payloads of ~size bytes.
func randomPayloadPool(size, n int) []string {
	pool := make([]string, n)
	for i := range pool {
		pool[i] = randomPayload(size)
	}
	return pool
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
