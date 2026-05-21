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

func bulkInsert(ctx context.Context, pool *pgxpool.Pool, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	payload := randomPayload(rowSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := strings.Repeat("(NOW(),$1),", 1000)
			batch = strings.TrimSuffix(batch, ",")
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
			conn, err := pool.Acquire(ctx)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Release()
			done := int64(0)
			for done < rowsPerWorker {
				if _, err := conn.Exec(ctx, stmt, payload); err != nil {
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
	const workers = 8
	cfg, err := pgxpool.ParseConfig(os.Getenv("POSTGRES_DSN"))
	if err != nil {
		return err
	}
	cfg.MaxConns = int32(workers)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

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
			payload := randomPayload(rowSize)
			batch := strings.Repeat("(NOW(),$1),", perWorkerPer100ms)
			batch = strings.TrimSuffix(batch, ",")
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
					if _, err := pool.Exec(ctx, stmt, payload); err != nil {
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

func randomPayload(size int) string {
	b := make([]byte, (size*3)/4+1)
	_, _ = rand.Read(b)
	s := base64.StdEncoding.EncodeToString(b)
	if len(s) > size {
		s = s[:size]
	}
	return s
}
