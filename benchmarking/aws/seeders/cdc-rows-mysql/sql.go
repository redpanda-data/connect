// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func openDB(maxConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", os.Getenv("MYSQL_DSN"))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	return db, nil
}

func seed(ctx context.Context, tables []string, rows int64, rowSize int) error {
	db, err := openDB(16)
	if err != nil {
		return err
	}
	defer db.Close()

	for _, table := range tables {
		if err := ensureTable(ctx, db, table, rowSize); err != nil {
			return err
		}
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

func ensureTable(ctx context.Context, db *sql.DB, table string, rowSize int) error {
	stmts := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf(`CREATE TABLE %s (
			id          BIGINT AUTO_INCREMENT PRIMARY KEY,
			created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			payload     TEXT NOT NULL
		) ENGINE=InnoDB`, table),
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", s, err)
		}
	}
	_ = rowSize
	return nil
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, rows int64, rowSize int) error {
	const workers = 16
	rowsPerWorker := rows / workers
	if rowsPerWorker == 0 {
		// Allow rows=0 (scenario.dataset.initial_rows: 0): ensureTable already
		// ran, so the table exists but stays empty.
		return nil
	}
	payload := randomPayload(rowSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const batchSize = 1000
			batch := strings.Repeat("(NOW(6),?),", batchSize)
			batch = strings.TrimSuffix(batch, ",")
			stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, batch)
			args := make([]any, batchSize)
			for i := range args {
				args[i] = payload
			}
			done := int64(0)
			for done < rowsPerWorker {
				if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
					errCh <- err
					return
				}
				done += int64(batchSize)
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
	// 8 workers, each writing rate/8/10 rows per 100ms tick. Mirrors the
	// postgres seeder's parallelism so the producer can match the scenario's
	// write_rate_per_sec at 80K+ writes/sec.
	const workers = 8
	db, err := openDB(workers)
	if err != nil {
		return err
	}
	defer db.Close()

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
			batch := strings.Repeat("(NOW(6),?),", perWorkerPer100ms)
			batch = strings.TrimSuffix(batch, ",")
			args := make([]any, perWorkerPer100ms)
			for i := range args {
				args[i] = payload
			}
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
					if _, err := db.ExecContext(ctx, stmt, args...); err != nil {
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
