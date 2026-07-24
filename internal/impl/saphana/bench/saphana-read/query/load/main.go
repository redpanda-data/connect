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
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/SAP/go-hdb/driver"
)

func main() {
	count := flag.Int("count", 1000000, "number of rows to insert")
	batch := flag.Int("batch", 5000, "rows per transaction")
	workers := flag.Int("workers", 20, "number of parallel insert workers")
	flag.Parse()

	dsn := os.Getenv("HANA_DSN")
	if dsn == "" {
		fmt.Fprintf(os.Stderr, "HANA_DSN environment variable is required\n")
		os.Exit(1)
	}
	schema := os.Getenv("HANA_SCHEMA")
	if schema == "" {
		fmt.Fprintf(os.Stderr, "HANA_SCHEMA environment variable is required\n")
		os.Exit(1)
	}
	table := fmt.Sprintf(`"%s"."BENCH_ORDERS_QUERY"`, schema)

	ctx := context.Background()

	db, err := sql.Open("hdb", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to database: %v\n", err)
		os.Exit(1)
	}

	_, _ = db.ExecContext(ctx, `DROP TABLE `+table)
	createSQL := `CREATE COLUMN TABLE ` + table + ` (` +
		`ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,` +
		`USER_ID INTEGER,` +
		`PRODUCT_ID INTEGER,` +
		`QUANTITY INTEGER,` +
		`PRICE DECIMAL(10,2),` +
		`STATUS NVARCHAR(20),` +
		`NOTES NVARCHAR(200),` +
		`CREATED_AT TIMESTAMP` +
		`)`
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create table: %v\n", err)
		os.Exit(1)
	}

	statuses := []string{"pending", "confirmed", "shipped"}

	effectiveWorkers := *workers
	if *count < effectiveWorkers {
		effectiveWorkers = *count
	}
	perWorker := *count / effectiveWorkers

	var (
		counter atomic.Int64
		wg      sync.WaitGroup
		errCh   = make(chan error, effectiveWorkers)
	)

	start := time.Now()

	stopProgress := make(chan struct{})
	go func() {
		prev := int64(0)
		prevTime := start
		for {
			select {
			case <-stopProgress:
				return
			case <-time.After(250 * time.Millisecond):
				now := time.Now()
				cur := counter.Load()
				rate := float64(cur-prev) / now.Sub(prevTime).Seconds()
				fmt.Printf("\r%d / %d  (%.0f rows/s)", cur, *count, rate)
				prev = cur
				prevTime = now
			}
		}
	}()

	for w := 0; w < effectiveWorkers; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()

			startRow := workerIdx * perWorker
			endRow := startRow + perWorker
			if workerIdx == effectiveWorkers-1 {
				endRow = *count
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerIdx)))

			for batchStart := startRow; batchStart < endRow; batchStart += *batch {
				batchEnd := batchStart + *batch
				if batchEnd > endRow {
					batchEnd = endRow
				}
				n := batchEnd - batchStart

				args := make([]any, 0, n*7)
				for i := batchStart; i < batchEnd; i++ {
					notes := strings.Repeat(fmt.Sprintf("note_%d ", i+1), 2)
					args = append(args,
						rng.Intn(100000)+1,
						rng.Intn(10000)+1,
						rng.Intn(10)+1,
						1.0+rng.Float64()*999.0,
						statuses[rng.Intn(3)],
						notes,
						time.Now().AddDate(0, 0, -rng.Intn(365)),
					)
				}

				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					errCh <- fmt.Errorf("worker %d begin tx: %w", workerIdx, err)
					return
				}
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO `+table+` (USER_ID, PRODUCT_ID, QUANTITY, PRICE, STATUS, NOTES, CREATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?)`,
					args...,
				); err != nil {
					_ = tx.Rollback()
					errCh <- fmt.Errorf("worker %d exec batch %d: %w", workerIdx, batchStart, err)
					return
				}
				if err := tx.Commit(); err != nil {
					errCh <- fmt.Errorf("worker %d commit: %w", workerIdx, err)
					return
				}
				counter.Add(int64(n))
			}
		}(w)
	}

	wg.Wait()
	close(stopProgress)
	close(errCh)

	for err := range errCh {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(start)
	total := int(counter.Load())
	fmt.Printf("\r%d / %d  (done)                    \n", total, *count)
	fmt.Printf("Inserted %d rows in %s (%.0f rows/s)\n", total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}
