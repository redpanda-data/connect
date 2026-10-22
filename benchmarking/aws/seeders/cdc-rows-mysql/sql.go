// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// This is the mysql port of cdc-rows-postgres: same worker/tick fairness
// math (every row and every write/sec accounted for, no truncation), same
// distinct-payload pool (identical payloads compress away and misreport
// throughput by 11-17x — see payloadPoolSize), with database/sql +
// go-sql-driver in place of pgx and `?` placeholders in place of `$n`.

package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

func openDB(maxConns int) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(os.Getenv("MYSQL_DSN"))
	if err != nil {
		return nil, err
	}
	// pgx (the postgres seeder's driver) caches prepared statements
	// transparently; go-sql-driver does NOT — without InterpolateParams it
	// returns driver.ErrSkip for parameterized Exec and database/sql wraps
	// every insert in a hidden server-side Prepare+Execute+Close, doubling
	// statement traffic and roughly halving the per-worker ceiling the
	// 16-worker design assumes. Payloads are base64 ASCII, so client-side
	// interpolation is injection-safe here.
	cfg.InterpolateParams = true
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
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
		"DROP TABLE IF EXISTS " + table,
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

// workerRowCounts splits rows across workers as evenly as possible,
// handing the remainder (rows % workers) one extra row each to the first
// `remainder` workers. Truncating division alone (rows/workers) silently
// drops up to workers-1 rows off the total — for rows=10, workers=16 that
// truncates to 0 per worker and seeds nothing at all — so every row must be
// accounted for in the returned counts, which always sum to exactly rows.
func workerRowCounts(rows int64, workers int) []int64 {
	counts := make([]int64, workers)
	if workers <= 0 {
		return counts
	}
	base := rows / int64(workers)
	remainder := rows % int64(workers)
	for w := range workers {
		counts[w] = base
		if int64(w) < remainder {
			counts[w]++
		}
	}
	return counts
}

func bulkInsert(ctx context.Context, db *sql.DB, table string, rows int64, rowSize int) error {
	const workers = 16
	counts := workerRowCounts(rows, workers)
	pool := randomPayloadPool(rowSize, payloadPoolSize)
	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		workerRows := counts[w]
		go func() {
			defer wg.Done()
			// A worker whose share rounded down to zero (e.g. rows=10 spread
			// over 16 workers) has nothing to insert.
			if workerRows == 0 {
				errCh <- nil
				return
			}
			const batchSize = 1000
			conn, err := db.Conn(ctx)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()

			cursor := 0
			done := int64(0)
			// The full-size statement is built once and reused for every
			// full batch. Only the trailing partial batch — whatever is
			// left after the last full batch — needs a statement sized to
			// exactly that many rows, otherwise the loop either overshoots
			// (inserting a full batch when fewer rows remain) or requires
			// padding args that don't exist.
			var fullStmt string
			var fullArgs []any
			if workerRows >= batchSize {
				fullStmt = fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, valuesList(batchSize))
				fullArgs = make([]any, batchSize)
			}
			for workerRows-done >= batchSize {
				fillArgs(fullArgs, pool, &cursor)
				if _, err := conn.ExecContext(ctx, fullStmt, fullArgs...); err != nil {
					errCh <- err
					return
				}
				done += batchSize
			}
			if remaining := workerRows - done; remaining > 0 {
				stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, valuesList(int(remaining)))
				args := make([]any, remaining)
				fillArgs(args, pool, &cursor)
				if _, err := conn.ExecContext(ctx, stmt, args...); err != nil {
					errCh <- err
					return
				}
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

// ticksPerSecond is the number of 100ms ticker fires per second that each
// workload worker uses to spread its per-second quota out smoothly instead
// of bursting it all in one Exec.
const ticksPerSecond = 10

// perWorkerRate splits a total rows/sec rate across workers as evenly as
// possible, handing the remainder (rate % workers) one extra row/sec each to
// the first `remainder` workers. Truncating division alone drops up to
// workers-1 rows/sec off the total, which is exactly the kind of small,
// deterministic undershoot that lets a backlog metric drift forever on an
// otherwise healthy run.
func perWorkerRate(rate, workers int) []int {
	rates := make([]int, workers)
	if workers <= 0 {
		return rates
	}
	base := rate / workers
	remainder := rate % workers
	for w := range workers {
		rates[w] = base
		if w < remainder {
			rates[w]++
		}
	}
	return rates
}

// tickCounts splits one worker's per-second quota across the ticksPerSecond
// ticks of its 100ms ticker, again distributing the remainder so the ticks
// sum to exactly ratePerWorker. This mirrors perWorkerRate one level down;
// without it, a worker's per-tick truncation (ratePerWorker/10) compounds
// with the per-worker truncation above into a rate that's measurably below
// what was requested.
func tickCounts(ratePerWorker int) [ticksPerSecond]int {
	var counts [ticksPerSecond]int
	base := ratePerWorker / ticksPerSecond
	remainder := ratePerWorker % ticksPerSecond
	for t := range ticksPerSecond {
		counts[t] = base
		if t < remainder {
			counts[t]++
		}
	}
	return counts
}

func workload(ctx context.Context, tables []string, rowSize, rate int, dur time.Duration) error {
	// A single goroutine driving large per-tick batches caps around 30-40K
	// inserts/sec on c8g.large because statement parsing + one network RTT
	// per tick eats the budget. Spread across workers, each with a smaller
	// batch, so the scenario's write_rate_per_sec is actually achievable.
	// 16 workers handles 150K writes/sec comfortably (each worker ~9.4K/sec,
	// well under the per-worker ceiling).
	const workers = 16
	db, err := openDB(workers)
	if err != nil {
		return err
	}
	defer db.Close()

	rates := perWorkerRate(rate, workers)
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		workerIdx := w
		ratePerWorker := rates[w]
		go func() {
			defer wg.Done()
			// Distinct payloads (built once) so change events aren't trivially
			// compressible — see payloadPoolSize.
			pool := randomPayloadPool(rowSize, payloadPoolSize)
			cursor := 0
			counts := tickCounts(ratePerWorker)

			// Ticks only ever need one of two row counts (base or base+1),
			// so the VALUES clauses are built once here instead of on every
			// tick; a size of 0 is skipped since such a tick issues no Exec.
			base := ratePerWorker / ticksPerSecond
			remainder := ratePerWorker % ticksPerSecond
			var baseValues, plusValues string
			var baseArgs, plusArgs []any
			if base > 0 {
				baseValues = valuesList(base)
				baseArgs = make([]any, base)
			}
			if remainder > 0 {
				plusValues = valuesList(base + 1)
				plusArgs = make([]any, base+1)
			}

			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			tIdx := workerIdx
			tickInSecond := 0
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
					// The table rotates every tick regardless of whether
					// this tick has any rows to insert, so the rotation
					// stays in lockstep across workers.
					table := tables[tIdx%len(tables)]
					tIdx++
					count := counts[tickInSecond]
					tickInSecond = (tickInSecond + 1) % ticksPerSecond
					if count == 0 {
						continue
					}
					values, args := baseValues, baseArgs
					if count == base+1 {
						values, args = plusValues, plusArgs
					}
					stmt := fmt.Sprintf("INSERT INTO %s (created_at, payload) VALUES %s", table, values)
					fillArgs(args, pool, &cursor)
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
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
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
// (NOW(6),?),(NOW(6),?),...
//
// Distinct payloads need distinct placeholders (see cdc-rows-postgres, whose
// single-repeated-placeholder bug this port never inherits). MySQL's
// per-statement limit is max_allowed_packet, not a placeholder count; n=1000
// rows of ~1.2 KB is ~1.2 MB, inside RDS's 64 MB default.
func valuesList(n int) string {
	var sb strings.Builder
	for i := range n {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(NOW(6),?)")
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
