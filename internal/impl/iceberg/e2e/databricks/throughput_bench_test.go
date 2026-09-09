// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package databrickse2e

import (
	"bytes"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestDatabricksThroughput characterizes the "small-commit
// throughput trap" against a live Unity Catalog: plain APPEND commits (zero
// RowOpConfig — NOT copy-on-write) at a sweep of records-per-Route batch
// sizes, each driven for a fixed wall window. Because the router commits
// synchronously per Route call, per-call latency IS per-commit latency, and
// sustained throughput is records/window. The smallest point (300) exposes
// UC's floor commit latency for pure appends, comparable against AWS Glue's
// ~320ms and the COW bench's ~7-10s overwrites.
//
// Gated behind -databricks.throughput because it holds a live catalog busy
// for ~15 minutes of real time.

var dbxThroughput = flag.Bool("databricks.throughput", false, "run the append throughput bench (drives the live catalog for ~15 minutes)")

const (
	// throughputWindow is the wall-clock measurement window per batch-size
	// point. The last Route call may overshoot it; throughput uses the actual
	// elapsed time at that call's completion.
	throughputWindow = 3 * time.Minute
	// bigRouteCutoff: if the first measured Route call of a point exceeds
	// this, the point is truncated to two calls total instead of the full
	// window (per the run plan for the largest batch point).
	bigRouteCutoff = 90 * time.Second
	// payloadLen makes each JSON record ~1.2KB, mirroring the earlier
	// benchmark methodology (id + seq + ~1.1KB string payload + JSON framing).
	payloadLen = 1100
)

// syncBuffer is a concurrency-safe bytes.Buffer for the capturing logger —
// the committer may log from goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newCapturingRouter mirrors newRouter but wires a capturing slog logger (the
// output_iceberg_test.go seam) so committer warnings — commit retries,
// unknown-state, prohibited-key stripping — are collectable evidence.
func newCapturingRouter(t *testing.T, namespace, tableName string, rowOp icebergimpl.RowOpConfig) (*icebergimpl.Router, *syncBuffer) {
	t.Helper()
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	tableStr, err := service.NewInterpolatedString(tableName)
	require.NoError(t, err)

	sb := &syncBuffer{}
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(sb, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})))

	commitCfg := icebergimpl.CommitConfig{
		ManifestMergeEnabled: true,
		MaxSnapshotAge:       24 * time.Hour,
		MaxRetries:           3,
	}
	router := icebergimpl.NewRouter(buildCatalogConfig(), namespaceStr, tableStr, true,
		icebergimpl.SchemaEvolutionConfig{Enabled: true}, commitCfg, rowOp, nil, logger)
	t.Cleanup(func() { router.Close() })
	return router, sb
}

// benchBatch builds a batch of structured append messages (~1.2KB of JSON
// each). Payload strings are zero-copy slices of a shared random pool so
// generation cost stays negligible next to multi-second commits; startID
// advances per call so ids stay unique and content varies across commits.
func benchBatch(pool string, startID int64, size int) service.MessageBatch {
	rng := rand.New(rand.NewSource(startID)) //nolint:gosec // bench entropy, not crypto
	msgs := make(service.MessageBatch, size)
	for i := range msgs {
		off := rng.Intn(len(pool) - payloadLen)
		m := service.NewMessage(nil)
		m.SetStructured(map[string]any{
			"id":      startID + int64(i),
			"seq":     int64(i),
			"payload": pool[off : off+payloadLen],
		})
		msgs[i] = m
	}
	return msgs
}

// warnEvidence tallies committer warning lines relevant to the parked #4591
// throttle/5xx hypothesis and keeps a few (redacted) samples.
type warnEvidence struct {
	commitRetries  int
	prohibitedKeys int
	unknownState   int
	otherWarnings  int
	samples        []string
}

func collectWarnings(logged string) warnEvidence {
	var ev warnEvidence
	for line := range strings.SplitSeq(logged, "\n") {
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		switch {
		case strings.Contains(line, "Commit attempt"):
			ev.commitRetries++
		case strings.Contains(lower, "prohibit"):
			ev.prohibitedKeys++
		case strings.Contains(lower, "unknown state"):
			ev.unknownState++
		case strings.Contains(lower, "level=warn"), strings.Contains(lower, "level=error"):
			ev.otherWarnings++
		default:
			continue
		}
		if len(ev.samples) < 5 {
			ev.samples = append(ev.samples, redact(line))
		}
	}
	return ev
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

type throughputPoint struct {
	batchSize     int
	commits       int
	records       int64
	elapsed       time.Duration
	p50, p95, max time.Duration
	failures      int
	warnings      warnEvidence
}

func (pt throughputPoint) String() string {
	perMin := float64(pt.commits) / pt.elapsed.Minutes()
	return fmt.Sprintf("batch=%d commits=%d records=%d window=%v rec/s=%.0f commit_p50=%v p95=%v max=%v commits/min=%.1f failures=%d retries=%d prohibited=%d unknown=%d otherWarn=%d",
		pt.batchSize, pt.commits, pt.records, pt.elapsed.Round(time.Second),
		float64(pt.records)/pt.elapsed.Seconds(),
		pt.p50.Round(time.Millisecond), pt.p95.Round(time.Millisecond), pt.max.Round(time.Millisecond),
		perMin, pt.failures,
		pt.warnings.commitRetries, pt.warnings.prohibitedKeys, pt.warnings.unknownState, pt.warnings.otherWarnings)
}

func TestDatabricksThroughput(t *testing.T) {
	skipIfNotConfigured(t)
	if !*dbxThroughput {
		t.Skip("set -databricks.throughput to run the append throughput bench")
	}
	ctx := t.Context()

	// Shared random pool for payload slicing (allocated once for the run).
	poolRng := rand.New(rand.NewSource(42)) //nolint:gosec // bench entropy, not crypto
	const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	poolBytes := make([]byte, 64*1024)
	for i := range poolBytes {
		poolBytes[i] = alnum[poolRng.Intn(len(alnum))]
	}
	pool := string(poolBytes)

	var results []throughputPoint

	for _, batchSize := range []int{300, 5000, 50000, 200000} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			tableName := uniqueTableName(fmt.Sprintf("tput_%d", batchSize))
			t.Cleanup(func() { dropTable(t, tableName) })

			// Pre-create the table so the measured window contains only
			// append commits — no CREATE TABLE inside the measurement.
			client := newCatalogClient(t, ctx)
			_, err := client.CreateTable(ctx, tableName, iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}},
				iceberg.NestedField{ID: 2, Name: "seq", Type: iceberg.Int64Type{}},
				iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.StringType{}},
			))
			require.NoError(t, err)

			// Zero RowOpConfig => every message is a plain append.
			router, logBuf := newCapturingRouter(t, *dbxSchema, tableName, icebergimpl.RowOpConfig{})

			// Uncounted warmup: absorbs the first-write bootstrap (table
			// load + timestamp-encoding property stamp) so measured calls
			// are steady-state appends.
			nextID := int64(1)
			require.NoError(t, router.Route(ctx, benchBatch(pool, nextID, 10)))
			nextID += 10

			var (
				latencies []time.Duration
				records   int64
				failures  int
			)
			start := time.Now()
			var elapsed time.Duration
			for {
				batch := benchBatch(pool, nextID, batchSize)
				nextID += int64(batchSize)

				callStart := time.Now()
				routeErr := router.Route(ctx, batch)
				callDur := time.Since(callStart)
				elapsed = time.Since(start)

				if routeErr != nil {
					failures++
					t.Logf("batch=%d commit %d FAILED after %v: %v", batchSize, len(latencies)+failures, callDur.Round(time.Millisecond), redact(routeErr.Error()))
					if failures >= 3 {
						t.Logf("batch=%d: aborting point after %d consecutive-ish failures", batchSize, failures)
						break
					}
				} else {
					latencies = append(latencies, callDur)
					records += int64(batchSize)
					t.Logf("batch=%d commit %d: %v (%.0f rec/s within call)", batchSize, len(latencies), callDur.Round(time.Millisecond), float64(batchSize)/callDur.Seconds())
				}

				if elapsed >= throughputWindow {
					break
				}
				// Very large batches: cap at two calls if a single call
				// blows past the cutoff (keeps live time bounded).
				if len(latencies)+failures >= 2 && callDur > bigRouteCutoff {
					t.Logf("batch=%d: truncating point to %d calls (single Route exceeded %v)", batchSize, len(latencies)+failures, bigRouteCutoff)
					break
				}
			}

			slices.Sort(latencies)
			pt := throughputPoint{
				batchSize: batchSize,
				commits:   len(latencies),
				records:   records,
				elapsed:   elapsed,
				p50:       percentile(latencies, 0.50),
				p95:       percentile(latencies, 0.95),
				max:       percentile(latencies, 1.0),
				failures:  failures,
				warnings:  collectWarnings(logBuf.String()),
			}
			results = append(results, pt)
			t.Logf("POINT RESULT: %s", pt)
			for _, s := range pt.warnings.samples {
				t.Logf("  warning sample: %s", s)
			}
		})
	}

	t.Log("=== THROUGHPUT SWEEP SUMMARY (append mode, live Unity Catalog) ===")
	for _, pt := range results {
		t.Logf("  %s", pt)
	}
}
