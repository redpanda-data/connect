// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// This harness isolates the COMMIT REGIME: how sink throughput responds to
// catalog commit latency, the number of concurrent in-flight submissions
// (max_in_flight), and the records carried per submission. It exists because
// the sink's headline throughput problem is latency-bound rather than
// CPU-bound, and the fix under evaluation (a commit linger) is a property of
// the commit batcher alone.
//
// It deliberately does NOT write parquet or touch object storage: files are
// synthesised metadata-only, so nothing here is confounded by encode or upload
// cost. Per-record CPU is measured separately by the bench/ package.
//
// Why a local latency injection is a faithful stand-in for a live catalog:
// measurement against a live Unity Catalog found commit latency near-flat
// across a 667x range of batch sizes (~5.2s at 300 records/commit rising only
// to ~9.7s at 200,000). Commit latency therefore behaves as a near-constant
// independent of batch size, which is exactly what a fixed injected delay
// models — with the advantage that the delay can be swept across regimes
// (a fast catalog at ~320ms, a slow engine-backed one at 5-10s) instead of
// being pinned to whatever one hosted service happens to do.

var (
	regimeSweep = flag.Bool("iceberg.commit-regime", false,
		"run the commit-regime sweep (takes minutes of wall time; prints a table)")
	regimeRealistic = flag.Bool("iceberg.commit-regime-realistic", false,
		"sweep at real engine-backed catalog latencies (5s/10s) instead of scaled-down ones")
)

// latentCatalog wraps memCatalog with a fixed per-commit delay and counts
// commits, standing in for a catalog whose commit path costs real wall time
// (credential vending, metadata write, engine-side validation).
//
// The committer serialises all commits under commitMu, so CommitTable is never
// called concurrently; the counter is atomic only so readers can sample it
// while the sweep runs.
type latentCatalog struct {
	*memCatalog
	delay   time.Duration
	commits atomic.Int64
}

func (c *latentCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	c.commits.Add(1)
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

// regimeParams describes one point in the sweep.
type regimeParams struct {
	commitLatency    time.Duration // injected per-commit catalog cost
	inFlight         int           // concurrent submitters, i.e. max_in_flight
	recordsPerSubmit int           // records carried by each submission
	window           time.Duration // measurement window
}

// regimeResult is what one point measured.
type regimeResult struct {
	params           regimeParams
	records          int64
	submissions      int64
	commits          int64
	elapsed          time.Duration
	recordsPerSecond float64
	recordsPerCommit float64
	submitsPerCommit float64
}

func (r regimeResult) String() string {
	return fmt.Sprintf("latency=%-6v in_flight=%-3d rec/submit=%-7d | rec/s=%-10.0f rec/commit=%-10.0f submits/commit=%-5.2f commits=%d",
		r.params.commitLatency, r.params.inFlight, r.params.recordsPerSubmit,
		r.recordsPerSecond, r.recordsPerCommit, r.submitsPerCommit, r.commits)
}

// runRegime drives `inFlight` concurrent submitters against a committer whose
// catalog costs `commitLatency` per commit, for `window` of wall time, and
// reports what got through. Each submitter models one in-flight WriteBatch:
// build files, submit, block until the commit that carries them returns.
func runRegime(tb testing.TB, p regimeParams) regimeResult {
	tb.Helper()
	ctx := tb.Context()

	tbl, mem := newTestTable(tb)
	cat := &latentCatalog{memCatalog: mem, delay: p.commitLatency}

	c, err := NewCommitter(tbl, cat, CommitConfig{
		ManifestMergeEnabled: false,
		MaxRetries:           1,
	}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil },
		service.MockResources().Logger())
	require.NoError(tb, err)
	defer c.Close()

	var records, submissions atomic.Int64
	deadline := time.Now().Add(p.window)
	schemaID := c.currentSchemaID()

	// Submitters cannot assert: require's FailNow is only valid on the test
	// goroutine. Each records its first error for the test goroutine to check.
	errs := make([]error, p.inFlight)

	var wg sync.WaitGroup
	start := time.Now()
	for i := range p.inFlight {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				df, err := recordCountDataFile(tbl.Spec(),
					fmt.Sprintf("%s/data/%s.parquet", tbl.Location(), uuid.New()),
					int64(p.recordsPerSubmit))
				if err != nil {
					errs[i] = err
					return
				}
				if err := c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: schemaID}); err != nil {
					errs[i] = err
					return
				}
				submissions.Add(1)
				records.Add(int64(p.recordsPerSubmit))
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Assert before deriving any rate from a possibly half-completed run.
	require.NoError(tb, errors.Join(errs...), "submitter(s) failed")

	commits := cat.commits.Load()
	res := regimeResult{
		params:      p,
		records:     records.Load(),
		submissions: submissions.Load(),
		commits:     commits,
		elapsed:     elapsed,
	}
	res.recordsPerSecond = float64(res.records) / elapsed.Seconds()
	if commits > 0 {
		res.recordsPerCommit = float64(res.records) / float64(commits)
		res.submitsPerCommit = float64(res.submissions) / float64(commits)
	}
	return res
}

// recordCountDataFile is synthDataFile with a caller-chosen record count, so a
// submission can represent a realistic batch rather than a single row.
//
// It returns an error rather than asserting, because the submitters that call it
// run on their own goroutines: require's FailNow is only valid on the goroutine
// running the test, so a failure here has to be carried back and asserted after
// the submitters have been waited on.
func recordCountDataFile(spec iceberg.PartitionSpec, path string, records int64) (iceberg.DataFile, error) {
	b, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		nil, nil, nil,
		records, records*64,
	)
	if err != nil {
		return nil, err
	}
	return b.Build(), nil
}

// TestCommitRegimeSweep characterises throughput across the commit regime.
// Flag-gated: it spends real wall time on purpose.
//
// The question it answers: does the existing batcher already coalesce
// concurrent submissions during a slow commit (in which case max_in_flight is
// the lever and a linger adds nothing), or does each commit carry only one
// submission (in which case a time-based linger is the fix)?
func TestCommitRegimeSweep(t *testing.T) {
	if !*regimeSweep {
		t.Skip("set -iceberg.commit-regime to run the commit-regime sweep")
	}

	latencies := []time.Duration{50 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond}
	window := 6 * time.Second
	if *regimeRealistic {
		latencies = []time.Duration{320 * time.Millisecond, 5 * time.Second, 10 * time.Second}
		window = 60 * time.Second
	}

	var results []regimeResult
	for _, latency := range latencies {
		for _, inFlight := range []int{1, 4, 16, 64} {
			res := runRegime(t, regimeParams{
				commitLatency:    latency,
				inFlight:         inFlight,
				recordsPerSubmit: 300, // the observed "throughput trap" batch size
				window:           window,
			})
			results = append(results, res)
			t.Log(res.String())
		}
	}

	t.Log("=== commit regime sweep (records/submit = 300) ===")
	for _, r := range results {
		t.Log(r.String())
	}
}

// TestCommitCoalescesConcurrentSubmissions pins the mechanism the sweep
// explores, cheaply enough to run in CI: when several submissions are in
// flight while a slow commit is running, they must be merged into ONE
// subsequent commit rather than committed one at a time.
//
// This is the property any linger implementation must preserve — and the
// reason a linger cannot help at max_in_flight=1, where there is never a
// second submission to coalesce with.
func TestCommitCoalescesConcurrentSubmissions(t *testing.T) {
	const (
		inFlight = 8
		latency  = 300 * time.Millisecond
	)
	ctx := t.Context()

	tbl, mem := newTestTable(t)
	cat := &latentCatalog{memCatalog: mem, delay: latency}

	c, err := NewCommitter(tbl, cat, CommitConfig{
		ManifestMergeEnabled: false,
		MaxRetries:           1,
	}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil },
		service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	schemaID := c.currentSchemaID()

	// Build the data files up front, on the test goroutine, so the submitters
	// below only have to commit — and so nothing in them needs to assert.
	files := make([]iceberg.DataFile, inFlight)
	for i := range files {
		df, err := recordCountDataFile(tbl.Spec(),
			fmt.Sprintf("%s/data/coalesce-%d-%s.parquet", tbl.Location(), i, uuid.New()), 300)
		require.NoError(t, err)
		files[i] = df
	}

	// Occupy the committer so the rest of the submissions queue behind it.
	errs := make([]error, inFlight)
	var wg sync.WaitGroup
	for i := range inFlight {
		wg.Go(func() {
			errs[i] = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{files[i]}, SchemaID: schemaID})
		})
	}
	wg.Wait()
	require.NoError(t, errors.Join(errs...), "submitter(s) failed")

	commits := cat.commits.Load()
	require.Positive(t, commits, "expected at least one commit")
	require.Less(t, commits, int64(inFlight),
		"expected %d concurrent submissions to coalesce into fewer than %d commits, got %d",
		inFlight, inFlight, commits)
	t.Logf("%d concurrent submissions coalesced into %d commits (%.2f submissions/commit)",
		inFlight, commits, float64(inFlight)/float64(commits))
}
