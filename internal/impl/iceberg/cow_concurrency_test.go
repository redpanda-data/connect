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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// occCatalog is an in-memory table.CatalogIO that, unlike memCatalog, actually
// enforces optimistic concurrency: on every CommitTable it validates the
// transaction's requirements (notably assert-ref-snapshot-id) against the
// current metadata and rejects a stale commit with rest.ErrCommitFailed — the
// same signal a real REST catalog returns on a 409. This is what turns two
// committers racing on the same table into a genuine stage -> conflict ->
// reload -> re-stage -> success exchange rather than two blind last-write-wins
// applies. All access to meta and the commit counter is guarded by mu, so the
// type is safe for concurrent use (and clean under -race).
type occCatalog struct {
	mu               sync.Mutex
	meta             table.Metadata
	metadataLocation string
	ident            table.Identifier
	location         string
	calls            int // total CommitTable invocations, including rejected ones
}

func (c *occCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return c.snapshot(), nil
}

func (c *occCatalog) CommitTable(_ context.Context, _ table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++

	// Enforce optimistic concurrency exactly as a real catalog does: if any
	// requirement no longer holds against the current metadata (e.g. main has
	// advanced since this transaction was built), reject with ErrCommitFailed so
	// the committer reloads and retries.
	for _, r := range reqs {
		if err := r.Validate(c.meta); err != nil {
			return nil, "", fmt.Errorf("%w: %v", rest.ErrCommitFailed, err)
		}
	}

	meta, err := table.UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	c.meta = meta
	return meta, c.metadataLocation, nil
}

func (c *occCatalog) snapshot() *table.Table {
	c.mu.Lock()
	defer c.mu.Unlock()
	return table.New(
		c.ident,
		c.meta,
		c.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	)
}

func (c *occCatalog) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// newOCCTable builds an unpartitioned v2 table for sc, backed by an
// OCC-enforcing in-memory catalog and the local filesystem.
func newOCCTable(t testing.TB, sc *iceberg.Schema) *occCatalog {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	return &occCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "cow_conc"},
		location:         location,
	}
}

// countTableRows returns the total live row count of the table, used to detect a
// duplicate that a keyed map (like scanRows) would silently hide by overwriting.
func countTableRows(t testing.TB, ctx context.Context, tbl *table.Table) int {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	return int(at.NumRows())
}

// TestCOWConcurrentCommittersConverge proves item 2.3: two committers applying
// copy-on-write mutations to the SAME table concurrently converge correctly via
// the OCC-conflict + retry path, with no lost updates and no duplicates.
//
// Both committers are constructed from the same seed snapshot and never reload
// before their first attempt, so whichever loses the race into the catalog's
// CommitTable lock is guaranteed to find main already advanced and get
// ErrCommitFailed — deterministically exercising stage -> conflict -> reload ->
// re-stage -> success regardless of goroutine scheduling. The writes still run
// genuinely concurrently (two goroutines), so the test is meaningful under
// -race. Exactly-once is checked two ways: exactly two snapshots carry a
// mutation commit-id (one per committer, so no landed commit was re-applied),
// and the catalog saw exactly three commit applications for the two mutations
// (one clean + one conflicted-then-retried).
func TestCOWConcurrentCommittersConverge(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	logger := service.MockResources().Logger()

	// mkWriter builds an independent writer+committer pair over the shared
	// catalog, both anchored to the catalog's current (seed) snapshot. A healthy
	// MaxRetries lets the loser of the race reload and re-stage.
	mkWriter := func(t *testing.T, occ *occCatalog) *writer {
		t.Helper()
		comm, err := NewCommitter(occ.snapshot(), occ, CommitConfig{MaxRetries: 10},
			func(context.Context) (*table.Table, error) { return occ.snapshot(), nil }, logger)
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, occ.snapshot(), "id")
		w.committer = comm
		return w
	}

	// runConcurrently fires both writes at once and fails on either error.
	runConcurrently := func(t *testing.T, ctx context.Context, w1, w2 *writer, b1, b2 service.MessageBatch) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)
		go func() { defer wg.Done(); errs[0] = w1.Write(ctx, b1) }()
		go func() { defer wg.Done(); errs[1] = w2.Write(ctx, b2) }()
		wg.Wait()
		require.NoError(t, errs[0], "committer 1 write")
		require.NoError(t, errs[1], "committer 2 write")
	}

	t.Run("different keys both land", func(t *testing.T) {
		ctx := t.Context()
		occ := newOCCTable(t, sc)

		// Seed id=1,2 in a single data file (so each mutation rewrites the shared
		// file, forcing the two committers into genuine contention).
		seed := appendCOWRows(t, ctx, occ.snapshot(), map[int64]string{1: "one", 2: "two"})
		_ = seed
		base := occ.callCount()

		w1 := mkWriter(t, occ)
		w2 := mkWriter(t, occ)

		runConcurrently(t, ctx, w1, w2,
			service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "ONE"})},
			service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})},
		)

		final := occ.snapshot()
		assert.Equal(t, map[int64]string{1: "ONE", 2: "TWO"}, scanRows(t, ctx, final),
			"both upserts must survive; neither may be lost to a stale overwrite")
		assert.Equal(t, 2, countTableRows(t, ctx, final), "exactly two rows, no duplicates")
		assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
		assertAllManifestsData(t, ctx, final)

		assert.Equal(t, 2, countSnapshotsWithCommitID(final), "each mutation committed exactly once")
		assert.Equal(t, 3, occ.callCount()-base,
			"one clean commit + one conflicted-then-retried commit = 3 catalog applications")
	})

	t.Run("same key last writer wins with no duplicate", func(t *testing.T) {
		ctx := t.Context()
		occ := newOCCTable(t, sc)

		seed := appendCOWRows(t, ctx, occ.snapshot(), map[int64]string{1: "seed"})
		_ = seed
		base := occ.callCount()

		w1 := mkWriter(t, occ)
		w2 := mkWriter(t, occ)

		runConcurrently(t, ctx, w1, w2,
			service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "A"})},
			service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "B"})},
		)

		final := occ.snapshot()
		// Exactly one row for the contended key: the retry re-stages its overwrite
		// against the winner's snapshot, deleting that row and re-appending its own,
		// so there is never a duplicate. Which value wins depends on the race, but
		// it must be one of the two and there must be exactly one.
		assert.Equal(t, 1, countTableRows(t, ctx, final), "exactly one row for the contended key — no duplicate")
		got := scanRows(t, ctx, final)
		require.Len(t, got, 1)
		assert.Contains(t, []string{"A", "B"}, got[1], "the surviving value must be one of the two writers'")
		assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
		assertAllManifestsData(t, ctx, final)

		assert.Equal(t, 2, countSnapshotsWithCommitID(final), "each mutation committed exactly once")
		assert.Equal(t, 3, occ.callCount()-base,
			"one clean commit + one conflicted-then-retried commit = 3 catalog applications")
	})
}

// gatedCatalog wraps a table.CatalogIO so its FIRST CommitTable call parks: it
// closes entered (signalling the caller has staged its files and reached the
// commit), then blocks until release is closed. Subsequent calls pass straight
// through. It lets a test freeze one committer at the exact written-but-not-
// yet-committed point while another committer runs to completion.
type gatedCatalog struct {
	inner   table.CatalogIO
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedCatalog(inner table.CatalogIO) *gatedCatalog {
	return &gatedCatalog{inner: inner, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedCatalog) LoadTable(ctx context.Context, ident table.Identifier) (*table.Table, error) {
	return g.inner.LoadTable(ctx, ident)
}

func (g *gatedCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
	return g.inner.CommitTable(ctx, ident, reqs, updates)
}

// parquetSet returns the set of .parquet paths under dir (recursively).
func parquetSet(t testing.TB, dir string) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	require.NoError(t, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			out[p] = struct{}{}
		}
		return nil
	}))
	return out
}

// waitClosed asserts ch closes within a generous timeout, failing with msg.
func waitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out: " + msg)
	}
}

// waitWrite asserts a writer goroutine finishes without error within a
// generous timeout.
func waitWrite(t *testing.T, ch <-chan error, msg string) {
	t.Helper()
	select {
	case err := <-ch:
		require.NoError(t, err, msg)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out: " + msg)
	}
}

// TestCOWConcurrentCleanupSparesInflightWriterFiles reproduces the reviewer
// scenario for authorship-tracked orphan cleanup with two real committers on
// one OCC-enforcing catalog: committer B has WRITTEN its copy-on-write data
// files but not yet committed while committer A runs a retried-success cleanup.
// Under the old directory-diff design B's files (a) appeared after A's
// before-listing and (b) were referenced by no snapshot, so A deleted them and
// B's commit would have landed referencing dead files. Authorship tracking must
// leave them untouched — A never authored them — and B must subsequently
// succeed with the correct final state.
//
// The interleaving is fully gated (no timing dependence):
//  1. A is anchored at the seed snapshot; main is then advanced so A's first
//     attempt is guaranteed a clean OCC conflict (forcing retried success —
//     the cleanup trigger).
//  2. A stages attempt 1 (writing files) and parks at its first CommitTable.
//  3. B (anchored post-advance) stages and parks at its first CommitTable —
//     B's files now exist on disk, committed by nobody, having appeared after
//     A's commit began.
//  4. A is released: conflict -> reload -> restage -> success, cleanup runs.
//     B's staged files must survive it.
//  5. B is released: clean conflict against A's commit -> reload -> restage ->
//     success. B's own cleanup then reclaims B's superseded attempt-1 files.
func TestCOWConcurrentCleanupSparesInflightWriterFiles(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	logger := service.MockResources().Logger()
	occ := newOCCTable(t, sc)

	// Seed id=1,2 in one data file so both mutations rewrite the shared file.
	_ = appendCOWRows(t, ctx, occ.snapshot(), map[int64]string{1: "one", 2: "two"})

	mkWriter := func(cat table.CatalogIO, anchor *table.Table) *writer {
		comm, err := NewCommitter(anchor, cat, CommitConfig{MaxRetries: 10},
			func(context.Context) (*table.Table, error) { return occ.snapshot(), nil }, logger)
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, anchor, "id")
		w.committer = comm
		return w
	}

	gateA := newGatedCatalog(occ)
	wA := mkWriter(gateA, occ.snapshot()) // anchored at the seed snapshot

	// Advance main AFTER A is anchored so A's first attempt is a guaranteed
	// clean conflict, deterministically driving the retried-success cleanup.
	_ = appendCOWRows(t, ctx, occ.snapshot(), map[int64]string{9: "advance"})

	gateB := newGatedCatalog(occ)
	wB := mkWriter(gateB, occ.snapshot()) // anchored post-advance

	// 2. A stages attempt 1 and parks at its first CommitTable.
	aDone := make(chan error, 1)
	go func() {
		aDone <- wA.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "A"})})
	}()
	waitClosed(t, gateA.entered, "A must stage and reach its first CommitTable")

	// 3. With A parked, snapshot the data dir, then let B stage and park too:
	// the files that appear in between are exactly B's staged-uncommitted files.
	preB := parquetSet(t, occ.location)
	bDone := make(chan error, 1)
	go func() {
		bDone <- wB.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "B"})})
	}()
	waitClosed(t, gateB.entered, "B must stage and reach its first CommitTable")

	bFiles := map[string]struct{}{}
	for p := range parquetSet(t, occ.location) {
		if _, ok := preB[p]; !ok {
			bFiles[p] = struct{}{}
		}
	}
	require.NotEmpty(t, bFiles, "B must have written data files before its commit")

	// 4. Release A: conflict -> reload -> restage -> success, cleanup runs.
	close(gateA.release)
	waitWrite(t, aDone, "A's retried commit must succeed")

	for p := range bFiles {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr,
			"B's written-but-uncommitted file must survive A's cleanup (A did not author it): %s", p)
	}

	// 5. Release B: it conflicts against A's commit, reloads, restages, and
	// must land with the correct final state.
	close(gateB.release)
	waitWrite(t, bDone, "B's commit must succeed after A")

	final := occ.snapshot()
	assert.Equal(t, map[int64]string{1: "A", 2: "B", 9: "advance"}, scanRows(t, ctx, final),
		"both mutations must land; neither may be lost or corrupted")
	assert.Equal(t, 3, countTableRows(t, ctx, final), "no duplicate rows")
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assert.Equal(t, 2, countSnapshotsWithCommitID(final), "each mutation committed exactly once")

	// B's superseded attempt-1 files are B's OWN recorded orphans: B's
	// retried-success cleanup must have reclaimed them.
	for p := range bFiles {
		_, statErr := os.Stat(p)
		assert.True(t, os.IsNotExist(statErr),
			"B's superseded attempt-1 file must be reclaimed by B's own cleanup: %s", p)
	}
}
