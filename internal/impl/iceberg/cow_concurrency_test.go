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
	"path/filepath"
	"sync"
	"testing"

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
