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
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file holds outcome-level tests for the committer: they assert what the
// TABLE looks like after a sequence of commits (how many snapshots it carries,
// how many manifests, which objects exist under its prefix, whether the
// catalog accepted it) rather than which iceberg-go call was made with which
// argument. The four groups are:
//
//   - knob outcomes: every CommitConfig field has an observable effect on the
//     table, so an inert field fails here even when it is faithfully plumbed;
//   - the whole-prefix orphan invariant: after any commit sequence, every
//     object under the table location is reachable from current metadata;
//   - catalog ceiling emulation: a catalog that rejects commits once the
//     serialised metadata exceeds a byte limit, modelling engine-backed
//     catalogs (AWS Glue caps table metadata at 50 MB) so that unbounded
//     metadata growth is reproducible without cloud credentials;
//   - catalog compatibility: a catalog that refuses the remove-snapshots
//     update must not wedge the output.

// --- helpers ---------------------------------------------------------------

// referencedPaths returns every object path reachable from the table's current
// metadata: each snapshot's manifest list, every manifest it names, and every
// data/delete file those manifests carry (deleted entries included, since the
// manifest still references them). Metadata JSON files are not included: the
// in-memory catalogs here never write them.
func referencedPaths(tb testing.TB, ctx context.Context, tbl *table.Table) map[string]struct{} {
	tb.Helper()
	fsys, err := tbl.FS(ctx)
	require.NoError(tb, err)
	refs := map[string]struct{}{}
	for _, snap := range tbl.Metadata().Snapshots() {
		refs[snap.ManifestList] = struct{}{}
		manifests, err := snap.Manifests(fsys)
		require.NoError(tb, err, "reading manifest list %s", snap.ManifestList)
		for _, m := range manifests {
			refs[m.FilePath()] = struct{}{}
			for entry, err := range m.Entries(fsys, false) {
				require.NoError(tb, err)
				refs[entry.DataFile().FilePath()] = struct{}{}
			}
		}
	}
	return refs
}

// tableObjects lists every regular file under the table location, as
// slash-separated absolute paths matching how iceberg-go records them for a
// LocalFS-backed table. Metadata JSON files are skipped: they are the
// catalog's, not the writer's, and the in-memory catalogs never write them.
func tableObjects(tb testing.TB, location string) map[string]struct{} {
	tb.Helper()
	out := map[string]struct{}{}
	err := filepath.WalkDir(filepath.FromSlash(location), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(path, ".metadata.json") {
			return nil
		}
		out[filepath.ToSlash(path)] = struct{}{}
		return nil
	})
	require.NoError(tb, err)
	return out
}

// orphans returns the objects under the table location that no snapshot in
// the current metadata references.
func orphans(tb testing.TB, ctx context.Context, tbl *table.Table) []string {
	tb.Helper()
	refs := referencedPaths(tb, ctx, tbl)
	var out []string
	for p := range tableObjects(tb, tbl.Location()) {
		if _, ok := refs[p]; !ok {
			out = append(out, strings.TrimPrefix(p, tbl.Location()))
		}
	}
	return out
}

// assertNoOrphans is the whole-prefix invariant: every object under the table
// location is referenced by current metadata. It is the only check that sees
// files the committer never knew it wrote (manifests and manifest lists are
// written inside iceberg-go, not by the writer).
func assertNoOrphans(tb testing.TB, ctx context.Context, tbl *table.Table, msg string) {
	tb.Helper()
	assert.Empty(tb, orphans(tb, ctx, tbl), msg)
}

func metadataBytes(tb testing.TB, meta table.Metadata) int {
	tb.Helper()
	b, err := json.Marshal(meta)
	require.NoError(tb, err)
	return len(b)
}

// appendOne commits a single synthetic data file through the committer.
func appendOne(tb testing.TB, ctx context.Context, c *committer) error {
	tb.Helper()
	df := synthDataFile(tb, c.table.Spec(), fmt.Sprintf("%s/data/%s.parquet", c.table.Location(), uuid.New()))
	return c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
}

func newOutcomeCommitter(tb testing.TB, cat table.CatalogIO, snapshot func() *table.Table, cfg CommitConfig) *committer {
	tb.Helper()
	c, err := NewCommitter(snapshot(), cat, cfg,
		func(context.Context) (*table.Table, error) { return snapshot(), nil },
		service.MockResources().Logger())
	require.NoError(tb, err)
	tb.Cleanup(c.Close)
	return c
}

// ceilingCatalog is a memCatalog that rejects any commit whose resulting
// serialised metadata would exceed ceilingBytes, with the 400-class error an
// engine-backed catalog returns. AWS Glue enforces exactly this shape of limit
// (50 MB of table metadata, surfaced over the Iceberg REST endpoint as
// "InvalidInputException: Prepared s3 payload size exceeds the size limit",
// an HTTP 400 that iceberg-go maps to rest.ErrBadRequest). A rejected commit
// applies nothing, matching the catalog semantics.
type ceilingCatalog struct {
	*memCatalog
	ceilingBytes int
}

func (c *ceilingCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	staged, err := table.UpdateTableMetadata(c.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(staged)
	if err != nil {
		return nil, "", err
	}
	if len(b) > c.ceilingBytes {
		return nil, "", fmt.Errorf("%w: InvalidInputException: Prepared s3 payload size exceeds the size limit (%d > %d bytes)", rest.ErrBadRequest, len(b), c.ceilingBytes)
	}
	c.meta = staged
	return staged, c.metadataLocation, nil
}

func (c *ceilingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// bytesPerSnapshot measures how much serialised metadata one append adds, by
// committing `n` appends with no ceiling and averaging the growth.
func bytesPerSnapshot(tb testing.TB, ctx context.Context, cat *ceilingCatalog, n int) int {
	tb.Helper()
	saved := cat.ceilingBytes
	cat.ceilingBytes = math.MaxInt
	defer func() { cat.ceilingBytes = saved }()
	c := newOutcomeCommitter(tb, cat, cat.snapshot, CommitConfig{MaxRetries: 1})
	before := metadataBytes(tb, cat.meta)
	for range n {
		require.NoError(tb, appendOne(tb, ctx, c))
	}
	c.Close()
	per := (metadataBytes(tb, cat.meta) - before) / n
	require.Positive(tb, per)
	return per
}

// --- 1. knob outcomes ------------------------------------------------------

// TestMaxSnapshotAgeExpiresSnapshots proves commit.max_snapshot_age has its
// documented effect: snapshots older than the configured age are removed
// from table metadata by the output's own commits. It deliberately asserts
// the table state, not where the value is written: a retention value stamped
// into a snapshot summary, or set as a table property that nothing acts on,
// both fail here. The two subtests pin the field's two documented
// behaviours: snapshots older than the age are expired, and zero disables
// expiry entirely.
func TestMaxSnapshotAgeExpiresSnapshots(t *testing.T) {
	const age = 300 * time.Millisecond

	t.Run("snapshots older than max_snapshot_age are expired", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})

		const old = 5
		for range old {
			require.NoError(t, appendOne(t, ctx, c))
		}
		oldest := cat.meta.Snapshots()[0].SnapshotID
		require.Len(t, cat.meta.Snapshots(), old)

		// Let every snapshot so far age past the retention horizon, then
		// commit once more. Iceberg's min-snapshots-to-keep default (1) may
		// retain the previous head regardless of age, so the bound is 2, not
		// 1: the new snapshot plus at most one retained ancestor.
		time.Sleep(age + 200*time.Millisecond)
		require.NoError(t, appendOne(t, ctx, c))

		snaps := cat.meta.Snapshots()
		assert.LessOrEqual(t, len(snaps), 2,
			"after a commit, snapshots older than max_snapshot_age must be gone; got %d snapshots", len(snaps))
		for _, s := range snaps {
			assert.NotEqual(t, oldest, s.SnapshotID, "the oldest snapshot must have been expired")
		}
		head := cat.meta.CurrentSnapshot()
		require.NotNil(t, head)
		require.NotNil(t, head.Summary)
		assert.NotContains(t, head.Summary.Properties, table.MaxSnapshotAgeMsKey, "retention is applied by expiry, not recorded in the summary")
	})

	t.Run("zero disables expiry", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: 0})

		for range 3 {
			require.NoError(t, appendOne(t, ctx, c))
		}
		time.Sleep(age + 200*time.Millisecond)
		require.NoError(t, appendOne(t, ctx, c))
		assert.Len(t, cat.meta.Snapshots(), 4, "max_snapshot_age: 0 must retain every snapshot")
	})
}

// --- 2. whole-prefix orphan invariant -------------------------------------

// TestSuccessfulAppendsLeaveNoOrphans is the control for the invariant: a
// clean run of appends must satisfy it, otherwise the failure-path tests
// below prove nothing.
func TestSuccessfulAppendsLeaveNoOrphans(t *testing.T) {
	ctx := t.Context()
	_, cat := newTestTable(t)
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1})
	for range 5 {
		require.NoError(t, appendOne(t, ctx, c))
	}
	assertNoOrphans(t, ctx, cat.snapshot(), "a clean run of appends must leave only referenced objects")
}

// --- 3. catalog ceiling emulation -----------------------------------------

// TestRetentionKeepsMetadataUnderCatalogCeiling reproduces the incident
// shape end to end, without a cloud catalog: a catalog with a hard metadata
// size limit, a writer committing continuously at a rate far above what the
// retention window would retain, and max_snapshot_age set. With working
// retention the metadata plateaus well under the ceiling and every commit
// succeeds. With inert retention it grows linearly and the catalog rejects
// the table permanently part-way through.
//
// The ceiling is sized from a measurement rather than a constant so the test
// does not depend on the exact serialised size of a snapshot: it admits about
// twenty snapshots' worth of growth, and the run commits sixty while
// retention should hold the live set to a handful.
func TestRetentionKeepsMetadataUnderCatalogCeiling(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &ceilingCatalog{memCatalog: mem}

	const measure = 3
	per := bytesPerSnapshot(t, ctx, cat, measure)
	cat.ceilingBytes = metadataBytes(t, cat.meta) + 20*per

	const (
		age      = 200 * time.Millisecond
		interval = 40 * time.Millisecond
		commits  = 60
	)
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	for i := range commits {
		if err := appendOne(t, ctx, c); err != nil {
			t.Fatalf("commit %d rejected with %d snapshots in metadata (%d bytes, ceiling %d): %v\n"+
				"max_snapshot_age=%s at one commit per %s should hold the live set to a handful of snapshots",
				i+1+measure, len(cat.meta.Snapshots()), metadataBytes(t, cat.meta), cat.ceilingBytes, err, age, interval)
		}
		time.Sleep(interval)
	}
	t.Logf("final metadata: %d snapshots, %d bytes (ceiling %d)", len(cat.meta.Snapshots()), metadataBytes(t, cat.meta), cat.ceilingBytes)
}

// --- 4. catalog compatibility ---------------------------------------------

// expiryRejectingCatalog models a catalog that does not implement the
// remove-snapshots table update: a commit carrying one is rejected with a 400
// and applies nothing; every other commit succeeds. The output must not wedge
// on such a catalog just because retention is on.
type expiryRejectingCatalog struct {
	*memCatalog
	rejected int
}

func (c *expiryRejectingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, u := range updates {
		if u.Action() == table.UpdateRemoveSnapshots {
			c.rejected++
			return nil, "", fmt.Errorf("%w: unsupported table update: %s", rest.ErrBadRequest, table.UpdateRemoveSnapshots)
		}
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *expiryRejectingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestExpiryRejectedByCatalogFallsBackOnce: the first commit the catalog
// rejects for carrying expiry is retried once without it; when that lands,
// expiry is disabled for the committer and never staged again, so a
// non-conforming catalog costs exactly one extra round trip for the lifetime
// of the table handle, and no commit is ever lost to it.
func TestExpiryRejectedByCatalogFallsBackOnce(t *testing.T) {
	// The trial must not spend the configured retry budget: max_retries: 1
	// is a supported setting and a refusing catalog must still get the data.
	for _, maxRetries := range []int{1, 3} {
		t.Run(fmt.Sprintf("max_retries=%d", maxRetries), func(t *testing.T) {
			testExpiryRejectedFallsBack(t, maxRetries)
		})
	}
}

func testExpiryRejectedFallsBack(t *testing.T, maxRetries int) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &expiryRejectingCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: maxRetries, MaxSnapshotAge: age})

	// Nothing is old enough to expire yet, so no remove-snapshots update is
	// staged and the commit is accepted as-is.
	require.NoError(t, appendOne(t, ctx, c))
	require.Zero(t, cat.rejected)
	require.False(t, c.unsupported.refusesExpiry())

	// Now the first snapshot is expirable: expiry is staged, rejected, and
	// the commit must still land on the retry without it.
	time.Sleep(age + 100*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "exactly one rejected attempt")
	assert.True(t, c.unsupported.refusesExpiry(), "a proven rejection must record the refusal")
	assert.Len(t, cat.meta.Snapshots(), 2, "the data commit must have landed")

	// Expiry stays off: no further rejections, snapshots accumulate.
	time.Sleep(age + 100*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "expiry must not be staged again once disabled")
	assert.Len(t, cat.meta.Snapshots(), 3)
}

// TestConflictWithExpiryStagedIsNotMistakenForRejectedExpiry: a clean 409
// while expiry happens to be staged is an ordinary conflict. It must take
// the reload-and-retry path, keep expiry enabled, and the retry must still
// expire what it should.
func TestConflictWithExpiryStagedIsNotMistakenForRejectedExpiry(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age})

	require.NoError(t, appendOne(t, ctx, c))
	time.Sleep(age + 100*time.Millisecond)

	cat.outcomes, cat.calls = []commitOutcome{commitConflict}, 0
	require.NoError(t, appendOne(t, ctx, c))
	require.Equal(t, 2, cat.calls, "one conflict, one success")
	assert.False(t, c.unsupported.refusesExpiry(), "a 409 must not be read as the catalog refusing expiry")
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the retried commit must still expire the aged snapshot")
}

// TestTrialLandingAmbiguouslyIsInconclusive: the attempt that drops expiry
// gets a lost response (5xx) but actually landed; the reload finds the
// commit-id token and returns success. The data is safe, but the token
// cannot prove WHICH attempt landed (an earlier ambiguous attempt may have
// carried expiry), so no verdict is recorded and the next commit tries
// expiry again.
func TestTrialLandingAmbiguouslyIsInconclusive(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age})

	require.NoError(t, appendOne(t, ctx, c))
	time.Sleep(age + 100*time.Millisecond)

	cat.outcomes, cat.calls = []commitOutcome{commitBadRequest, commitLandThenUnknown}, 0
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 2, cat.calls)
	assert.False(t, c.unsupported.refusesExpiry(), "an ambiguous trial proves nothing about expiry")
	assert.Len(t, cat.meta.Snapshots(), 2)
}

// TestUnrelatedRejectionDoesNotDisableExtras: a 400 that has nothing to do
// with the optional updates (here, every commit is rejected) must surface as
// an error without marking expiry unsupported, since the trial does not land.
// Guards against a wedged table (the Glue ceiling) silently switching
// retention off on top of everything else.
func TestUnrelatedRejectionDoesNotDisableExtras(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	fc := &flakyCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	c := newOutcomeCommitter(t, fc, fc.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	require.NoError(t, appendOne(t, ctx, c))
	time.Sleep(age + 100*time.Millisecond)

	fc.failuresLeft, fc.failErr = 1<<30, fmt.Errorf("rejected: %w", rest.ErrBadRequest)
	before := fc.calls()
	err := appendOne(t, ctx, c)
	require.ErrorIs(t, err, rest.ErrBadRequest)
	assert.False(t, c.unsupported.refusesExpiry())
	assert.Equal(t, 2, fc.calls()-before, "one attempt with expiry, then one trial without it")
}

// TestUnsupportedVerdictSurvivesCommitterRecreation: the router recreates
// the committer whenever the writer is recreated (schema evolution, error
// paths), so a verdict held only on the committer would be re-learned, at
// the cost of a rejected round trip and a warning, every generation. Sharing
// it through CommitConfig.UnsupportedUpdates (as the router does per table)
// makes the second generation start from the verdict.
func TestUnsupportedVerdictSurvivesCommitterRecreation(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &expiryRejectingCatalog{memCatalog: mem}
	shared := newUnsupportedUpdates()
	const age = 100 * time.Millisecond
	cfg := CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, UnsupportedUpdates: shared}

	first := newOutcomeCommitter(t, cat, cat.snapshot, cfg)
	require.NoError(t, appendOne(t, ctx, first))
	time.Sleep(age + 100*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, first))
	require.Equal(t, 1, cat.rejected)
	require.True(t, shared.refusesExpiry())
	first.Close()

	second := newOutcomeCommitter(t, cat, cat.snapshot, cfg)
	time.Sleep(age + 100*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, second))
	assert.Equal(t, 1, cat.rejected, "the new committer generation must not re-learn the verdict")
	assert.Len(t, cat.meta.Snapshots(), 3)
}

// TestRefusalVerdictIsReprobed: a refusal stands for
// commit.unsupported_update_retry_interval, then the update is tried once
// more. A genuinely refusing catalog pays one rejected round trip per
// interval; a mis-verdict heals itself.
func TestRefusalVerdictIsReprobed(t *testing.T) {
	const interval = 150 * time.Millisecond

	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &expiryRejectingCatalog{memCatalog: mem}
	const age = 50 * time.Millisecond
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, UnsupportedUpdateRetryInterval: interval})

	require.NoError(t, appendOne(t, ctx, c))
	time.Sleep(age + 50*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, c))
	require.Equal(t, 1, cat.rejected)
	require.True(t, c.unsupported.refusesExpiry())

	time.Sleep(interval + 50*time.Millisecond)
	require.False(t, c.unsupported.refusesExpiry(), "the verdict must lapse")
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 2, cat.rejected, "expiry is re-probed once the verdict lapses")
	assert.True(t, c.unsupported.refusesExpiry(), "and the refusal is recorded again")
}

// TestExpiryWithNothingRemovableIsNotBlamedForRejection: main's
// min-snapshots-to-keep retains the only old snapshot, so expiry is requested
// (something is older than the age) but removes nothing. An unrelated 400
// must then not trigger the drop-expiry trial: the request body would be
// identical, the trial could never land, and every failing commit would pay
// an extra round trip for nothing.
func TestExpiryWithNothingRemovableIsNotBlamedForRejection(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	fc := &flakyCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond

	// Seed one snapshot and set the retention BEFORE the committer exists,
	// so the committer's own table handle sees it.
	seedTable(t, ctx, fc.snapshot(), 1)
	head := mem.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(mem.meta,
		[]table.Update{table.NewSetSnapshotRefUpdate(table.MainBranch, head, table.BranchRef, 0, 0, 10 /* min-snapshots-to-keep */)}, "")
	require.NoError(t, err)
	mem.meta = meta
	time.Sleep(age + 100*time.Millisecond)

	// Expiry is the only extra. It is requested (the seed is older than the
	// age) and removes nothing (min-snapshots-to-keep). Every commit is
	// rejected.
	c := newOutcomeCommitter(t, fc, fc.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	fc.failuresLeft, fc.failErr = 1<<30, fmt.Errorf("rejected: %w", rest.ErrBadRequest)
	before := fc.calls() // the seed commit went through fc too
	err = appendOne(t, ctx, c)
	require.ErrorIs(t, err, rest.ErrBadRequest)
	assert.Equal(t, 1, fc.calls()-before, "no trial may run when expiry removed nothing")
	assert.False(t, c.unsupported.refusesExpiry())
}

// TestElapsedRefAgeIsExpiredEvenWhenNoSnapshotIsOld pins the documented
// promise that a tag whose own max-ref-age-ms has elapsed is removed, which
// requires the expiry pre-check to look at refs and not only at snapshot
// ages: here every snapshot is far younger than max_snapshot_age.
func TestElapsedRefAgeIsExpiredEvenWhenNoSnapshotIsOld(t *testing.T) {
	ctx := t.Context()
	_, cat := newTestTable(t)
	seedTable(t, ctx, cat.snapshot(), 1)
	pinned := cat.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(cat.meta,
		[]table.Update{table.NewSetSnapshotRefUpdate("short-lived", pinned, table.TagRef, 1 /* max-ref-age-ms */, 0, 0)}, "")
	require.NoError(t, err)
	cat.meta = meta
	time.Sleep(50 * time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: time.Hour})
	require.NoError(t, appendOne(t, ctx, c))

	refs := map[string]struct{}{}
	for name := range cat.meta.Refs() {
		refs[name] = struct{}{}
	}
	assert.NotContains(t, refs, "short-lived", "a tag past its own max-ref-age-ms must be removed")
	assert.Len(t, cat.meta.Snapshots(), 2, "the young snapshot it pointed at is kept")
}

// TestExpiredRefWithPinnedSnapshotIsStillBlamedForRejection: a tag past its
// own max-ref-age-ms points at a snapshot another tag pins, so expiry stages
// a remove-snapshot-ref update and no remove-snapshots. A catalog that 400s
// that update must still get the drop-expiry trial, or the table wedges on
// exactly the kind of 400 the elimination logic exists to rule out.
func TestExpiredRefWithPinnedSnapshotIsStillBlamedForRejection(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	fc := &flakyCatalog{memCatalog: mem}

	seedTable(t, ctx, fc.snapshot(), 1)
	pinned := mem.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(mem.meta, []table.Update{
		table.NewSetSnapshotRefUpdate("keep", pinned, table.TagRef, 0, 0, 0),
		table.NewSetSnapshotRefUpdate("stale", pinned, table.TagRef, 1 /* max-ref-age-ms */, 0, 0),
	}, "")
	require.NoError(t, err)
	mem.meta = meta
	time.Sleep(50 * time.Millisecond)

	// Every snapshot is far younger than the age: only the ref removal is
	// staged. Every commit is rejected.
	c := newOutcomeCommitter(t, fc, fc.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: time.Hour})
	fc.failuresLeft, fc.failErr = 1<<30, fmt.Errorf("rejected: %w", rest.ErrBadRequest)
	before := fc.calls()
	err = appendOne(t, ctx, c)
	require.ErrorIs(t, err, rest.ErrBadRequest)
	assert.Equal(t, 2, fc.calls()-before, "one attempt with the ref removal, then one trial without it")
	assert.False(t, c.unsupported.refusesExpiry(), "the trial did not land, so no verdict")
}

// TestTrialVerdictSurvivesConflictBeforeLanding: the trial attempt hits a
// clean 409 and the reload-and-retry lands without expiry. Every outcome
// since the trial began was definitive and the extra stayed dropped, so the
// landing proves the refusal just as the trial itself would have. Without
// this a busy multi-writer table on a refusing catalog would pay the
// rejected round trip on every commit and never record the verdict.
func TestTrialVerdictSurvivesConflictBeforeLanding(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age})

	require.NoError(t, appendOne(t, ctx, c))
	time.Sleep(age + 100*time.Millisecond)

	cat.outcomes, cat.calls = []commitOutcome{commitBadRequest, commitConflict}, 0
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 3, cat.calls, "400 with expiry, 409 without, then it lands")
	assert.True(t, c.unsupported.refusesExpiry(), "a definitive detour does not spoil the verdict")
	assert.Len(t, cat.meta.Snapshots(), 2)
}

// TestBranchOwnSnapshotAgeWinsOverConfiguredAge pins the documented promise
// that a branch's own retention properties apply where set. iceberg-go
// prefers a branch's max-snapshot-age-ms over the configured age, so the
// expiry pre-check must consider it too, or expiry is never even requested
// while every snapshot is younger than max_snapshot_age.
func TestBranchOwnSnapshotAgeWinsOverConfiguredAge(t *testing.T) {
	ctx := t.Context()
	_, cat := newTestTable(t)
	seedTable(t, ctx, cat.snapshot(), 2)
	head := cat.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(cat.meta,
		[]table.Update{table.NewSetSnapshotRefUpdate(table.MainBranch, head, table.BranchRef, 0, 1 /* max-snapshot-age-ms */, 1 /* min-snapshots-to-keep */)}, "")
	require.NoError(t, err)
	cat.meta = meta
	time.Sleep(50 * time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: time.Hour})
	require.NoError(t, appendOne(t, ctx, c))
	assert.Len(t, cat.meta.Snapshots(), 1, "main's own 1ms retention expires every snapshot but the new head")
}

// reqRecordingCatalog keeps every requirement the committer sends, so a test
// can tell whether ExpireSnapshots ran: it asserts the position of every
// ref on the table, the data commit only main's.
type reqRecordingCatalog struct {
	*memCatalog
	reqs []table.Requirement
}

func (c *reqRecordingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.reqs = append(c.reqs, reqs...)
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *reqRecordingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

func (c *reqRecordingCatalog) assertedRefs(t *testing.T) []string {
	t.Helper()
	var refs []string
	for _, r := range c.reqs {
		if r.GetType() != "assert-ref-snapshot-id" {
			continue
		}
		b, err := json.Marshal(r)
		require.NoError(t, err)
		var wire struct {
			Ref string `json:"ref"`
		}
		require.NoError(t, json.Unmarshal(b, &wire))
		refs = append(refs, wire.Ref)
	}
	return refs
}

// TestPinnedOldSnapshotDoesNotTriggerExpiry: a tag pins the only old
// snapshot. The head of a retained ref is always kept, so there is nothing
// expiry could do, and the pre-check must say so. Otherwise every commit to
// the table would run the retention walk and assert every ref's position,
// conflicting with any concurrent ref mover for no retention benefit.
func TestPinnedOldSnapshotDoesNotTriggerExpiry(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &reqRecordingCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond

	seedTable(t, ctx, cat.snapshot(), 1)
	pinned := mem.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(mem.meta,
		[]table.Update{table.NewSetSnapshotRefUpdate("keep", pinned, table.TagRef, 0, 0, 0)}, "")
	require.NoError(t, err)
	mem.meta = meta
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	cat.reqs = nil
	require.NoError(t, appendOne(t, ctx, c))
	assert.NotContains(t, cat.assertedRefs(t), "keep", "expiry must not be requested when only a ref head is old")
	assert.Len(t, cat.meta.Snapshots(), 2)
}

// TestTableRetentionPropertyWinsOverConfiguredAge: a table that carries
// history.expire.max-snapshot-age-ms keeps that retention, whichever way it
// differs from max_snapshot_age, mirroring how the table's
// commit.manifest-merge.enabled wins over manifest_merge_enabled.
func TestTableRetentionPropertyWinsOverConfiguredAge(t *testing.T) {
	seedWithTableAge := func(t *testing.T, ageMs int64) *memCatalog {
		ctx := t.Context()
		_, cat := newTestTable(t)
		seedTable(t, ctx, cat.snapshot(), 1)
		meta, err := table.UpdateTableMetadata(cat.meta,
			[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{table.MaxSnapshotAgeMsKey: strconv.FormatInt(ageMs, 10)})}, "")
		require.NoError(t, err)
		cat.meta = meta
		time.Sleep(150 * time.Millisecond)
		return cat
	}

	t.Run("longer table retention keeps what the field would expire", func(t *testing.T) {
		cat := seedWithTableAge(t, time.Hour.Milliseconds())
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: 100 * time.Millisecond})
		require.NoError(t, appendOne(t, t.Context(), c))
		assert.Len(t, cat.meta.Snapshots(), 2, "the table's own hour-long retention keeps the old snapshot")
	})

	t.Run("shorter table retention expires what the field would keep", func(t *testing.T) {
		cat := seedWithTableAge(t, 1)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: time.Hour})
		require.NoError(t, appendOne(t, t.Context(), c))
		assert.Len(t, cat.meta.Snapshots(), 1, "the table's own 1ms retention expires the old snapshot")
	})
}

// retentionConflictCatalog models a concurrent writer that changes the
// table's retention: its second call sets history.expire.max-snapshot-age-ms
// to an hour out of band and reports a clean 409. The first call is a 400;
// later calls behave normally.
type retentionConflictCatalog struct {
	*memCatalog
	calls int
}

func (c *retentionConflictCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	switch c.calls {
	case 1:
		return nil, "", fmt.Errorf("%w: rejected", rest.ErrBadRequest)
	case 2:
		meta, err := table.UpdateTableMetadata(c.meta, []table.Update{
			table.NewSetPropertiesUpdate(iceberg.Properties{table.MaxSnapshotAgeMsKey: strconv.FormatInt(time.Hour.Milliseconds(), 10)}),
		}, "")
		if err != nil {
			return nil, "", err
		}
		c.meta = meta
		return nil, "", rest.ErrCommitFailed
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *retentionConflictCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestTrialVerdictNotRecordedWhenConflictRemovesEligibility: the trial drops
// expiry, hits a 409, and the reload shows another writer gave the table its
// own hour-long retention meanwhile, so nothing is eligible any more. The
// landing that follows would not have carried expiry anyway, so it proves
// nothing about the catalog and no verdict is recorded.
func TestTrialVerdictNotRecordedWhenConflictRemovesEligibility(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &retentionConflictCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond

	// Seed through the plain in-memory catalog so the script starts with the
	// committer's first attempt.
	seedTable(t, ctx, tbl, 2)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age})
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 3, cat.calls, "400 with expiry, 409 while the retention changes, then it lands")
	assert.False(t, c.unsupported.refusesExpiry(), "the landing proves nothing once eligibility is gone")
	assert.Len(t, cat.meta.Snapshots(), 3, "the table's own retention keeps the seeds")
}
