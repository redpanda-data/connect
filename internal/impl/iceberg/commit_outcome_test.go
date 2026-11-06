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
//     update must not wedge the output, and the manifest-merge property
//     contract (stamped when absent, kept when present) holds.

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

func manifestCount(tb testing.TB, ctx context.Context, tbl *table.Table) int {
	tb.Helper()
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0
	}
	fsys, err := tbl.FS(ctx)
	require.NoError(tb, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(tb, err)
	return len(manifests)
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

// TestManifestMergeBoundsManifestCount proves commit.manifest_merge_enabled
// has its documented effect ("Merge small manifest files during commits to
// reduce metadata overhead"): with it enabled, a table that has taken many
// single-file appends carries fewer manifests than appends; with it disabled,
// exactly one manifest per append. iceberg-go's merge threshold is
// commit.manifest.min-count-to-merge (default 100), so the run goes past it.
// Asserting on the table rather than on the chosen append strategy means a
// setting iceberg-go never reads fails here, whatever the plumbing looks like.
func TestManifestMergeBoundsManifestCount(t *testing.T) {
	const appends = 110

	t.Run("disabled keeps one manifest per append", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: false})
		for range appends {
			require.NoError(t, appendOne(t, ctx, c))
		}
		require.Len(t, cat.meta.Snapshots(), appends, "sequential appends must not coalesce")
		assert.Equal(t, appends, manifestCount(t, ctx, cat.snapshot()))
	})

	t.Run("enabled merges manifests past the threshold", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true})
		for range appends {
			require.NoError(t, appendOne(t, ctx, c))
		}
		require.Len(t, cat.meta.Snapshots(), appends, "sequential appends must not coalesce")
		got := manifestCount(t, ctx, cat.snapshot())
		assert.Less(t, got, appends,
			"manifest_merge_enabled: true must merge manifests once past min-count-to-merge; got %d manifests for %d appends", got, appends)
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
// retention window would retain, and the output's default-on
// max_snapshot_age. With working retention the metadata plateaus well under
// the ceiling and every commit succeeds. With inert retention it grows
// linearly and the catalog rejects the table permanently part-way through.
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
// on such a catalog just because retention is on by default.
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

// TestManifestMergePropertyStampedWhenAbsentKeptWhenPresent pins the
// contract of commit.manifest_merge_enabled: it reaches the table as the
// standard commit.manifest-merge.enabled property when absent, an existing
// value on the table wins over the configured one, and neither it nor the
// retention value is written into snapshot summaries (where they were inert).
func TestManifestMergePropertyStampedWhenAbsentKeptWhenPresent(t *testing.T) {
	t.Run("absent on the table: stamped with the configured value on first commit", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, MaxSnapshotAge: time.Hour})
		require.NoError(t, appendOne(t, ctx, c))

		assert.Equal(t, "true", cat.meta.Properties()[table.ManifestMergeEnabledKey])
		snap := cat.meta.CurrentSnapshot()
		require.NotNil(t, snap)
		require.NotNil(t, snap.Summary)
		assert.NotContains(t, snap.Summary.Properties, table.ManifestMergeEnabledKey, "manifest merge is a table property, not a summary entry")
		assert.NotContains(t, snap.Summary.Properties, table.MaxSnapshotAgeMsKey, "retention is applied by expiry, not recorded in the summary")
	})

	t.Run("absent and configured to the library default: nothing is written", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: table.ManifestMergeEnabledDefault})
		require.NoError(t, appendOne(t, ctx, c))
		assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey,
			"absent already means the default; a set-properties update would change nothing")
	})

	t.Run("present on the table: the table's value is kept", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		tx := cat.snapshot().NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{table.ManifestMergeEnabledKey: "false"}))
		_, err := tx.Commit(ctx)
		require.NoError(t, err)

		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true})
		require.NoError(t, appendOne(t, ctx, c))
		assert.Equal(t, "false", cat.meta.Properties()[table.ManifestMergeEnabledKey],
			"an operator's explicit table property must not be overridden")
	})
}

// mergePropRejectingCatalog models a catalog that refuses the
// commit.manifest-merge.enabled table property with a 400 whose text does
// NOT name the key, so the prohibited-keys stripper cannot learn it. Every
// other commit succeeds.
type mergePropRejectingCatalog struct {
	*memCatalog
	rejected int
}

func (c *mergePropRejectingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, u := range updates {
		if u.Action() == table.UpdateSetProperties {
			c.rejected++
			return nil, "", fmt.Errorf("%w: table property update not allowed by this catalog", rest.ErrBadRequest)
		}
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *mergePropRejectingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestMergePropertyRejectedByCatalogFallsBackOnce: a catalog that refuses the
// manifest-merge set-properties update without naming the key costs one
// extra attempt, the data still lands, and the property is never staged
// again for the committer. Retention is unaffected.
func TestMergePropertyRejectedByCatalogFallsBackOnce(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &mergePropRejectingCatalog{memCatalog: mem}
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, MaxSnapshotAge: time.Hour})

	// Retention is on but the table is empty, so no expiry is requested and
	// the only extra on the first attempt is the merge property: one
	// rejection, then the trial without it lands.
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "first attempt rejected; the drop-merge trial lands")
	assert.True(t, c.unsupported.refusesMergeProp())
	assert.False(t, c.unsupported.refusesExpiry(), "expiry was never the culprit")
	assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey)

	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "the property must not be staged again")
	assert.Len(t, cat.meta.Snapshots(), 2)
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
// an error without marking either extra unsupported, since neither trial
// lands. Guards against a wedged table (the Glue ceiling) silently switching
// retention off on top of everything else.
func TestUnrelatedRejectionDoesNotDisableExtras(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	fc := &flakyCatalog{memCatalog: mem, failuresLeft: 1 << 30, failErr: fmt.Errorf("rejected: %w", rest.ErrBadRequest)}
	c := newOutcomeCommitter(t, fc, fc.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, MaxSnapshotAge: time.Hour})

	err := appendOne(t, ctx, c)
	require.ErrorIs(t, err, rest.ErrBadRequest)
	assert.False(t, c.unsupported.refusesExpiry())
	assert.False(t, c.unsupported.refusesMergeProp())
	assert.Equal(t, 2, fc.calls(), "one attempt, then one trial without the merge property; nothing was expirable so expiry was never requested or blamed")
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

// TestMergePropertyNotStagedOnceStripperKnowsIt: when the catalog has named
// commit.manifest-merge.enabled as prohibited (the stripper has learned it),
// the property must not be staged at all. Staging it would make iceberg-go
// merge manifests locally while the stripper removes the update, so the
// table would merge without the property ever persisting.
func TestMergePropertyNotStagedOnceStripperKnowsIt(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &mergePropRejectingCatalog{memCatalog: mem}
	known := newProhibitedKeySet()
	known.add(table.ManifestMergeEnabledKey)
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, ProhibitedKeys: known})

	for range 3 {
		require.NoError(t, appendOne(t, ctx, c))
	}
	assert.Zero(t, cat.rejected, "no set-properties update may reach the catalog")
	assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey)
	assert.Equal(t, 3, manifestCount(t, ctx, cat.snapshot()), "without the property the table keeps one manifest per commit")
}

// TestExpiryWithNothingRemovableIsNotBlamedForRejection: a tag pins the only
// old snapshot, so expiry is requested (something is older than the age) but
// removes nothing. An unrelated 400 must then not trigger the drop-expiry
// trial: the request body would be identical, the trial could never land,
// and every failing commit would pay an extra round trip for nothing.
func TestExpiryWithNothingRemovableIsNotBlamedForRejection(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	fc := &flakyCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond

	// Seed one snapshot and tag it BEFORE the committer exists, so the
	// committer's own table handle sees the pin.
	seedTable(t, ctx, fc.snapshot(), 1)
	pinned := mem.meta.CurrentSnapshot().SnapshotID
	meta, err := table.UpdateTableMetadata(mem.meta,
		[]table.Update{table.NewSetSnapshotRefUpdate("keep", pinned, table.TagRef, 0, 0, 0)}, "")
	require.NoError(t, err)
	mem.meta = meta
	time.Sleep(age + 100*time.Millisecond)

	// Merge left at the library default so no property is staged: expiry is
	// the only extra, it is requested (the seed is older than the age) and
	// removes nothing (the tag pins it). Every commit is rejected.
	c := newOutcomeCommitter(t, fc, fc.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	fc.failuresLeft, fc.failErr = 1<<30, fmt.Errorf("rejected: %w", rest.ErrBadRequest)
	before := fc.calls() // the seed commit went through fc too
	err = appendOne(t, ctx, c)
	require.ErrorIs(t, err, rest.ErrBadRequest)
	assert.Equal(t, 1, fc.calls()-before, "no trial may run when expiry removed nothing")
	assert.False(t, c.unsupported.refusesExpiry())
}

// namedProhibitedKeyCatalog models Unity Catalog: it rejects a commit whose
// set-properties names a forbidden key, and says which key, so the stripper
// can learn it.
type namedProhibitedKeyCatalog struct {
	*memCatalog
	rejected int
}

func (c *namedProhibitedKeyCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, u := range updates {
		if u.Action() == table.UpdateSetProperties {
			c.rejected++
			return nil, "", fmt.Errorf("%w: Table properties contain prohibited keys: %s", rest.ErrBadRequest, table.ManifestMergeEnabledKey)
		}
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *namedProhibitedKeyCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestProhibitedKeyRetryDoesNotSpendRetryBudget: the learn-and-strip retry,
// like the elimination trials, must not consume a configured attempt. With
// max_retries: 1 against a catalog that names the merge property as
// prohibited, the data must still land on the stripped retry.
func TestProhibitedKeyRetryDoesNotSpendRetryBudget(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &namedProhibitedKeyCatalog{memCatalog: mem}
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true})

	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "one named rejection, then the stripped retry lands")
	assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey)
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, cat.rejected, "the learned key is never sent again")
	assert.Len(t, cat.meta.Snapshots(), 2)
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
