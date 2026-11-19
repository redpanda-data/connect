// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
//   - maintenance commits: snapshot expiry and the manifest-merge property
//     land in their own commits after a data commit (table_maintenance.go),
//     retry conflicts, wait out the pass interval after a failure, and
//     never hold up or fail a data commit, whatever the catalog does.

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

// releasePass frees the pass slot, standing in for the interval elapsing.
func (s *tableMaintenanceState) releasePass() { s.nextPassAt.Store(0) }

func newOutcomeCommitter(tb testing.TB, cat table.CatalogIO, snapshot func() *table.Table, cfg CommitConfig) *committer {
	tb.Helper()
	return newLoggedOutcomeCommitter(tb, cat, snapshot, cfg, service.MockResources().Logger())
}

func newLoggedOutcomeCommitter(tb testing.TB, cat table.CatalogIO, snapshot func() *table.Table, cfg CommitConfig, logger *service.Logger) *committer {
	tb.Helper()
	// The production default pass interval (ten minutes) would let only the
	// first commit of a test run maintenance. Unless a test sets its own
	// interval, every commit is followed by a pass.
	if cfg.MaintenanceInterval == 0 {
		cfg.MaintenanceInterval = time.Nanosecond
	}
	c, err := NewCommitter(snapshot(), cat, cfg,
		func(context.Context) (*table.Table, error) { return snapshot(), nil },
		logger)
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
		assert.NotEmpty(t, orphans(t, ctx, cat.snapshot()),
			"expiry removes snapshots from metadata only; their manifests stay on storage for orphan-file maintenance (WithPostCommit(false))")
	})

	t.Run("zero disables expiry", func(t *testing.T) {
		// Zero age and the merge property off is the default config: every
		// snapshot is kept and the pass makes no catalog call, so the
		// default behaves as before the knobs took effect.
		ctx := t.Context()
		_, mem := newTestTable(t)
		cat := &scriptedCatalog{memCatalog: mem}
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: 0})

		for range 3 {
			require.NoError(t, appendOne(t, ctx, c))
		}
		time.Sleep(age + 200*time.Millisecond)
		require.NoError(t, appendOne(t, ctx, c))
		assert.Len(t, cat.meta.Snapshots(), 4, "max_snapshot_age: 0 must retain every snapshot")
		assert.Equal(t, 4, cat.calls, "four data commits and no maintenance commit")
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

// --- 4. maintenance commits -----------------------------------------------

// actionRecordingCatalog keeps, per CommitTable call, the actions of the
// updates it carried, so a test can tell a data commit from a maintenance
// commit and prove no call carried both.
type actionRecordingCatalog struct {
	*memCatalog
	calls [][]string
}

func (c *actionRecordingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	var actions []string
	for _, u := range updates {
		actions = append(actions, u.Action())
	}
	c.calls = append(c.calls, actions)
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (c *actionRecordingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestMaintenanceRunsAsItsOwnCommit pins the design: a data commit never
// carries the remove-snapshots or the set-properties update. Each lands in a
// commit of its own after the data commit, so a catalog that rejects either
// has no data to reject along with it.
func TestMaintenanceRunsAsItsOwnCommit(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &actionRecordingCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, cat.snapshot(), 1)
	time.Sleep(age + 100*time.Millisecond)
	cat.calls = nil

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, ManifestMergeEnabled: true})
	require.NoError(t, appendOne(t, ctx, c))

	require.Len(t, cat.calls, 3, "the data commit, then the property, then expiry")
	assert.NotContains(t, cat.calls[0], string(table.UpdateSetProperties), "the data commit must not carry the property")
	assert.NotContains(t, cat.calls[0], string(table.UpdateRemoveSnapshots), "the data commit must not carry expiry")
	assert.Equal(t, []string{string(table.UpdateSetProperties)}, cat.calls[1])
	assert.Equal(t, []string{string(table.UpdateRemoveSnapshots)}, cat.calls[2])
	assert.Equal(t, "true", cat.meta.Properties()[table.ManifestMergeEnabledKey])
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the aged seed snapshot is expired")
}

// TestMaintenanceFollowsRowDeltaCommits: the pass runs after every data
// commit path, not only the batched append. Here a merge-on-read row delta.
// TestLandedExpiryLogsTheRemovedCount: after upgrade this output starts
// removing history from tables that kept everything before, so an operator
// asking why time travel stopped working needs a log line that says when
// and how much. A pass that removes nothing logs nothing.
func TestLandedExpiryLogsTheRemovedCount(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &actionRecordingCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, cat.snapshot(), 3)
	time.Sleep(age + 100*time.Millisecond)
	before := len(cat.meta.Snapshots())

	var logs bytes.Buffer
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	c := newLoggedOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age}, logger)
	require.NoError(t, appendOne(t, ctx, c))

	after := len(cat.meta.Snapshots())
	removed := before + 1 - after
	require.Positive(t, removed, "the aged seed snapshots are expired")
	want := fmt.Sprintf("Expired %d snapshots from %s, %d retained", removed, strings.Join(c.table.Identifier(), "."), after)
	assert.Equal(t, 1, strings.Count(logs.String(), want), "one line for the landed expiry, got:\n%s", logs.String())

	// The next pass has nothing to expire and says nothing.
	c.maint.releasePass()
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, strings.Count(logs.String(), "Expired "), "a pass that removes nothing must not log, got:\n%s", logs.String())
}

func TestMaintenanceFollowsRowDeltaCommits(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	require.NoError(t, c.Commit(ctx, morUpsertInput(t, ctx, cat.snapshot(), 7)))
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the row-delta commit must be followed by expiry")
}

// TestExpiryConflictIsRetriedWithinThePass: the expiry commit asserts every
// ref, so another writer landing during its round trip is a clean 409. The
// pass reloads and re-stages, bounded by max_retries, so a busy multi-writer
// table (the kind that hits metadata ceilings) still gets expired.
func TestExpiryConflictIsRetriedWithinThePass(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age})
	cat.outcomes, cat.calls = []commitOutcome{commitSucceed, commitConflict}, 0
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 3, cat.calls, "the data commit, an expiry conflict, then expiry lands")
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the retried expiry must still remove the aged snapshot")
}

// TestExpiryConflictsExhaustingRetriesLeaveTheDataCommitIntact: when every
// expiry attempt conflicts, the pass gives up after max_retries attempts, the
// data commit that preceded it is unaffected, and the next pass tries again.
func TestExpiryConflictsExhaustingRetriesLeaveTheDataCommitIntact(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 2, MaxSnapshotAge: age})
	cat.outcomes, cat.calls = []commitOutcome{commitSucceed, commitConflict, commitConflict}, 0
	require.NoError(t, appendOne(t, ctx, c), "a failed expiry pass must not fail the data commit")
	assert.Equal(t, 3, cat.calls, "the data commit, then exactly max_retries expiry attempts")
	assert.Len(t, cat.meta.Snapshots(), 2, "nothing was expired this pass")

	require.NoError(t, appendOne(t, ctx, c))
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the next pass expires the aged snapshots")
}

// occWrappingCatalog enforces commit requirements the way a real catalog
// does before handing the call to inner: a commit built on metadata that is
// no longer current is a clean 409. memCatalog alone ignores requirements, so
// a stale table handle would go unnoticed.
type occWrappingCatalog struct {
	*memCatalog
	inner table.CatalogIO
}

func (c *occWrappingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	for _, r := range reqs {
		if err := r.Validate(c.meta); err != nil {
			return nil, "", fmt.Errorf("%w: %v", rest.ErrCommitFailed, err)
		}
	}
	return c.inner.CommitTable(ctx, ident, reqs, updates)
}

func (c *occWrappingCatalog) snapshot() *table.Table { return tableBoundTo(c.memCatalog, c) }

// TestAmbiguousExpiryOutcomeReloadsTheTable: the expiry commit lands but its
// response is lost. The pass must reload, or the committer's table handle is
// behind the catalog and the next data commit conflicts with the output's own
// expiry. At max_retries: 1 that would nack the batch.
func TestAmbiguousExpiryOutcomeReloadsTheTable(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	scripted := &scriptedCatalog{memCatalog: mem}
	cat := &occWrappingCatalog{memCatalog: mem, inner: scripted}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
	scripted.outcomes = []commitOutcome{commitSucceed, commitLandThenUnknown}
	require.NoError(t, appendOne(t, ctx, c), "an ambiguous expiry outcome must not fail the data commit")
	assert.Equal(t, 2, scripted.calls, "the data commit, then one expiry attempt")
	require.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the expiry did land despite the lost response")

	require.NoError(t, appendOne(t, ctx, c), "the next data commit must start from the reloaded table; a stale handle is a 409 here")
}

// TestRejectedExpiryIsRetriedOncePerInterval: a catalog that rejects the
// remove-snapshots update costs one failed round trip per pass interval, not
// one per commit, and never a data commit. A 400 is definitive, so it is not
// retried within the pass either; max_retries covers conflicts only.
func TestRejectedExpiryIsRetriedOncePerInterval(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &expiryRejectingCatalog{memCatalog: mem}
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, MaxSnapshotAge: age, MaintenanceInterval: time.Hour})
	for range 3 {
		require.NoError(t, appendOne(t, ctx, c))
	}
	assert.Equal(t, 1, cat.rejected, "one attempt per interval, and no retry of a definitive rejection within the pass")
	assert.Len(t, cat.meta.Snapshots(), 4, "every data commit landed; nothing was expired")

	c.maint.releasePass() // the interval has passed
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 2, cat.rejected, "the next pass tries again")
	assert.Len(t, cat.meta.Snapshots(), 5)
}

// TestRejectedMergePropertyIsRetriedOncePerInterval: a catalog that refuses
// the property without naming the key (so the stripper cannot learn it) sees
// one failed set-properties commit per pass interval. Gating on "the table
// lacks the property" alone would retry on every commit.
func TestRejectedMergePropertyIsRetriedOncePerInterval(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &mergePropRejectingCatalog{memCatalog: mem}
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 3, ManifestMergeEnabled: true, MaintenanceInterval: time.Hour})

	for range 3 {
		require.NoError(t, appendOne(t, ctx, c))
	}
	assert.Equal(t, 1, cat.rejected, "the property is attempted once per interval, not once per commit")
	assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey)
	assert.Len(t, cat.meta.Snapshots(), 3, "every data commit landed")
	assert.Equal(t, 3, manifestCount(t, ctx, cat.snapshot()), "without the property the table keeps one manifest per commit")

	c.maint.releasePass()
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 2, cat.rejected, "the next pass tries again")
}

// TestNamedMergePropertyRejectionIsLearned: a catalog that names
// commit.manifest-merge.enabled as prohibited (Unity Catalog) teaches the
// stripper the key from the rejected maintenance commit. The property is
// never sent again, on any later pass, and the data commits never saw it.
func TestNamedMergePropertyRejectionIsLearned(t *testing.T) {
	ctx := t.Context()
	_, mem := newTestTable(t)
	cat := &namedProhibitedKeyCatalog{memCatalog: mem}
	known := newProhibitedKeySet()
	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, ProhibitedKeys: known})

	for range 3 {
		require.NoError(t, appendOne(t, ctx, c))
	}
	assert.Equal(t, 1, cat.rejected, "one named rejection; the key is learned and never sent again")
	assert.True(t, known.has(table.ManifestMergeEnabledKey), "the stripper learned the key from the maintenance commit")
	assert.NotContains(t, cat.meta.Properties(), table.ManifestMergeEnabledKey)
	assert.Len(t, cat.meta.Snapshots(), 3, "every data commit landed")
	assert.Equal(t, 3, manifestCount(t, ctx, cat.snapshot()), "the table keeps one manifest per commit, as documented for such catalogs")
}

// TestMaintenanceRunsAtMostOncePerInterval: between passes, data commits make
// exactly one catalog call each, whatever is eligible to expire. The pass
// interval is what bounds the extra round trips on a busy table.
func TestMaintenanceRunsAtMostOncePerInterval(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem} // unscripted: counts calls
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, MaintenanceInterval: time.Hour})
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 2, cat.calls, "the first commit is followed by a pass: data, then expiry")

	for range 2 {
		time.Sleep(age + 100*time.Millisecond)
		require.NoError(t, appendOne(t, ctx, c))
	}
	assert.Equal(t, 4, cat.calls, "no pass within the interval, even with snapshots eligible")
	assert.Len(t, cat.meta.Snapshots(), 3, "snapshots accumulate between passes")

	c.maint.releasePass()
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 6, cat.calls, "the interval has passed: data, then expiry")
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2)
}

// TestMaintenanceWithNothingToDoMakesNoCatalogCall: a pass that stages
// nothing costs nothing. iceberg-go commits a transaction only when it
// carries an update, so neither case below reaches the catalog.
func TestMaintenanceWithNothingToDoMakesNoCatalogCall(t *testing.T) {
	t.Run("only a retained ref's head is old", func(t *testing.T) {
		// A tag pins the only old snapshot. The head of a retained ref is
		// always kept, so expiry stages nothing: no commit, so no ref is
		// asserted and no concurrent ref mover is conflicted with.
		ctx := t.Context()
		tbl, mem := newTestTable(t)
		cat := &scriptedCatalog{memCatalog: mem}
		// A full second, so the three commits below stay younger than the
		// age however slow the machine is; only the seed may be old.
		const age = time.Second
		seedTable(t, ctx, tbl, 1)
		pinned := mem.meta.CurrentSnapshot().SnapshotID
		meta, err := table.UpdateTableMetadata(mem.meta,
			[]table.Update{table.NewSetSnapshotRefUpdate("keep", pinned, table.TagRef, 0, 0, 0)}, "")
		require.NoError(t, err)
		mem.meta = meta
		time.Sleep(age + 100*time.Millisecond)

		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age})
		for range 3 {
			require.NoError(t, appendOne(t, ctx, c))
		}
		assert.Equal(t, 3, cat.calls, "three data commits and no maintenance commit")
		assert.Len(t, cat.meta.Snapshots(), 4)
		tagged := false
		for name := range cat.meta.Refs() {
			tagged = tagged || name == "keep"
		}
		assert.True(t, tagged, "the tag and the snapshot it pins are kept")
	})

	t.Run("property already on the table and nothing aged", func(t *testing.T) {
		ctx := t.Context()
		tbl, mem := newTestTable(t)
		cat := &scriptedCatalog{memCatalog: mem}
		tx := tbl.NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{table.ManifestMergeEnabledKey: "true"}))
		_, err := tx.Commit(ctx)
		require.NoError(t, err)

		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true, MaxSnapshotAge: time.Hour})
		for range 3 {
			require.NoError(t, appendOne(t, ctx, c))
		}
		assert.Equal(t, 3, cat.calls, "three data commits and no maintenance commit")
	})
}

// expiryRejectingCatalog models a catalog that does not implement the
// remove-snapshots table update: a commit carrying one is rejected with a 400
// and applies nothing; every other commit succeeds. No such catalog is known;
// the tests use it to prove a rejected maintenance commit cannot touch data.
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

// TestManifestMergePropertyStampedWhenAbsentKeptWhenPresent pins the
// contract of commit.manifest_merge_enabled: it reaches the table as the
// standard commit.manifest-merge.enabled property when absent (set by the
// maintenance pass that follows the first commit), an existing
// value on the table wins over the configured one, and it is not written
// into snapshot summaries (where it was inert).
// TestStampedMergePropertyIsLogged: the property stays on shared table
// metadata after this output stops writing, so the commit that sets it is
// logged at info, once per table, and a table that already carries it logs
// nothing.
func TestStampedMergePropertyIsLogged(t *testing.T) {
	ctx := t.Context()
	_, cat := newTestTable(t)
	var logs bytes.Buffer
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	c := newLoggedOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true}, logger)
	require.NoError(t, appendOne(t, ctx, c))
	want := fmt.Sprintf("Set table property %s=true on %s", table.ManifestMergeEnabledKey, strings.Join(c.table.Identifier(), "."))
	assert.Equal(t, 1, strings.Count(logs.String(), want), "one line for the landed stamp, got:\n%s", logs.String())

	c.maint.releasePass()
	require.NoError(t, appendOne(t, ctx, c))
	assert.Equal(t, 1, strings.Count(logs.String(), "Set table property"), "the property is already on the table, nothing to log, got:\n%s", logs.String())
}

func TestManifestMergePropertyStampedWhenAbsentKeptWhenPresent(t *testing.T) {
	t.Run("absent on the table: stamped with the configured value after the first commit", func(t *testing.T) {
		ctx := t.Context()
		_, cat := newTestTable(t)
		c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, ManifestMergeEnabled: true})
		require.NoError(t, appendOne(t, ctx, c))

		assert.Equal(t, "true", cat.meta.Properties()[table.ManifestMergeEnabledKey])
		snap := cat.meta.CurrentSnapshot()
		require.NotNil(t, snap)
		require.NotNil(t, snap.Summary)
		assert.NotContains(t, snap.Summary.Properties, table.ManifestMergeEnabledKey, "manifest merge is a table property, not a summary entry")
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

// TestMergePropertyNotStagedOnceStripperKnowsIt: when the catalog has named
// commit.manifest-merge.enabled as prohibited (the stripper has learned it),
// the property must not be staged at all: the stripper would strip the only
// update and send the catalog a commit with nothing in it.
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

// TestElapsedRefAgeIsExpiredEvenWhenNoSnapshotIsOld pins the documented
// promise that a tag whose own max-ref-age-ms has elapsed is removed, even
// when every snapshot is far younger than max_snapshot_age.
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

// TestBranchOwnSnapshotAgeWinsOverConfiguredAge pins the documented promise
// that a branch's own retention properties apply where set: iceberg-go
// prefers a branch's max-snapshot-age-ms over the configured age.
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

// cancellingCatalog models the caller's context ending mid-pass: on call
// number `at` it applies the commit, then cancels the context and reports
// the cancellation, as a catalog client would when the context dies with
// the request in flight and the server lands it anyway. Every other call
// delegates.
type cancellingCatalog struct {
	*memCatalog
	cancel context.CancelFunc
	at     int
	calls  int
}

func (c *cancellingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	c.calls++
	if c.calls == c.at {
		if _, _, err := c.memCatalog.CommitTable(ctx, ident, reqs, updates); err != nil {
			return nil, "", err
		}
		c.cancel()
		return nil, "", fmt.Errorf("reading commit response: %w", context.Canceled)
	}
	return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

// TestMaintenanceStoppedByContextKeepsTheSlot: the row-delta and overwrite
// paths run the pass on the caller's context. A context that ends mid-pass
// (shutdown, a deadline) is not a catalog failure: the data commit still
// succeeds and nothing is warned about. The pass slot stays booked, as after
// any failed pass, so a table whose expiry walk outlives the context is not
// walked again on the very next commit. The interrupted commit may still
// have landed, so the table is reloaded on a detached context: the
// requirement-enforcing wrapper would otherwise 409 the next data commit, a
// nacked batch at max_retries: 1.
func TestMaintenanceStoppedByContextKeepsTheSlot(t *testing.T) {
	tbl, mem := newTestTable(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelling := &cancellingCatalog{memCatalog: mem, cancel: cancel, at: 2}
	cat := &occWrappingCatalog{memCatalog: mem, inner: cancelling}
	// A full second, so the commits below stay younger than the age however
	// slow the machine is; only the seed may be old.
	const age = time.Second
	seedTable(t, t.Context(), tbl, 1)
	time.Sleep(age + 100*time.Millisecond)
	cancelling.calls = 0

	c := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, MaintenanceInterval: time.Hour})
	require.NoError(t, c.Commit(ctx, morUpsertInput(t, t.Context(), cat.snapshot(), 3)), "the data commit landed before the context ended")
	assert.Equal(t, 2, cancelling.calls, "the data commit, then the expiry attempt the cancellation interrupted")
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2, "the interrupted expiry did land")
	booked := c.maint.nextPassAt.Load()
	assert.NotZero(t, booked, "the pass slot stays booked for the interval")

	require.NoError(t, appendOne(t, t.Context(), c), "the next data commit must start from the reloaded table; a stale handle is a 409 here")
	assert.Equal(t, 3, cancelling.calls, "a data commit only: no pass runs until the interval has passed")
	assert.Equal(t, booked, c.maint.nextPassAt.Load(), "the slot is unchanged")
	assert.LessOrEqual(t, len(cat.meta.Snapshots()), 2)
}

// TestMaintenanceIntervalSurvivesCommitterRecreation: the router recreates the
// committer whenever the writer is recreated (schema evolution, error paths)
// and hands every generation the same tableMaintenanceState. A pass slot
// booked by one generation holds for the next, so recreation does not cost a
// maintenance round trip per generation; a state held only on the committer
// would.
func TestMaintenanceIntervalSurvivesCommitterRecreation(t *testing.T) {
	ctx := t.Context()
	tbl, mem := newTestTable(t)
	cat := &scriptedCatalog{memCatalog: mem} // unscripted: counts calls
	const age = 100 * time.Millisecond
	seedTable(t, ctx, tbl, 1)
	time.Sleep(age + 100*time.Millisecond)

	shared := newTableMaintenanceState()
	cfg := CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, MaintenanceInterval: time.Hour, Maintenance: shared}
	first := newOutcomeCommitter(t, cat, cat.snapshot, cfg)
	require.NoError(t, appendOne(t, ctx, first))
	require.Equal(t, 2, cat.calls, "the first generation's first commit runs a pass")
	first.Close()

	second := newOutcomeCommitter(t, cat, cat.snapshot, cfg)
	time.Sleep(age + 100*time.Millisecond)
	require.NoError(t, appendOne(t, ctx, second))
	assert.Equal(t, 3, cat.calls, "the second generation is inside the interval the first booked: no pass")
	assert.Len(t, cat.meta.Snapshots(), 2, "the aged snapshot waits for the next interval")

	private := newOutcomeCommitter(t, cat, cat.snapshot, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age, MaintenanceInterval: time.Hour})
	require.NoError(t, appendOne(t, ctx, private))
	assert.Equal(t, 5, cat.calls, "a committer without the shared state starts its own interval: data, then expiry")
}

// TestClaimPassAdmitsOneCaller: the slot is claimed with a compare-and-swap,
// so two committers for one table racing on the same due time run one pass,
// not two.
func TestClaimPassAdmitsOneCaller(t *testing.T) {
	const callers = 32
	s := newTableMaintenanceState()
	now := time.Now()
	var (
		start sync.WaitGroup
		done  sync.WaitGroup
		won   atomic.Int32
	)
	start.Add(1)
	for range callers {
		done.Go(func() {
			start.Wait()
			if s.claimPass(now, time.Hour) {
				won.Add(1)
			}
		})
	}
	start.Done()
	done.Wait()
	assert.Equal(t, int32(1), won.Load(), "exactly one caller claims a due slot")
	assert.False(t, s.claimPass(now.Add(59*time.Minute), time.Hour), "the slot is booked for the interval")
	assert.True(t, s.claimPass(now.Add(time.Hour), time.Hour), "and is free again once it has passed")
}
