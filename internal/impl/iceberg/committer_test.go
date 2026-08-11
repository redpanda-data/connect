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
	"strconv"
	"strings"
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

// memCatalog is a minimal in-memory table.CatalogIO used to drive transaction
// commits without standing up a real catalog. It mirrors the pattern used in
// iceberg-go's own table_test.go.
type memCatalog struct {
	meta             table.Metadata
	metadataLocation string
	ident            table.Identifier
	location         string
}

func (m *memCatalog) LoadTable(context.Context, table.Identifier) (*table.Table, error) {
	return m.snapshot(), nil
}

func (m *memCatalog) CommitTable(_ context.Context, _ table.Identifier, _ []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	meta, err := table.UpdateTableMetadata(m.meta, updates, "")
	if err != nil {
		return nil, "", err
	}
	m.meta = meta
	return meta, m.metadataLocation, nil
}

func (m *memCatalog) snapshot() *table.Table {
	return table.New(
		m.ident,
		m.meta,
		m.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		m,
	)
}

// newTestTable creates an iceberg table backed by an in-memory catalog and the
// local filesystem under tb.TempDir().
func newTestTable(tb testing.TB) (*table.Table, *memCatalog) {
	tb.Helper()
	location := filepath.ToSlash(tb.TempDir())

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)

	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(tb, err)

	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "t"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// synthDataFile builds a DataFile referencing path. The path need not exist on
// disk — AddDataFiles validates metadata only.
func synthDataFile(tb testing.TB, spec iceberg.PartitionSpec, path string) iceberg.DataFile {
	tb.Helper()
	b, err := iceberg.NewDataFileBuilder(
		spec,
		iceberg.EntryContentData,
		path,
		iceberg.ParquetFile,
		nil, nil, nil,
		1, 256,
	)
	require.NoError(tb, err)
	return b.Build()
}

// seedTable commits `count` separate single-file snapshots, approximating a
// table that has accumulated many manifests from many small writes. The
// committed paths each carry a fresh uuid, so they do not collide with any
// later "real" commit by the unit under test. Returns the latest table handle.
func seedTable(tb testing.TB, ctx context.Context, tbl *table.Table, count int) *table.Table {
	tb.Helper()
	for range count {
		df := synthDataFile(tb, tbl.Spec(),
			fmt.Sprintf("%s/data/seed-%s.parquet", tbl.Location(), uuid.New()))
		tx := tbl.NewTransaction()
		require.NoError(tb, tx.AddDataFiles(ctx, []iceberg.DataFile{df}, nil,
			table.WithoutAutoNameMapping(), table.WithoutDuplicateCheck()))
		next, err := tx.Commit(ctx)
		require.NoError(tb, err)
		tbl = next
	}
	return tbl
}

// TestCommitterSkipsDuplicateCheck pins the T6692 fix: the committer must
// bypass iceberg-go's default duplicate-path check on AddDataFiles. That check
// is an O(snapshot) scan of every manifest entry and dominated the commit hot
// path for Comcast. The writer already guarantees per-file path uniqueness via
// uuid stamping (writer.go), so the check is redundant.
//
// Behavioural assertion: committing a path that already exists in the table
// succeeds. Without the fix, AddDataFiles returns
// "cannot add files that are already referenced by table" and Commit fails.
func TestCommitterSkipsDuplicateCheck(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)

	tbl = seedTable(t, ctx, tbl, 1)

	logger := service.MockResources().Logger()
	c, err := NewCommitter(tbl, cat, CommitConfig{
		ManifestMergeEnabled: false,
		MaxRetries:           1,
	}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
	require.NoError(t, err)
	defer c.Close()

	dupPath := fmt.Sprintf("%s/data/collision.parquet", tbl.Location())
	df1 := synthDataFile(t, tbl.Spec(), dupPath)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df1}, SchemaID: c.currentSchemaID()}))

	// Second commit at the same path: only succeeds if the dup check is off.
	df2 := synthDataFile(t, tbl.Spec(), dupPath)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df2}, SchemaID: c.currentSchemaID()}))
}

// TestNewCommitterRejectsNilReloadTable pins the constructor guard:
// commitLocked dereferences reloadTable on every failure branch, so a nil one
// must be rejected at construction with a clear error instead of panicking
// mid-commit.
func TestNewCommitterRejectsNilReloadTable(t *testing.T) {
	tbl, cat := newTestTable(t)
	_, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1}, nil, service.MockResources().Logger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reloadTable must not be nil")
}

// commitOutcome scripts how scriptedCatalog handles a single CommitTable call.
type commitOutcome int

const (
	commitSucceed             commitOutcome = iota // apply the update and report success
	commitLandThenFail                             // apply the update but report ErrCommitFailed (lost/ambiguous ack)
	commitConflict                                 // do NOT apply the update and report ErrCommitFailed (clean 409)
	commitLandThenUnknown                          // apply the update but report ErrCommitStateUnknown (landed, ambiguous 5xx)
	commitUnknownNoLand                            // do NOT apply the update and report ErrCommitStateUnknown (5xx before applying)
	commitLandThenServerError                      // apply the update but report a NON-sentinel ambiguous 5xx (rest.ErrServerError)
)

// scriptedCatalog drives per-call CommitTable outcomes so tests can model the
// two ways a commit "fails" from the client's point of view: a genuine conflict
// where nothing landed, and a lost response where the write actually landed.
// Calls beyond the scripted outcomes succeed normally.
type scriptedCatalog struct {
	*memCatalog
	outcomes []commitOutcome
	calls    int
}

func (c *scriptedCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	outcome := commitSucceed
	if c.calls < len(c.outcomes) {
		outcome = c.outcomes[c.calls]
	}
	c.calls++

	var applies bool
	var retErr error
	switch outcome {
	case commitSucceed:
		applies, retErr = true, nil
	case commitLandThenFail:
		applies, retErr = true, rest.ErrCommitFailed
	case commitLandThenUnknown:
		applies, retErr = true, rest.ErrCommitStateUnknown
	case commitConflict:
		applies, retErr = false, rest.ErrCommitFailed
	case commitUnknownNoLand:
		applies, retErr = false, rest.ErrCommitStateUnknown
	case commitLandThenServerError:
		// iceberg-go maps only 500/502/503/504 to ErrCommitStateUnknown; other
		// 5xx surface as ErrServerError. Equally ambiguous, different sentinel.
		applies, retErr = true, fmt.Errorf("%w: internal error", rest.ErrServerError)
	}

	if applies {
		if _, _, err := c.memCatalog.CommitTable(ctx, ident, reqs, updates); err != nil {
			return nil, "", err
		}
	}
	if retErr != nil {
		return nil, "", retErr
	}
	return c.meta, c.metadataLocation, nil
}

func (c *scriptedCatalog) snapshot() *table.Table {
	return table.New(
		c.ident,
		c.meta,
		c.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil },
		c,
	)
}

// countDataFileRefs counts how many live data-file entries in the table's
// current snapshot reference path. A correctly committed file appears exactly
// once; a duplicate-registration bug shows up as two.
func countDataFileRefs(tb testing.TB, ctx context.Context, tbl *table.Table, path string) int {
	tb.Helper()
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0
	}
	fs, err := tbl.FS(ctx)
	require.NoError(tb, err)
	manifests, err := snap.Manifests(fs)
	require.NoError(tb, err)

	n := 0
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fs, true) {
			require.NoError(tb, err)
			if entry.DataFile().FilePath() == path {
				n++
			}
		}
	}
	return n
}

// countSnapshotsWithCommitID counts how many snapshots in the table's metadata
// carry the mutation idempotency token (commitIDProp) in their summary. Only the
// copy-on-write and merge-on-read mutation paths stamp it — plain appends and
// seed writes do not — so for an exactly-once mutation this is exactly 1. A
// duplicate-apply bug (a landed commit re-applied on retry) shows up as 2.
func countSnapshotsWithCommitID(tbl *table.Table) int {
	n := 0
	for _, s := range tbl.Metadata().Snapshots() {
		if s.Summary != nil && s.Summary.Properties[commitIDProp] != "" {
			n++
		}
	}
	return n
}

func newScriptedCommitter(tb testing.TB, outcomes ...commitOutcome) (*committer, *scriptedCatalog) {
	tb.Helper()
	_, mem := newTestTable(tb)
	cat := &scriptedCatalog{memCatalog: mem, outcomes: outcomes}
	logger := service.MockResources().Logger()
	c, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
	require.NoError(tb, err)
	tb.Cleanup(c.Close)
	return c, cat
}

// TestCommitterRetryDoesNotDuplicateLandedFile is the regression test for the
// "conflicting sequence numbers" corruption: a commit attempt lands server-side
// but reports failure (a lost/ambiguous response), and connect's retry reloads
// the table — which now already references the file — then re-adds it with the
// duplicate check disabled. The file must end up registered exactly once.
func TestCommitterRetryDoesNotDuplicateLandedFile(t *testing.T) {
	ctx := t.Context()
	c, cat := newScriptedCommitter(t, commitLandThenFail)

	path := fmt.Sprintf("%s/data/landed.parquet", cat.location)
	df := synthDataFile(t, cat.snapshot().Spec(), path)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	require.Equal(t, 1, countDataFileRefs(t, ctx, cat.snapshot(), path),
		"a file that landed on the failed attempt must not be re-added by the retry")
}

// TestCommitterRetryReAddsOnGenuineConflict guards against the idempotency fix
// over-filtering: on a clean conflict nothing landed, so the retry MUST re-add
// the file. The file must end up committed exactly once (not dropped).
func TestCommitterRetryReAddsOnGenuineConflict(t *testing.T) {
	ctx := t.Context()
	c, cat := newScriptedCommitter(t, commitConflict)

	path := fmt.Sprintf("%s/data/retried.parquet", cat.location)
	df := synthDataFile(t, cat.snapshot().Spec(), path)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	require.Equal(t, 1, countDataFileRefs(t, ctx, cat.snapshot(), path),
		"a file that did not land must be re-added by the retry")
}

// TestCommitterRetriesUnknownStateAndDedupes covers the landed-but-ambiguous
// case (5xx/timeout -> ErrCommitStateUnknown): the append path must retry
// rather than surface an error, and the dedupe must keep the landed file
// registered exactly once. Before ErrCommitStateUnknown was retried this
// Commit returned an error and the batch was redelivered under a new path.
func TestCommitterRetriesUnknownStateAndDedupes(t *testing.T) {
	ctx := t.Context()
	c, cat := newScriptedCommitter(t, commitLandThenUnknown)

	path := fmt.Sprintf("%s/data/landed-unknown.parquet", cat.location)
	df := synthDataFile(t, cat.snapshot().Spec(), path)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	require.Equal(t, 1, countDataFileRefs(t, ctx, cat.snapshot(), path),
		"a file that landed before an unknown-state response must be registered exactly once")
}

// TestCommitterRetriesUnknownStateWithoutLanding covers the other unknown-state
// branch: a 5xx before the write landed. The retry must still commit the file
// (exactly once), proving the unknown-state retry does not drop legitimate work.
func TestCommitterRetriesUnknownStateWithoutLanding(t *testing.T) {
	ctx := t.Context()
	c, cat := newScriptedCommitter(t, commitUnknownNoLand)

	path := fmt.Sprintf("%s/data/unknown-noland.parquet", cat.location)
	df := synthDataFile(t, cat.snapshot().Spec(), path)
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	require.Equal(t, 1, countDataFileRefs(t, ctx, cat.snapshot(), path),
		"a file that did not land before an unknown-state response must be committed once on retry")
}

// TestStaleSchemaErrorOnAllEntryPoints (T-16) proves every commit entry point
// rejects an input whose SchemaID no longer matches the table's current schema
// with a StaleSchemaError carrying the right field values, and exercises the
// error's message.
func TestStaleSchemaErrorOnAllEntryPoints(t *testing.T) {
	ctx := t.Context()

	assertStale := func(t *testing.T, err error, writerID, currentID int) {
		t.Helper()
		require.Error(t, err)
		var se *StaleSchemaError
		require.ErrorAs(t, err, &se)
		assert.Equal(t, writerID, se.WriterSchemaID)
		assert.Equal(t, currentID, se.CurrentSchemaID)
	}

	t.Run("doCommit (append path)", func(t *testing.T) {
		tbl, cat := newTestTable(t)
		c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer c.Close()
		cur := c.currentSchemaID()
		df := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/stale-%s.parquet", tbl.Location(), uuid.New()))
		assertStale(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: cur + 1}), cur+1, cur)
	})

	t.Run("commitRowDelta (merge-on-read path)", func(t *testing.T) {
		tbl, cat := newTestTable(t)
		c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer c.Close()
		cur := c.currentSchemaID()
		// A delete file present routes c.Commit through commitRowDelta.
		w := newDeleteWriter(t, tbl)
		dels, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 2})})
		require.NoError(t, err)
		assertStale(t, c.Commit(ctx, CommitInput{DeleteFiles: dels, SchemaID: cur + 1}), cur+1, cur)
	})

	t.Run("commitOverwrite (copy-on-write path)", func(t *testing.T) {
		tbl, cat := newTestTable(t)
		c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer c.Close()
		cur := c.currentSchemaID()
		// The schema check fires before any filesystem or reader work, so an empty
		// OverwriteInput is enough to reach it.
		assertStale(t, c.commitOverwrite(ctx, OverwriteInput{SchemaID: cur + 1}), cur+1, cur)
	})

	t.Run("Error message", func(t *testing.T) {
		e := &StaleSchemaError{WriterSchemaID: 5, CurrentSchemaID: 3}
		assert.Equal(t, "stale schema: data written with schema 5 but table is at schema 3", e.Error())
	})
}

// TestCommitStampsMaxSnapshotAge (T-20) proves a committer configured with a
// non-zero MaxSnapshotAge stamps MaxSnapshotAgeMsKey into the committed snapshot's
// summary (via the shared props map in commitLocked).
func TestCommitStampsMaxSnapshotAge(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)
	const age = 48 * time.Hour
	c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1, MaxSnapshotAge: age}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	df := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/age-%s.parquet", tbl.Location(), uuid.New()))
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	snap := cat.snapshot().CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Summary)
	assert.Equal(t, strconv.FormatInt(age.Milliseconds(), 10), snap.Summary.Properties[table.MaxSnapshotAgeMsKey],
		"the committed snapshot summary must carry the configured max snapshot age")
}

// TestNewCommitterClampsMaxRetries (CORR-4) proves a configured MaxRetries below 1
// is clamped up to 1, so the retry loop runs at least one attempt rather than
// returning a "committing transaction after 0 attempts" error wrapping nil.
func TestNewCommitterClampsMaxRetries(t *testing.T) {
	ctx := t.Context()
	for _, n := range []int{0, -3} {
		t.Run(fmt.Sprintf("max_retries_%d", n), func(t *testing.T) {
			tbl, cat := newTestTable(t)
			c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: n}, reloadFn(cat), service.MockResources().Logger())
			require.NoError(t, err)
			defer c.Close()
			assert.Equal(t, 1, c.cfg.MaxRetries, "MaxRetries must be clamped to at least 1")

			df := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/clamp-%s.parquet", tbl.Location(), uuid.New()))
			require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}),
				"a clamped committer must still perform the commit")
		})
	}
}

// TestMaxRetriesLint (CORR-4) pins the config lint rule: max_retries below 1 must
// produce a lint error, while the default and any value >= 1 must not.
func TestMaxRetriesLint(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()
	base := func(extra string) string {
		return `
iceberg:
  catalog:
    url: http://localhost:8181/api/catalog
  namespace: ns
  table: t
  storage:
    aws_s3:
      bucket: b
` + extra
	}
	cases := []struct {
		name     string
		extra    string
		wantLint bool
	}{
		{"default (no commit block)", "", false},
		{"explicit valid", "  commit:\n    max_retries: 2\n", false},
		{"zero", "  commit:\n    max_retries: 0\n", true},
		{"negative", "  commit:\n    max_retries: -1\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lints, err := linter.LintOutputYAML([]byte(base(tc.extra)))
			require.NoError(t, err)
			var found []string
			for _, l := range lints {
				if strings.Contains(l.Error(), "max_retries") {
					found = append(found, l.Error())
				}
			}
			if tc.wantLint {
				assert.NotEmpty(t, found, "expected a max_retries lint, got: %v", lints)
			} else {
				assert.Empty(t, found, "expected no max_retries lint, got: %v", found)
			}
		})
	}
}

// BenchmarkAddDataFilesDupCheck measures the cost of Transaction.AddDataFiles
// against a snapshot pre-seeded with N existing data files, with iceberg-go's
// duplicate-path check on vs off. It is the local reproduction harness for
// T6692.
//
// Run locally with e.g.:
//
//	go test -bench BenchmarkAddDataFilesDupCheck -benchmem -benchtime=20x \
//	    ./internal/impl/iceberg
//
// The `dup_check=true` variants scale roughly linearly with seed; the
// `dup_check=false` variants are flat. The committer in this package ships
// with dup_check off (see committer.go doCommit) — the `dup_check=true`
// numbers are what production looked like before T6692.
func BenchmarkAddDataFilesDupCheck(b *testing.B) {
	ctx := b.Context()

	for _, seed := range []int{10, 100, 1000} {
		// Build the seeded table once per `seed` size and reuse it across the
		// inner sub-benchmarks. Seeding 1000 commits is the dominant cost, and
		// the AddDataFiles call we measure operates on the parent snapshot
		// without mutating it — so sharing is safe.
		tbl, _ := newTestTable(b)
		tbl = seedTable(b, ctx, tbl, seed)

		for _, dupCheck := range []bool{true, false} {
			opts := []table.WriteOption{table.WithoutAutoNameMapping()}
			if !dupCheck {
				opts = append(opts, table.WithoutDuplicateCheck())
			}

			b.Run(fmt.Sprintf("seed=%d/dup_check=%v", seed, dupCheck), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; b.Loop(); i++ {
					df := synthDataFile(b, tbl.Spec(),
						fmt.Sprintf("%s/data/bench-%d.parquet", tbl.Location(), i))
					tx := tbl.NewTransaction()
					if err := tx.AddDataFiles(ctx, []iceberg.DataFile{df}, nil, opts...); err != nil {
						b.Fatal(err)
					}
					// Discard the transaction — we are timing AddDataFiles, not Commit.
				}
			})
		}
	}
}
