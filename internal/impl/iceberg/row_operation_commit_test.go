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

// These tests exercise the real equality-delete write path and RowDelta commit
// against an in-memory catalog backed by the local filesystem (the same harness
// the committer tests use), so they validate actual file production and commit
// semantics without needing a containerised catalog.

func newDeleteWriter(t testing.TB, tbl *table.Table) *writer {
	t.Helper()
	return &writer{
		table:         tbl,
		caseSensitive: true,
		rowOpCfg:      RowOpConfig{IdentifierFields: []string{"id"}},
		logger:        service.MockResources().Logger(),
	}
}

func reloadFn(cat *memCatalog) func(context.Context) (*table.Table, error) {
	return func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }
}

func TestWriteEqualityDeletesProducesDeleteFiles(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newTestTable(t)
	w := newDeleteWriter(t, tbl)

	msgs := service.MessageBatch{
		structuredMsg(t, map[string]any{"id": 2}),
		structuredMsg(t, map[string]any{"id": 4}),
	}
	deleteFiles, err := w.writeEqualityDeletes(ctx, msgs)
	require.NoError(t, err)
	require.Len(t, deleteFiles, 1)
	assert.Equal(t, iceberg.EntryContentEqDeletes, deleteFiles[0].ContentType())
	assert.Equal(t, []int{1}, deleteFiles[0].EqualityFieldIDs())
	assert.EqualValues(t, 2, deleteFiles[0].Count())
}

func TestWriteEqualityDeletesMissingKeyErrors(t *testing.T) {
	ctx := t.Context()
	tbl, _ := newTestTable(t)
	w := newDeleteWriter(t, tbl)

	// Message has no "id" field — the delete key cannot be resolved.
	_, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"other": 1})})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing or null")
}

func TestCommitDeleteOnlyProducesDeleteSnapshot(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)
	w := newDeleteWriter(t, tbl)

	deleteFiles, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 2})})
	require.NoError(t, err)

	c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Commit(ctx, CommitInput{DeleteFiles: deleteFiles, SchemaID: c.currentSchemaID()}))

	snap := cat.snapshot().CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Summary)
	assert.Equal(t, table.OpDelete, snap.Summary.Operation)
}

func TestCommitUpsertProducesOverwriteSnapshot(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)
	w := newDeleteWriter(t, tbl)

	deleteFiles, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 7})})
	require.NoError(t, err)

	// A data file alongside the deletes turns the RowDelta into an overwrite,
	// the shape an upsert produces.
	dataFile := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/new-%s.parquet", tbl.Location(), uuid.New()))

	c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Commit(ctx, CommitInput{
		Files:       []iceberg.DataFile{dataFile},
		DeleteFiles: deleteFiles,
		SchemaID:    c.currentSchemaID(),
	}))

	snap := cat.snapshot().CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Summary)
	assert.Equal(t, table.OpOverwrite, snap.Summary.Operation)
}

func TestCommitInsertOnlyStaysAppend(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)

	dataFile := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/ins-%s.parquet", tbl.Location(), uuid.New()))

	c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	// No delete files: the committer must take the AddDataFiles fast path,
	// producing an append snapshot (no behavioural change for existing users).
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{dataFile}, SchemaID: c.currentSchemaID()}))

	snap := cat.snapshot().CurrentSnapshot()
	require.NotNil(t, snap)
	require.NotNil(t, snap.Summary)
	assert.Equal(t, table.OpAppend, snap.Summary.Operation)
}

// newTypedKeyTable builds an unpartitioned v2 table whose identifier column has
// the given type, backed by an in-memory catalog and the local filesystem.
func newTypedKeyTable(t testing.TB, keyField iceberg.NestedField) *table.Table {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	sc := iceberg.NewSchema(0,
		keyField,
		iceberg.NestedField{ID: 9, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/v1-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "typed"},
		location:         location,
	}
	return cat.snapshot()
}

// TestWriteEqualityDeletesTypedKeys proves the equality-delete writer produces
// valid delete files for the key types that used to be gated off — uuid,
// decimal, timestamp, and a >2^53 int64 — by writing real Parquet via the
// iceberg-go equality-delete writer over the local filesystem.
func TestWriteEqualityDeletesTypedKeys(t *testing.T) {
	ctx := t.Context()
	cases := []struct {
		name string
		key  iceberg.NestedField
		val  any
	}{
		{"uuid", iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.UUID, Required: true}, "f47ac10b-58cc-4372-a567-0e02b2c3d479"},
		{"decimal", iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.DecimalTypeOf(10, 2), Required: true}, "123.45"},
		{"timestamp", iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Timestamp, Required: true}, time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)},
		{"int64-big", iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64, Required: true}, int64(9007199254740993)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl := newTypedKeyTable(t, tc.key)
			w := &writer{
				table:         tbl,
				caseSensitive: true,
				rowOpCfg:      RowOpConfig{IdentifierFields: []string{"k"}},
				logger:        service.MockResources().Logger(),
			}
			df, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"k": tc.val})})
			require.NoError(t, err)
			require.Len(t, df, 1)
			assert.Equal(t, iceberg.EntryContentEqDeletes, df[0].ContentType())
			assert.Equal(t, []int{1}, df[0].EqualityFieldIDs())
			assert.EqualValues(t, 1, df[0].Count())
		})
	}
}

// TestCommitRowDeltaConcurrentNotCoalesced pins the fix for the cross-batch
// duplicate bug: delete-bearing commits must each become their own snapshot and
// never be coalesced with another commit (coalescing would put multiple rows
// for the same key at one sequence number, where equality deletes can't remove
// same-commit data). Firing N concurrent delete commits must yield N snapshots.
func TestCommitRowDeltaConcurrentNotCoalesced(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTable(t)
	logger := service.MockResources().Logger()
	c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 5}, reloadFn(cat), logger)
	require.NoError(t, err)
	defer c.Close()

	const n = 4
	deletes := make([][]iceberg.DataFile, n)
	for i := range deletes {
		w := newDeleteWriter(t, tbl)
		df, derr := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": i})})
		require.NoError(t, derr)
		deletes[i] = df
	}

	// The schema is stable for this test; capture the ID once (a writer likewise
	// uses its own table reference, not the committer's, to set SchemaID).
	schemaID := c.currentSchemaID()
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Commit(ctx, CommitInput{DeleteFiles: deletes[i], SchemaID: schemaID})
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		require.NoErrorf(t, e, "commit %d", i)
	}
	assert.Len(t, cat.snapshot().Metadata().Snapshots(), n,
		"each delete-bearing commit must produce its own snapshot (no coalescing)")
}

// newTestTableV1 mirrors newTestTable but creates a format-version-1 table, so
// the committer's automatic v1→v2 upgrade path can be exercised.
func newTestTableV1(tb testing.TB) (*table.Table, *memCatalog) {
	tb.Helper()
	location := filepath.ToSlash(tb.TempDir())
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
	)
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "1"})
	require.NoError(tb, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "t"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// flakyCatalog wraps memCatalog and fails the first failuresLeft CommitTable
// calls with failErr before delegating to the real catalog. It lets the tests
// drive the committer's retry/conflict and commit-failure paths deterministically
// without a containerised catalog. commitCalls counts every CommitTable
// invocation so a test can assert how many attempts actually occurred.
type flakyCatalog struct {
	*memCatalog
	mu           sync.Mutex
	failuresLeft int
	failErr      error
	commitCalls  int
}

func (f *flakyCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	f.mu.Lock()
	f.commitCalls++
	if f.failuresLeft > 0 {
		f.failuresLeft--
		f.mu.Unlock()
		return nil, "", f.failErr
	}
	f.mu.Unlock()
	return f.memCatalog.CommitTable(ctx, ident, reqs, updates)
}

func (f *flakyCatalog) snapshot() *table.Table {
	return table.New(f.ident, f.meta, f.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, f)
}

func (f *flakyCatalog) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commitCalls
}

func countParquetFiles(tb testing.TB, dir string) int {
	tb.Helper()
	n := 0
	require.NoError(tb, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			n++
		}
		return nil
	}))
	return n
}

// TestCommitUpgradesFormatVersionV1ToV2 pins the automatic format-version upgrade
// (committer.commitLocked): a commit against a v1 table transparently upgrades it
// to v2 (required for row-level deletes), and a subsequent commit succeeds without
// the once-guarded warning interfering.
func TestCommitUpgradesFormatVersionV1ToV2(t *testing.T) {
	ctx := t.Context()
	tbl, cat := newTestTableV1(t)
	require.EqualValues(t, 1, tbl.Metadata().Version(), "precondition: table starts at v1")

	c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer c.Close()

	df1 := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/up-%s.parquet", tbl.Location(), uuid.New()))
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df1}, SchemaID: c.currentSchemaID()}))
	assert.EqualValues(t, 2, cat.snapshot().Metadata().Version(), "table must be upgraded to v2 on first commit")

	// Second commit: the table is already v2, so no upgrade occurs and the
	// once-guarded warning must not have left the committer in a broken state.
	df2 := synthDataFile(t, tbl.Spec(), fmt.Sprintf("%s/data/up-%s.parquet", tbl.Location(), uuid.New()))
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df2}, SchemaID: c.currentSchemaID()}))
	assert.EqualValues(t, 2, cat.snapshot().Metadata().Version())
}

// TestCommitRetriesOnConflict exercises the retry loop in committer.commitLocked:
// a commit that fails with rest.ErrCommitFailed is retried (reloading table
// metadata between attempts) and ultimately succeeds, while exhausting the retry
// budget surfaces an error naming the attempt count.
func TestCommitRetriesOnConflict(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	t.Run("succeeds after retries", func(t *testing.T) {
		_, plain := newTestTable(t)
		fc := &flakyCatalog{memCatalog: plain, failuresLeft: 2, failErr: rest.ErrCommitFailed}
		ftbl := fc.snapshot()
		c, err := NewCommitter(ftbl, CommitConfig{MaxRetries: 5}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer c.Close()

		df := synthDataFile(t, ftbl.Spec(), fmt.Sprintf("%s/data/retry-%s.parquet", ftbl.Location(), uuid.New()))
		require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))
		// 2 conflict failures + 1 success.
		assert.Equal(t, 3, fc.calls(), "commit must be retried past the conflicts")
	})

	t.Run("fails after exhausting retries", func(t *testing.T) {
		_, plain := newTestTable(t)
		fc := &flakyCatalog{memCatalog: plain, failuresLeft: 1 << 30, failErr: rest.ErrCommitFailed}
		ftbl := fc.snapshot()
		c, err := NewCommitter(ftbl, CommitConfig{MaxRetries: 2}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer c.Close()

		df := synthDataFile(t, ftbl.Spec(), fmt.Sprintf("%s/data/retry-%s.parquet", ftbl.Location(), uuid.New()))
		err = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "after 2 attempts")
		assert.Equal(t, 2, fc.calls(), "must attempt exactly MaxRetries times")
	})
}

// TestWriteCleansUpFilesOnCommitFailure pins the best-effort cleanup in
// writer.Write: when the commit fails, the parquet data files already written to
// storage are removed rather than left orphaned. A control writer with a healthy
// committer confirms the write path does produce files, so the absence in the
// failure case is genuinely due to cleanup and not a vacuous pass.
func TestWriteCleansUpFilesOnCommitFailure(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	// Control: a healthy committer leaves the written parquet file in place.
	t.Run("control writes a file", func(t *testing.T) {
		tbl, cat := newTestTable(t)
		c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), logger)
		require.NoError(t, err)
		defer c.Close()
		require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))
		w := &writer{table: tbl, committer: c, caseSensitive: true, logger: logger}
		require.NoError(t, w.Write(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 1})}))
		assert.Positive(t, countParquetFiles(t, tbl.Location()), "write path should produce a parquet file")
	})

	t.Run("failed commit cleans up", func(t *testing.T) {
		_, plain := newTestTable(t)
		fc := &flakyCatalog{memCatalog: plain, failuresLeft: 1 << 30, failErr: errors.New("storage unavailable")}
		ftbl := fc.snapshot()
		c, err := NewCommitter(ftbl, CommitConfig{MaxRetries: 2}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer c.Close()

		require.NoError(t, os.MkdirAll(filepath.Join(ftbl.Location(), "data"), 0o755))
		w := &writer{table: ftbl, committer: c, caseSensitive: true, logger: logger}
		err = w.Write(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 1})})
		require.Error(t, err)
		assert.Zero(t, countParquetFiles(t, ftbl.Location()), "the uncommitted parquet file must be cleaned up after a failed commit")
	})
}

// BenchmarkCommitterAppend measures the append fast path (no delete files),
// which existing append-only users hit. It is the baseline for confirming the
// row-operation work did not regress the commit hot path: the only added cost
// on this path is one branch and a nil-slice append in doCommit.
func BenchmarkCommitterAppend(b *testing.B) {
	ctx := b.Context()
	logger := service.MockResources().Logger()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tbl, cat := newTestTable(b)
		c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), logger)
		require.NoError(b, err)
		df := synthDataFile(b, tbl.Spec(), fmt.Sprintf("%s/data/bench-%d.parquet", tbl.Location(), i))
		b.StartTimer()

		if err := c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		c.Close()
	}
}

// BenchmarkCommitterRowDelta measures the mutation path (one data file plus one
// equality-delete file committed via RowDelta). Comparing it against
// BenchmarkCommitterAppend quantifies the cost of the merge-on-read commit
// relative to a plain append.
func BenchmarkCommitterRowDelta(b *testing.B) {
	ctx := b.Context()
	logger := service.MockResources().Logger()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tbl, cat := newTestTable(b)
		w := newDeleteWriter(b, tbl)
		deleteFiles, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(b, map[string]any{"id": i})})
		require.NoError(b, err)
		dataFile := synthDataFile(b, tbl.Spec(), fmt.Sprintf("%s/data/bench-%d.parquet", tbl.Location(), i))
		c, err := NewCommitter(tbl, CommitConfig{MaxRetries: 1}, reloadFn(cat), logger)
		require.NoError(b, err)
		b.StartTimer()

		if err := c.Commit(ctx, CommitInput{
			Files:       []iceberg.DataFile{dataFile},
			DeleteFiles: deleteFiles,
			SchemaID:    c.currentSchemaID(),
		}); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		c.Close()
	}
}
