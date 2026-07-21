// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

// De-risking harness (NOT production code): characterises the write
// amplification of copy-on-write (COW) row-level mutations in iceberg-go, to
// decide whether COW is viable for streaming CDC in the connect iceberg output.
//
// Run with:
//
//	go test -run TestCOWWriteAmplification -v ./internal/impl/iceberg/
//
// It seeds a table with M data files, each holding R rows with contiguous,
// non-overlapping id ranges (simulating a sorted/clustered key — the realistic
// CDC-on-ordered-key case), then applies a delete touching K keys and measures
// how much of the table COW rewrites, comparing against merge-on-read (MOR).

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// cowArrowSchema is the Arrow schema matching the (id int64, payload string)
// table below. Field names match the iceberg schema so Append can bind them.
var cowArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

// newAmpTable builds an unpartitioned v2 table (id int64, payload string) backed
// by an in-memory catalog and the local filesystem. deleteMode, when non-empty,
// is baked into table metadata as write.delete.mode.
func newAmpTable(tb testing.TB, deleteMode string) (*table.Table, *memCatalog) {
	tb.Helper()
	location := filepath.ToSlash(tb.TempDir())

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	props := iceberg.Properties{table.PropertyFormatVersion: "2"}
	if deleteMode != "" {
		props[table.WriteDeleteModeKey] = deleteMode
	}
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(tb, err)

	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "amp"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// appendDataFile writes ONE real parquet data file holding `rows` rows with
// contiguous ids [startID, startID+rows) and a per-row random payload (so the
// file has realistic size and real id min/max stats), committing it as its own
// snapshot. Returns the latest table handle.
func appendDataFile(tb testing.TB, ctx context.Context, tbl *table.Table, rng *rand.Rand, startID int64, rows, payloadBytes int) *table.Table {
	tb.Helper()
	mem := memory.NewGoAllocator()
	bldr := array.NewRecordBuilder(mem, cowArrowSchema)
	defer bldr.Release()

	idB := bldr.Field(0).(*array.Int64Builder)
	payB := bldr.Field(1).(*array.StringBuilder)
	buf := make([]byte, payloadBytes)
	for i := range rows {
		idB.Append(startID + int64(i))
		for j := range buf {
			buf[j] = byte('a' + rng.Intn(26))
		}
		payB.Append(string(buf))
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()
	rdr, err := array.NewRecordReader(cowArrowSchema, []arrow.RecordBatch{rec})
	require.NoError(tb, err)
	defer rdr.Release()

	tx := tbl.NewTransaction()
	require.NoError(tb, tx.Append(ctx, rdr, nil))
	next, err := tx.Commit(ctx)
	require.NoError(tb, err)
	return next
}

// parquetStats returns the count and total on-disk byte size of all parquet
// files under dir.
func parquetStats(tb testing.TB, dir string) (count int, bytes int64) {
	tb.Helper()
	require.NoError(tb, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			info, ierr := d.Info()
			if ierr != nil {
				return ierr
			}
			count++
			bytes += info.Size()
		}
		return nil
	}))
	return count, bytes
}

// countDeleteManifestFiles returns the number of delete files referenced by the
// current snapshot (delete-content manifests). Zero after a COW mutation; > 0
// after a MOR mutation.
func countDeleteManifestFiles(tb testing.TB, ctx context.Context, tbl *table.Table) int {
	tb.Helper()
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0
	}
	fsys, err := tbl.FS(ctx)
	require.NoError(tb, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(tb, err)

	n := 0
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for entry, err := range m.Entries(fsys, true) {
			require.NoError(tb, err)
			_ = entry
			n++
		}
	}
	return n
}

func summaryInt(tb testing.TB, s *table.Summary, key string) int64 {
	tb.Helper()
	v, ok := s.Properties[key]
	if !ok || v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	require.NoError(tb, err)
	return n
}

type ampResult struct {
	name    string
	mode    string
	m, r, k int
	scatter string

	seedFiles int
	seedBytes int64

	// snapshot summary counters for the mutation
	addedDataFiles   int64
	addedFilesSize   int64
	deletedDataFiles int64
	removedFilesSize int64
	addedRecords     int64
	deletedRecords   int64
	addedDeleteFiles int64
	addedPosDelFiles int64
	addedEqDelFiles  int64

	deleteManifestFiles int
	elapsed             time.Duration
}

// buildKeys returns the set of ids to mutate. For "within" all K keys live in a
// single (middle) file; for "perfile" the K keys are spread one-per-file across
// K distinct files (worst case for COW).
func buildKeys(m, r, k int, scatter string) []int64 {
	keys := make([]int64, 0, k)
	switch scatter {
	case "within":
		fileIdx := int64(m / 2)
		base := fileIdx * int64(r)
		for i := range k {
			keys = append(keys, base+int64(i))
		}
	case "perfile":
		for i := range k {
			keys = append(keys, int64(i)*int64(r)) // first id of file i
		}
	}
	return keys
}

func runAmpScenario(tb testing.TB, ctx context.Context, mode string, m, r, k, payloadBytes int, scatter string) ampResult {
	tb.Helper()
	rng := rand.New(rand.NewSource(int64(m*1_000_000 + r*1000 + k)))

	tbl, cat := newAmpTable(tb, mode)
	for f := range m {
		tbl = appendDataFile(tb, ctx, tbl, rng, int64(f)*int64(r), r, payloadBytes)
	}

	seedFiles, seedBytes := parquetStats(tb, cat.location)

	keys := buildKeys(m, r, k, scatter)
	filter := iceberg.IsIn(iceberg.Reference("id"), keys...)

	tx := tbl.NewTransaction()
	start := time.Now()
	require.NoError(tb, tx.Delete(ctx, filter, nil))
	next, err := tx.Commit(ctx)
	require.NoError(tb, err)
	elapsed := time.Since(start)

	snap := next.CurrentSnapshot()
	require.NotNil(tb, snap)
	require.NotNil(tb, snap.Summary)

	res := ampResult{
		name: fmt.Sprintf("M=%d R=%d K=%d %s", m, r, k, scatter), mode: mode,
		m: m, r: r, k: k, scatter: scatter,
		seedFiles: seedFiles, seedBytes: seedBytes,
		addedDataFiles:      summaryInt(tb, snap.Summary, "added-data-files"),
		addedFilesSize:      summaryInt(tb, snap.Summary, "added-files-size"),
		deletedDataFiles:    summaryInt(tb, snap.Summary, "deleted-data-files"),
		removedFilesSize:    summaryInt(tb, snap.Summary, "removed-files-size"),
		addedRecords:        summaryInt(tb, snap.Summary, "added-records"),
		deletedRecords:      summaryInt(tb, snap.Summary, "deleted-records"),
		addedDeleteFiles:    summaryInt(tb, snap.Summary, "added-delete-files"),
		addedPosDelFiles:    summaryInt(tb, snap.Summary, "added-position-delete-files"),
		addedEqDelFiles:     summaryInt(tb, snap.Summary, "added-equality-delete-files"),
		deleteManifestFiles: countDeleteManifestFiles(tb, ctx, next),
		elapsed:             elapsed,
	}
	return res
}

// TestCOWWriteAmplification is the headline harness. It sweeps M/K/scatter for
// both COW and MOR delete modes and prints a table of measured amplification.
func TestCOWWriteAmplification(t *testing.T) {
	if testing.Short() {
		t.Skip("amplification harness is slow; skipped under -short")
	}
	ctx := t.Context()

	const (
		R            = 1000 // rows per seeded file
		payloadBytes = 128  // per-row payload size -> realistic file sizes
	)

	type scen struct {
		m, k    int
		scatter string
	}
	scenarios := []scen{
		// K keys all within ONE file (best case).
		{10, 1, "within"},
		{50, 1, "within"},
		{200, 1, "within"},
		{200, 10, "within"},
		{200, 100, "within"},
		// K keys spread ONE-PER-FILE across K distinct files (worst case).
		{10, 1, "perfile"},
		{10, 10, "perfile"},
		{50, 1, "perfile"},
		{50, 10, "perfile"},
		{50, 50, "perfile"},
		{200, 1, "perfile"},
		{200, 10, "perfile"},
		{200, 100, "perfile"},
	}

	var results []ampResult
	for _, s := range scenarios {
		for _, mode := range []string{table.WriteModeCopyOnWrite, table.WriteModeMergeOnRead} {
			results = append(results, runAmpScenario(t, ctx, mode, s.m, R, s.k, payloadBytes, s.scatter))
		}
	}

	// Correctness sanity, per the engine-agnostic property: COW must produce
	// zero delete files; MOR must produce delete files.
	for _, r := range results {
		if r.mode == table.WriteModeCopyOnWrite {
			require.Zerof(t, r.deleteManifestFiles, "COW %s must produce zero delete files (found %d)", r.name, r.deleteManifestFiles)
			require.Zerof(t, r.addedDeleteFiles, "COW %s must not report added-delete-files", r.name)
		} else {
			require.Positivef(t, r.deleteManifestFiles, "MOR %s must produce delete files", r.name)
		}
	}

	// ---- Report ----
	t.Log("")
	t.Logf("Seed layout: R=%d rows/file, payload=%d bytes/row, contiguous non-overlapping id ranges per file (clustered key)", R, payloadBytes)
	t.Log("")
	t.Logf("%-28s %-13s | %6s %10s | %6s %10s %6s | %5s %8s | %8s %8s | %8s | %9s",
		"scenario", "mode", "files", "seedBytes", "+files", "+bytes", "-files", "-recs", "-bytes", "delFiles", "amp(x)", "wall", "tblRewr%")
	t.Log(strings.Repeat("-", 160))

	for _, r := range results {
		perRowBytes := float64(r.seedBytes) / float64(r.m*r.r)
		logicalBytes := perRowBytes * float64(r.k)

		var ampX float64
		var writtenBytes int64
		if r.mode == table.WriteModeCopyOnWrite {
			writtenBytes = r.addedFilesSize
		} else {
			writtenBytes = r.addedFilesSize // delete-file bytes written
		}
		if logicalBytes > 0 {
			ampX = float64(writtenBytes) / logicalBytes
		}
		tblRewrPct := 100 * float64(r.removedFilesSize) / float64(r.seedBytes)

		t.Logf("%-28s %-13s | %6d %10d | %6d %10d %6d | %5d %8d | %8d %8.1f | %8s | %8.2f",
			r.name, r.mode,
			r.seedFiles, r.seedBytes,
			r.addedDataFiles, r.addedFilesSize, r.deletedDataFiles,
			r.deletedRecords, r.removedFilesSize,
			r.deleteManifestFiles, ampX, r.elapsed.Round(time.Microsecond).String(), tblRewrPct)
	}
	t.Log("")
	t.Log("amp(x)   = bytes written by the mutation / bytes logically changed (~K rows)")
	t.Log("tblRewr% = removed-files-size / total seeded bytes (fraction of table COW rewrote)")
}
