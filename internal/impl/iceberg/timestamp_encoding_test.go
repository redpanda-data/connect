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
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// encTestSchema is the canonical schema for timestamp-encoding tests: an id
// column, a no-tz `timestamp` column (the one the encodings disagree on) and a
// `timestamptz` column (identical in both encodings).
func encTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp, Required: false},
		iceberg.NestedField{ID: 3, Name: "tstz", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
	)
}

// newEncTable builds an unpartitioned v2 table for sc over an in-memory
// catalog and the local filesystem, with extra table properties merged in.
func newEncTable(t testing.TB, sc *iceberg.Schema, extra iceberg.Properties) (*table.Table, *memCatalog) {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	props := iceberg.Properties{table.PropertyFormatVersion: "2"}
	maps.Copy(props, extra)
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "enc"},
		location:         location,
	}
	return cat.snapshot(), cat
}

var encSeedTime = time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)

// seedEncTimestampFile appends one real parquet data file to the table through
// the connector's own shredder append path (writer.writeDataFiles) using the
// given timestamp encoding, and returns its path. Seeding with
// TimestampEncodingLegacy therefore produces a file byte-annotated exactly as
// pre-fix connector releases wrote it.
func seedEncTimestampFile(t testing.TB, ctx context.Context, cat *memCatalog, enc icebergx.TimestampEncoding) string {
	t.Helper()
	tbl := cat.snapshot()
	// LocalFS does not create the data/ subdir implicitly.
	require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))
	w := &writer{table: tbl, caseSensitive: true, tsEncoding: enc, logger: service.MockResources().Logger()}
	files, err := w.writeDataFiles(ctx, service.MessageBatch{structuredMsg(t, map[string]any{
		"id": int64(1), "ts": encSeedTime, "tstz": encSeedTime,
	})})
	require.NoError(t, err)
	require.Len(t, files, 1)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.AddDataFiles(ctx, files, nil, table.WithoutAutoNameMapping(), table.WithoutDuplicateCheck()))
	_, err = tx.Commit(ctx)
	require.NoError(t, err)
	return files[0].FilePath()
}

// footerTimestampAdjusted opens a parquet file's footer and returns fieldID ->
// isAdjustedToUTC for every leaf carrying a TIMESTAMP logical type.
func footerTimestampAdjusted(t testing.TB, path string) map[int]bool {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	info, err := f.Stat()
	require.NoError(t, err)
	pf, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	require.NoError(t, err)
	out := map[int]bool{}
	for _, el := range pf.Metadata().Schema {
		if lt, ok := el.LogicalType.Get(); ok && lt.Timestamp != nil {
			out[int(el.FieldID)] = lt.Timestamp.IsAdjustedToUTC
		}
	}
	return out
}

// --- append-path uniformity ------------------------------------------------

// TestAppendWritesResolvedTimestampEncoding pins the writer-side guarantee: a
// table resolved to an encoding gets EVERY new file annotated with it. In
// legacy mode the no-tz column keeps isAdjustedToUTC=true (identical to
// pre-fix output, so an existing table never becomes mixed); in spec mode it
// is false. `timestamptz` is UTC-adjusted in both.
func TestAppendWritesResolvedTimestampEncoding(t *testing.T) {
	ctx := t.Context()

	t.Run("legacy", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		path := seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)
		ann := footerTimestampAdjusted(t, path)
		assert.Equal(t, map[int]bool{2: true, 3: true}, ann,
			"legacy mode must annotate the no-tz timestamp column isAdjustedToUTC=true (pre-fix bytes) and timestamptz true")
	})

	t.Run("spec", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		path := seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingSpec)
		ann := footerTimestampAdjusted(t, path)
		assert.Equal(t, map[int]bool{2: false, 3: true}, ann,
			"spec mode must annotate the no-tz timestamp column isAdjustedToUTC=false and timestamptz true")
	})
}

// --- resolution: property present -------------------------------------------

func TestResolveTimestampEncodingPropertyPresent(t *testing.T) {
	ctx := t.Context()

	t.Run("spec", func(t *testing.T) {
		tbl, cat := newEncTable(t, encTestSchema(), iceberg.Properties{icebergx.TimestampEncodingProperty: "spec"})
		enc, out, err := resolveTimestampEncoding(ctx, tbl, reloadFn(cat), nil)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Same(t, tbl, out, "a present property must be used as-is, with no stamp commit")
	})

	t.Run("legacy", func(t *testing.T) {
		tbl, cat := newEncTable(t, encTestSchema(), iceberg.Properties{icebergx.TimestampEncodingProperty: "legacy"})
		enc, _, err := resolveTimestampEncoding(ctx, tbl, reloadFn(cat), nil)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingLegacy, enc)
	})

	t.Run("unknown value fails loud", func(t *testing.T) {
		tbl, cat := newEncTable(t, encTestSchema(), iceberg.Properties{icebergx.TimestampEncodingProperty: "sideways"})
		_, _, err := resolveTimestampEncoding(ctx, tbl, reloadFn(cat), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), icebergx.TimestampEncodingProperty)
		assert.Contains(t, err.Error(), "sideways")
	})

	// The property, when present, must win WITHOUT touching any data file:
	// files pinned legacy stay legacy even if unreadable.
	t.Run("no probe when property present", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), iceberg.Properties{icebergx.TimestampEncodingProperty: "legacy"})
		path := seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)
		require.NoError(t, os.Remove(path), "removing the data file so any probe attempt would fail")
		enc, _, err := resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), nil)
		require.NoError(t, err, "resolution must not read data files when the property is present")
		assert.Equal(t, icebergx.TimestampEncodingLegacy, enc)
	})
}

// --- resolution: property absent (footer-probe bootstrap + stamp) ------------

func TestResolveTimestampEncodingBootstrap(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	stampedValue := func(cat *memCatalog) string {
		return cat.snapshot().Properties()[icebergx.TimestampEncodingProperty]
	}

	t.Run("no no-tz timestamp columns resolves spec", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "tstz", Type: iceberg.PrimitiveTypes.TimestampTz},
		)
		tbl, cat := newEncTable(t, sc, nil)
		enc, out, err := resolveTimestampEncoding(ctx, tbl, reloadFn(cat), logger)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Equal(t, "spec", stampedValue(cat), "the decision must be stamped onto the table")
		assert.Equal(t, "spec", out.Properties()[icebergx.TimestampEncodingProperty], "the returned table must carry the stamp")
	})

	t.Run("empty table resolves spec", func(t *testing.T) {
		tbl, cat := newEncTable(t, encTestSchema(), nil)
		enc, _, err := resolveTimestampEncoding(ctx, tbl, reloadFn(cat), logger)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Equal(t, "spec", stampedValue(cat))
	})

	t.Run("legacy data file resolves legacy", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)
		enc, _, err := resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingLegacy, enc,
			"a table whose files carry isAdjustedToUTC=true must pin legacy")
		assert.Equal(t, "legacy", stampedValue(cat))
	})

	t.Run("spec data file resolves spec", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingSpec)
		enc, _, err := resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Equal(t, "spec", stampedValue(cat))
	})

	t.Run("unreadable data file fails loud", func(t *testing.T) {
		tbl, cat := newEncTable(t, encTestSchema(), nil)
		// Register a data file whose content is not parquet: the probe must
		// error rather than guess an encoding.
		junkPath := filepath.Join(tbl.Location(), "data", "junk.parquet")
		require.NoError(t, os.MkdirAll(filepath.Dir(junkPath), 0o755))
		require.NoError(t, os.WriteFile(junkPath, []byte("not parquet"), 0o644))
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFiles(ctx, []iceberg.DataFile{synthDataFile(t, tbl.Spec(), filepath.ToSlash(junkPath))}, nil,
			table.WithoutAutoNameMapping(), table.WithoutDuplicateCheck()))
		_, err := tx.Commit(ctx)
		require.NoError(t, err)

		_, _, err = resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.Error(t, err, "an unreadable footer must fail resolution, not guess")
		assert.Contains(t, err.Error(), "probing parquet footer")
		assert.Empty(t, stampedValue(cat), "no stamp may be committed when the probe fails")
	})

	t.Run("stamp makes re-resolution probe-free", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		path := seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)

		enc, _, err := resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.NoError(t, err)
		require.Equal(t, icebergx.TimestampEncodingLegacy, enc)
		require.Equal(t, "legacy", stampedValue(cat))

		// Remove the data file from disk: a second resolution (e.g. a new
		// process) must ride the stamped property and never probe again.
		require.NoError(t, os.Remove(path))
		enc, _, err = resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.NoError(t, err, "re-resolution must use the stamp, not the (now missing) file")
		assert.Equal(t, icebergx.TimestampEncodingLegacy, enc)
	})

	// A data file that predates the timestamp column (added later by schema
	// evolution) has no timestamp leaf to inspect; when no file carries the
	// column the table resolves spec — no legacy file can exist for it.
	t.Run("files without the timestamp column resolve spec", func(t *testing.T) {
		idOnly := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		)
		_, cat := newEncTable(t, idOnly, nil)
		tbl := cat.snapshot()
		require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))
		w := &writer{table: tbl, caseSensitive: true, logger: service.MockResources().Logger()}
		files, err := w.writeDataFiles(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": int64(1)})})
		require.NoError(t, err)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.AddDataFiles(ctx, files, nil, table.WithoutAutoNameMapping(), table.WithoutDuplicateCheck()))
		_, err = tx.Commit(ctx)
		require.NoError(t, err)

		// Evolve the schema: add the ts column AFTER the file was written.
		tx = cat.snapshot().NewTransaction()
		us := tx.UpdateSchema(true, false)
		us.AddColumn([]string{"ts"}, iceberg.PrimitiveTypes.Timestamp, "", false, nil)
		require.NoError(t, us.Commit())
		_, err = tx.Commit(ctx)
		require.NoError(t, err)

		enc, _, err := resolveTimestampEncoding(ctx, cat.snapshot(), reloadFn(cat), logger)
		require.NoError(t, err)
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Equal(t, "spec", stampedValue(cat))
	})
}

// --- stamping race ------------------------------------------------------------

// TestStampTimestampEncodingRace covers two writers bootstrapping the same
// table concurrently: our SetProperties commit loses, and the outcome depends
// on what the winner stamped.
func TestStampTimestampEncodingRace(t *testing.T) {
	ctx := t.Context()
	// Schema without timestamp columns: the probe trivially resolves spec with
	// no file access, isolating the stamping behaviour under test.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)

	failingTable := func(t *testing.T) *table.Table {
		_, cat := newEncTable(t, sc, nil)
		flaky := &flakyCatalog{memCatalog: cat, failuresLeft: 1 << 30, failErr: errors.New("commit conflict")}
		return flaky.snapshot()
	}

	t.Run("winner stamped the same value", func(t *testing.T) {
		winner, _ := newEncTable(t, sc, iceberg.Properties{icebergx.TimestampEncodingProperty: "spec"})
		reload := func(context.Context) (*table.Table, error) { return winner, nil }
		enc, out, err := resolveTimestampEncoding(ctx, failingTable(t), reload, nil)
		require.NoError(t, err, "losing the stamp race to an agreeing writer must be benign")
		assert.Equal(t, icebergx.TimestampEncodingSpec, enc)
		assert.Same(t, winner, out, "the reloaded (stamped) table must be adopted")
	})

	t.Run("winner stamped a different value", func(t *testing.T) {
		winner, _ := newEncTable(t, sc, iceberg.Properties{icebergx.TimestampEncodingProperty: "legacy"})
		reload := func(context.Context) (*table.Table, error) { return winner, nil }
		_, _, err := resolveTimestampEncoding(ctx, failingTable(t), reload, nil)
		require.Error(t, err, "a disagreeing concurrent stamp must fail loud")
		assert.Contains(t, err.Error(), "concurrent writer")
	})

	t.Run("no concurrent stamp propagates the commit error", func(t *testing.T) {
		bare, _ := newEncTable(t, sc, nil)
		reload := func(context.Context) (*table.Table, error) { return bare, nil }
		_, _, err := resolveTimestampEncoding(ctx, failingTable(t), reload, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "commit conflict")
	})
}

// --- copy-on-write guard --------------------------------------------------------

// TestCOWLegacyTimestampGuard pins the guard that keeps mutating copy-on-write
// off legacy-pinned tables with no-tz timestamp columns: iceberg-go reads their
// UTC-adjusted files back as timestamptz and the rewrite fails with a cryptic
// "cannot promote timestamptz to timestamp" mid-commit — the guard converts
// that into an upfront, actionable error before any file is written.
func TestCOWLegacyTimestampGuard(t *testing.T) {
	ctx := t.Context()

	newCOWEncWriter := func(t *testing.T, cat *memCatalog, enc icebergx.TimestampEncoding) *writer {
		t.Helper()
		comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3, SkipFormatUpgrade: true}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		w.tsEncoding = enc
		return w
	}

	t.Run("mutating write on legacy table errors upfront", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)
		w := newCOWEncWriter(t, cat, icebergx.TimestampEncodingLegacy)

		err := w.Write(ctx, service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": int64(1), "ts": encSeedTime, "tstz": encSeedTime}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "legacy UTC-adjusted parquet encoding",
			"the guard must name the problem")
		assert.Contains(t, err.Error(), icebergx.TimestampEncodingProperty+"=spec",
			"the guard must give the migration path")
		assert.Contains(t, err.Error(), "merge-on-read",
			"the guard must offer the strategy alternative")

		// Delete-only mutations rewrite files too and must hit the same guard.
		err = w.Write(ctx, service.MessageBatch{cowMsg(t, "delete", map[string]any{"id": int64(1)})})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "legacy UTC-adjusted parquet encoding")
	})

	t.Run("insert-only batch on legacy table succeeds", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)
		w := newCOWEncWriter(t, cat, icebergx.TimestampEncodingLegacy)

		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsg(t, "insert", map[string]any{"id": int64(2), "ts": encSeedTime, "tstz": encSeedTime}),
		}), "insert-only copy-on-write is a plain append and must not be guarded")

		// Uniformity: the appended file must carry the legacy annotation.
		final := cat.snapshot()
		snap := final.CurrentSnapshot()
		require.NotNil(t, snap)
		fsys, err := final.FS(ctx)
		require.NoError(t, err)
		manifests, err := snap.Manifests(fsys)
		require.NoError(t, err)
		checked := 0
		for _, m := range manifests {
			for entry, err := range m.Entries(fsys, true) {
				require.NoError(t, err)
				ann := footerTimestampAdjusted(t, entry.DataFile().FilePath())
				assert.Equal(t, map[int]bool{2: true, 3: true}, ann,
					"every file of a legacy table must stay legacy-annotated: %s", entry.DataFile().FilePath())
				checked++
			}
		}
		assert.Equal(t, 2, checked, "expected the seed file and the appended file")
	})

	t.Run("mutating write on spec table unaffected", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingSpec)
		w := newCOWEncWriter(t, cat, icebergx.TimestampEncodingSpec)

		upsertAt := encSeedTime.Add(time.Hour)
		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": int64(1), "ts": upsertAt, "tstz": upsertAt}),
		}), "copy-on-write over spec-encoded files must keep working")
		assert.Equal(t, 1, countRowsWithID(t, ctx, cat.snapshot(), "id", 1), "the upsert must not duplicate the row")
	})

	t.Run("legacy table without timestamp columns unaffected", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
			iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
		)
		_, cat := newEncTable(t, sc, nil)
		seed := appendCOWRows(t, ctx, cat.snapshot(), map[int64]string{1: "one"})
		_ = seed
		w := newCOWEncWriter(t, cat, icebergx.TimestampEncodingLegacy)

		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": int64(1), "payload": "ONE"}),
		}), "the guard must only fire when the schema actually has a no-tz timestamp column")
		assert.Equal(t, map[int64]string{1: "ONE"}, scanRows(t, ctx, cat.snapshot()))
	})

	t.Run("merge-on-read on legacy table unaffected", func(t *testing.T) {
		_, cat := newEncTable(t, encTestSchema(), nil)
		seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingLegacy)

		comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := &writer{
			table:         cat.snapshot(),
			committer:     comm,
			caseSensitive: true,
			tsEncoding:    icebergx.TimestampEncodingLegacy,
			rowOpCfg: RowOpConfig{
				Operation:        mustInterp(t, `${! metadata("op") }`),
				IdentifierFields: []string{"id"},
				MergeStrategy:    mergeStrategyMOR,
			},
			logger: service.MockResources().Logger(),
		}

		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": int64(1), "ts": encSeedTime.Add(time.Hour), "tstz": encSeedTime}),
		}), "merge-on-read must keep working on a legacy table (no file rewrites)")
		snap := cat.snapshot().CurrentSnapshot()
		require.NotNil(t, snap)
		assert.Equal(t, table.OpOverwrite, snap.Summary.Operation, "the upsert must land as a row-delta overwrite")
	})
}
