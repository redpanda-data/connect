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
	iofs "io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// This file pins the fixes for a batch of validated review findings. Each test
// is written so it FAILS if the corresponding fix is reverted — asserting the
// new behaviour, never a tautology.

// blockedCommitCatalog is a memCatalog whose CommitTable blocks until the
// commit's own context is cancelled (the batcher's background context, torn
// down by committer.Close). It makes the Submit-with-cancelled-ctx tests
// deterministic: the batcher's Submit selects between the response channel and
// the caller's ctx.Done, so against a fast in-memory catalog a queued commit
// could occasionally complete before Submit observes the cancellation and the
// select would race. With the commit parked, the only ready case is the
// caller's cancelled ctx — which is exactly the production shape being pinned:
// the request is still in flight when the caller gives up.
type blockedCommitCatalog struct {
	*memCatalog
}

func (*blockedCommitCatalog) CommitTable(ctx context.Context, _ table.Identifier, _ []table.Requirement, _ []table.Update) (table.Metadata, string, error) {
	<-ctx.Done()
	return nil, "", ctx.Err()
}

func (b *blockedCommitCatalog) snapshot() *table.Table {
	return table.New(b.ident, b.meta, b.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, b)
}

// TestCommitSubmitCtxCancelledIsAmbiguous pins the ambiguity classification of
// a batcher Submit that fails with the CALLER's context error: the batcher runs
// commits on its own background context, so the caller's cancellation does not
// stop the queued request — it may still land server-side. committer.Commit
// must therefore wrap a context.Canceled/DeadlineExceeded Submit error in
// rest.ErrCommitStateUnknown, so every downstream cleanup gate treats the
// outcome as possibly-landed. Without the wrap, the writer's cleanup would
// delete parquet files a still-landing snapshot may reference — table
// corruption.
func TestCommitSubmitCtxCancelledIsAmbiguous(t *testing.T) {
	logger := service.MockResources().Logger()

	// Committer level: a pure-append Commit under an already-cancelled ctx must
	// surface the unknown-state sentinel, not a bare context error.
	t.Run("commit error carries the unknown-state sentinel", func(t *testing.T) {
		_, mem := newTestTable(t)
		cat := &blockedCommitCatalog{memCatalog: mem}
		c, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 1},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer c.Close()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		df := synthDataFile(t, cat.snapshot().Spec(), fmt.Sprintf("%s/data/cancelled-%s.parquet", cat.location, uuid.New()))
		err = c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()})
		require.Error(t, err)
		assert.ErrorIs(t, err, rest.ErrCommitStateUnknown,
			"a Submit aborted by the caller's cancelled ctx is ambiguous — the queued commit may still land — and must be marked unknown-state")
		assert.ErrorIs(t, err, context.Canceled, "the underlying context error must remain inspectable")
	})

	// Writer level: after the ambiguous Write failure the written parquet files
	// must be left in place (cleanup skipped), because a still-landing snapshot
	// may reference them. LocalFS ignores ctx, so the parquet write itself
	// succeeds even under a cancelled ctx.
	t.Run("writer skips cleanup on the ambiguous outcome", func(t *testing.T) {
		_, mem := newTestTable(t)
		cat := &blockedCommitCatalog{memCatalog: mem}
		tbl := cat.snapshot()
		c, err := NewCommitter(tbl, cat, CommitConfig{MaxRetries: 1},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer c.Close()

		require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))
		w := &writer{table: tbl, committer: c, caseSensitive: true, logger: logger}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err = w.Write(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": 1})})
		require.Error(t, err)
		require.ErrorIs(t, err, rest.ErrCommitStateUnknown,
			"the writer must see the unknown-state sentinel so its cleanup gate engages")
		assert.Positive(t, countParquetFiles(t, tbl.Location()),
			"cleanup must be SKIPPED: the queued commit may still land and reference the written files")
	})
}

// TestCOWTimeColumnOwnLocationRoundTrip pins jsonLeafValue's TimeType
// canonicalisation: the wall clock is formatted in the value's OWN location and
// truncated to microseconds ("15:04:05.999999"), matching the shredder insert
// path's convertTime storage semantics (14:30 EST stores 14:30, not the 19:30
// UTC shift). Formatting tm.UTC() would shift the stored value/key by the zone
// offset, and emitting more than 6 fractional digits makes Arrow's time64[us]
// parsers (and the filter-literal parser) reject the value outright.
func TestCOWTimeColumnOwnLocationRoundTrip(t *testing.T) {
	ctx := t.Context()
	est := time.FixedZone("EST", -5*3600)

	t.Run("unit canonicalisation", func(t *testing.T) {
		// Own-location wall clock: 14:30 EST encodes as 14:30, not 19:30 UTC.
		got, err := jsonLeafValue(iceberg.PrimitiveTypes.Time, time.Date(2026, 3, 4, 14, 30, 0, 0, est), nil, false)
		require.NoError(t, err)
		assert.Equal(t, "14:30:00", got,
			"a TIME value must encode the wall clock in its own location, matching the insert path's storage")

		// Nanosecond precision truncates to at most 6 fractional digits.
		got, err = jsonLeafValue(iceberg.PrimitiveTypes.Time, time.Date(2000, 1, 1, 23, 59, 59, 123456789, time.UTC), nil, false)
		require.NoError(t, err)
		assert.Equal(t, "23:59:59.123456", got,
			"sub-microsecond precision must be truncated to 6 digits so Arrow's time64[us] parser accepts the value")
	})

	// End-to-end through a real copy-on-write upsert: a non-UTC TIME data
	// column value must scan back as the microsecond wall clock in its own
	// location.
	t.Run("data column end-to-end", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(),
			iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.Time})
		tbl, cat := newCOWTable(t, sc)
		wall := time.Date(2026, 3, 4, 14, 30, 0, 0, est)
		_ = seedCOWRows(t, ctx, tbl, cat, "id", []map[string]any{{"id": int64(1), "v": wall}})

		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm

		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"id": int64(1), "v": wall}),
		}))

		final := cat.snapshot()
		assert.Equal(t, 1, countRowsWithID(t, ctx, final, "id", 1), "the upsert must rewrite in place, not duplicate")
		got, present := cowReadColJSON(t, ctx, final, "v", 1)
		require.True(t, present, "the upserted TIME value must be present")
		assert.JSONEq(t, `"14:30:00"`, got,
			"the stored TIME must be the wall clock in the value's own location (14:30 EST -> 14:30, not 19:30)")
	})

	// Merge key: a non-UTC, nanosecond-precision TIME key must build a filter
	// (previously >6 fractional digits failed the literal parse) and must
	// actually replace the keyed row — no silent duplicate.
	t.Run("merge key with non-UTC nanosecond precision", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Time, Required: true},
			iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
		)
		key := time.Date(2000, 1, 1, 14, 30, 0, 123456789, est)
		other := time.Date(2000, 1, 1, 1, 2, 3, 0, time.UTC)

		final := driveCOWKeyed(t, ctx, sc,
			[]map[string]any{
				{"k": key, "payload": "old"},
				{"k": other, "payload": "untouched"},
			},
			service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": key, "payload": "NEW"}),
			})
		byPay := invertByPayload(t, final)
		require.Len(t, final, 2, "the upsert must replace the keyed row in place — a silent no-match would leave 3 rows")
		assert.Contains(t, byPay, "NEW")
		assert.Contains(t, byPay, "untouched")
		assert.NotContains(t, byPay, "old", "the pre-upsert row must be overwritten, not duplicated")
		assert.JSONEq(t, `"14:30:00.123456"`, byPay["NEW"],
			"the stored key must be the own-location wall clock truncated to microseconds")
	})
}

// TestCOWStringTimestampDataColumn pins cowMassage's primitive-leaf handling of
// STRING values in timestamp/timestamptz DATA columns: they are parsed via
// bloblang.ValueAsTimestamp, matching the insert path (the shredder's
// convertTimestamp falls back to the same parse for non-numeric values).
// Previously the rewrite path errored "timestamp column requires a time value,
// got string", deterministically failing every mutating batch over data the
// table already accepted on insert.
func TestCOWStringTimestampDataColumn(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0, cowIDField(),
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.TimestampTz})
	tbl, cat := newCOWTable(t, sc)

	// Both the seed insert and the upsert supply the timestamp as an RFC 3339
	// string, exactly as a JSON CDC feed does.
	_ = seedCOWRows(t, ctx, tbl, cat, "id", []map[string]any{
		{"id": int64(1), "ts": "2026-01-02T03:04:05.678901Z"},
	})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	const upTS = "2026-03-04T05:06:07.123456Z"
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": int64(1), "ts": upTS}),
	}), "a string timestamp in a data column must be parsed like the insert path, not rejected")

	final := cat.snapshot()
	require.Equal(t, 1, countRowsWithID(t, ctx, final, "id", 1), "exactly one row must remain")
	got, present := readTimestampMicros(t, ctx, final, "ts", 1)
	require.True(t, present, "the upserted timestamp must be present")
	assert.Equal(t, time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC).UnixMicro(), got,
		"the stored timestamp must equal the parsed RFC 3339 string at microsecond precision")
}

// readBinaryColByID reads the raw bytes of binary column col for the row whose
// int64 "id" == id, copying them out of the Arrow buffer so they stay valid
// after release.
func readBinaryColByID(t testing.TB, ctx context.Context, tbl *table.Table, col string, id int64) ([]byte, bool) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		bArr := rec.Column(rec.Schema().FieldIndices(col)[0]).(*array.Binary)
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) != id {
				continue
			}
			if bArr.IsNull(r) {
				return nil, false
			}
			return append([]byte(nil), bArr.Value(r)...), true
		}
	}
	return nil, false
}

// readStringColByID reads string column col for the row whose int64 "id" == id.
func readStringColByID(t testing.TB, ctx context.Context, tbl *table.Table, col string, id int64) (string, bool) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		sArr := rec.Column(rec.Schema().FieldIndices(col)[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) != id {
				continue
			}
			if sArr.IsNull(r) {
				return "", false
			}
			return sArr.Value(r), true
		}
	}
	return "", false
}

// TestCOWBinaryAndStringByteColumns pins cowMassage's byte-shape
// canonicalisation for the JSON->Arrow round trip:
//
//   - binary columns: Arrow's BinaryBuilder base64-DECODES every JSON string,
//     so both []byte AND string values must be base64-encoded on the way in.
//     The dangerous case is a Go string that happens to BE valid base64 (like
//     "deadbeef"): passing it through unchanged silently stored its base64
//     decoding — corruption with no error anywhere.
//   - string columns fed []byte: json.Marshal base64-encodes []byte, so the
//     stored text would be the base64 of what the insert path stores verbatim;
//     cowMassage must convert to string(b) first.
func TestCOWBinaryAndStringByteColumns(t *testing.T) {
	ctx := t.Context()

	runUpsert := func(t *testing.T, sc *iceberg.Schema, row func() map[string]any) *table.Table {
		t.Helper()
		tbl, cat := newCOWTable(t, sc)
		_ = seedCOWRows(t, ctx, tbl, cat, "id", []map[string]any{row()})

		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", row())}))
		return cat.snapshot()
	}

	t.Run("base64-looking string into binary, []byte into string", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(),
			iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
			iceberg.NestedField{ID: 3, Name: "s", Type: iceberg.PrimitiveTypes.String},
		)
		// "deadbeef" IS valid base64 — the old code silently stored its
		// 6-byte base64 decoding instead of the 8 raw characters.
		row := func() map[string]any {
			return map[string]any{"id": int64(1), "b": "deadbeef", "s": []byte("raw-bytes")}
		}
		final := runUpsert(t, sc, row)

		require.Equal(t, 1, countRowsWithID(t, ctx, final, "id", 1), "the upsert must rewrite in place")
		gotB, ok := readBinaryColByID(t, ctx, final, "b", 1)
		require.True(t, ok, "the binary value must be present")
		assert.Equal(t, []byte("deadbeef"), gotB,
			"the binary column must hold the raw 8 characters byte-for-byte, not their base64 decoding")
		gotS, ok := readStringColByID(t, ctx, final, "s", 1)
		require.True(t, ok, "the string value must be present")
		assert.Equal(t, "raw-bytes", gotS,
			"a []byte fed to a string column must be stored as its text, not base64-encoded by json.Marshal")
	})

	t.Run("non-UTF8 bytes round-trip byte-for-byte", func(t *testing.T) {
		sc := iceberg.NewSchema(0, cowIDField(),
			iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.Binary})
		raw := []byte{0x00, 0xFF, 0x10}
		row := func() map[string]any {
			return map[string]any{"id": int64(1), "b": append([]byte(nil), raw...)}
		}
		final := runUpsert(t, sc, row)

		require.Equal(t, 1, countRowsWithID(t, ctx, final, "id", 1))
		gotB, ok := readBinaryColByID(t, ctx, final, "b", 1)
		require.True(t, ok)
		assert.Equal(t, raw, gotB, "non-UTF8 []byte must round-trip byte-for-byte through the base64 leg")
	})
}

// readFloat64ColByID reads float64 column col for the row whose int64 "id" ==
// id.
func readFloat64ColByID(t testing.TB, ctx context.Context, tbl *table.Table, col string, id int64) (float64, bool) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		fArr := rec.Column(rec.Schema().FieldIndices(col)[0]).(*array.Float64)
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) != id {
				continue
			}
			if fArr.IsNull(r) {
				return 0, false
			}
			return fArr.Value(r), true
		}
	}
	return 0, false
}

// TestCOWNaNInfFloatDataColumn pins cowMassage's handling of non-finite floats
// in the copy-on-write rewrite: NaN and ±Inf are encoded as their strconv
// string forms, which Arrow's float builders parse back via strconv.ParseFloat.
// Previously json.Marshal rejected them ("json: unsupported value"), failing
// the WHOLE mutating batch even though the insert path stores non-finite
// doubles without complaint (parquet supports them).
func TestCOWNaNInfFloatDataColumn(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0, cowIDField(),
		iceberg.NestedField{ID: 2, Name: "v", Type: iceberg.PrimitiveTypes.Float64})
	tbl, cat := newCOWTable(t, sc)
	_ = seedCOWRows(t, ctx, tbl, cat, "id", []map[string]any{
		{"id": int64(1), "v": float64(0)},
		{"id": int64(2), "v": float64(0)},
		{"id": int64(3), "v": float64(0)},
	})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": int64(1), "v": math.NaN()}),
		cowMsg(t, "upsert", map[string]any{"id": int64(2), "v": math.Inf(1)}),
		cowMsg(t, "upsert", map[string]any{"id": int64(3), "v": math.Inf(-1)}),
	}), "non-finite float values must not fail the mutating batch")

	final := cat.snapshot()
	v1, ok := readFloat64ColByID(t, ctx, final, "v", 1)
	require.True(t, ok)
	assert.True(t, math.IsNaN(v1), "NaN must round-trip as NaN, got %v", v1)
	v2, ok := readFloat64ColByID(t, ctx, final, "v", 2)
	require.True(t, ok)
	assert.True(t, math.IsInf(v2, 1), "+Inf must round-trip as +Inf, got %v", v2)
	v3, ok := readFloat64ColByID(t, ctx, final, "v", 3)
	require.True(t, ok)
	assert.True(t, math.IsInf(v3, -1), "-Inf must round-trip as -Inf, got %v", v3)
}

// listParquetPaths returns the slash-form paths of every .parquet file under
// dir.
func listParquetPaths(t testing.TB, dir string) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	require.NoError(t, filepath.WalkDir(dir, func(p string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			out[filepath.ToSlash(p)] = struct{}{}
		}
		return nil
	}))
	return out
}

// TestCleanupSparesSupersededSnapshotFiles pins the orphan-cleanup reference
// scan (committer.referencedCandidatePaths): it must consider EVERY snapshot
// committed since the start marker, not only the current one. A file committed
// by our own landed-but-superseded snapshot can be dropped from the CURRENT
// snapshot by an external rewrite (compaction, another writer) within the retry
// window — but the historical snapshot still references it, and time travel /
// incremental readers depend on it. A current-only scan would classify such a
// file as an orphan and delete data a committed snapshot depends on.
func TestCleanupSparesSupersededSnapshotFiles(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	require.NotNil(t, seedTbl.CurrentSnapshot())
	// The snapshot current when the commit call began: the scan's start marker.
	startID := seedTbl.CurrentSnapshot().SnapshotID
	before := listParquetPaths(t, seedTbl.Location())

	// A real committed copy-on-write upsert lands snapshot S1, whose files F we
	// identify as the parquet files that appeared on disk.
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm
	require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))

	var landed []string
	for p := range listParquetPaths(t, seedTbl.Location()) {
		if _, ok := before[p]; !ok {
			landed = append(landed, p)
		}
	}
	require.NotEmpty(t, landed, "the upsert must have written new data files")
	for _, p := range landed {
		require.Equal(t, 1, countDataFileRefs(t, ctx, cat.snapshot(), p),
			"precondition: S1 (current) must reference the landed file %s", p)
	}

	// External compaction: a DIRECT delete-all transaction through a plain
	// table handle bound to the same catalog (not through the committer)
	// replaces the current snapshot so it no longer references F, while the
	// historical S1 still does.
	ext := cat.snapshot().NewTransaction()
	require.NoError(t, ext.Delete(ctx, iceberg.AlwaysTrue{}, nil))
	_, err = ext.Commit(ctx)
	require.NoError(t, err)
	for _, p := range landed {
		require.Zero(t, countDataFileRefs(t, ctx, cat.snapshot(), p),
			"precondition: the external rewrite must drop %s from the CURRENT snapshot — otherwise this test cannot distinguish the fixed scan from a current-only one", p)
	}
	// Adopt the post-rewrite metadata, as the committer's own reload does
	// before cleanup runs in production.
	comm.table = cat.snapshot()

	// Control: a recorded path no snapshot references IS an orphan and must go.
	orphan := fmt.Sprintf("%s/data/orphan-%s.parquet", seedTbl.Location(), uuid.New())
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0o644))

	candidates := map[string]struct{}{orphan: {}}
	for _, p := range landed {
		candidates[p] = struct{}{}
	}
	comm.cleanupOrphanedOverwriteFiles(ctx, candidates, startID)

	for _, p := range landed {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr,
			"file %s committed by our landed-but-superseded snapshot must survive cleanup after an external rewrite", p)
	}
	_, statErr := os.Stat(orphan)
	assert.True(t, os.IsNotExist(statErr), "a recorded file no snapshot references must be removed in the same sweep")

	// Subset property: the reference scan returns ONLY candidates that are
	// actually referenced — a referenced file is in, a path no snapshot knows is
	// out.
	refs, err := comm.referencedCandidatePaths(ctx, map[string]struct{}{
		landed[0]: {},
		seedTbl.Location() + "/data/nonexistent.parquet": {},
	}, startID)
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{landed[0]: {}}, refs,
		"referencedCandidatePaths must return exactly the referenced subset of its candidates")
}

// TestAppendCommitIdempotentViaCommitID pins the append path's commit-id
// idempotency token (doCommit): a pure-append commit that lands server-side but
// reports an ambiguous outcome must be detected on the retry via
// committedSnapshotHasID and short-circuit to success WITHOUT re-committing.
// The path-keyed dropAlreadyCommitted check alone cannot provide this: an
// external compaction can rewrite the landed files away between the landing and
// our reload, making the paths invisible — the token in the (historical)
// snapshot summary is what still proves the batch landed.
func TestAppendCommitIdempotentViaCommitID(t *testing.T) {
	ctx := t.Context()
	c, cat := newScriptedCommitter(t, commitLandThenUnknown)

	df := synthDataFile(t, cat.snapshot().Spec(), fmt.Sprintf("%s/data/append-idem-%s.parquet", cat.location, uuid.New()))
	require.NoError(t, c.Commit(ctx, CommitInput{Files: []iceberg.DataFile{df}, SchemaID: c.currentSchemaID()}))

	assert.Equal(t, 1, cat.calls,
		"a landed append must be detected via the commit-id token, not re-committed")
	assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()),
		"the append snapshot must carry the commit-id token exactly once — its presence is what makes the retry short-circuit possible")
}

// TestProbeTimestampEncodingHonoursContext pins the per-manifest-entry ctx
// check in probeTimestampEncoding: the probe can walk (and open footers for) up
// to a thousand data files serially while the router holds the table entry's
// lock, so a cancelled context must abort it promptly with an error wrapping
// context.Canceled rather than grinding through the remaining object-store
// reads.
func TestProbeTimestampEncodingHonoursContext(t *testing.T) {
	ctx := t.Context()
	// A table whose schema HAS a no-tz timestamp column and holds a committed
	// parquet data file, so the probe genuinely reaches the manifest-entry walk.
	_, cat := newEncTable(t, encTestSchema(), nil)
	seedEncTimestampFile(t, ctx, cat, icebergx.TimestampEncodingSpec)

	// Control: with a live context the same table probes cleanly, proving the
	// cancelled run's error comes from the ctx check and not the harness.
	enc, err := probeTimestampEncoding(ctx, cat.snapshot())
	require.NoError(t, err)
	require.Equal(t, icebergx.TimestampEncodingSpec, enc)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = probeTimestampEncoding(cancelled, cat.snapshot())
	require.Error(t, err, "an already-cancelled context must abort the probe")
	assert.ErrorIs(t, err, context.Canceled, "the probe error must wrap the context error")
}

// TestCOWFixedColumnMirrorsInsertPath pins cowMassage's FixedType leaf: Arrow's
// FixedSizeBinary JSON reader base64-decodes every JSON string (same convention
// as binary), and the insert path accepts ONLY []byte for a fixed column — so
// the rewrite path must base64-encode []byte and reject anything else loudly.
// Without the arm, a plain string fell through the old encoder's default
// verbatim branch and Arrow base64-decoded it: silently stored wrong bytes when
// the text happened to be valid base64, or failed the whole batch when it
// wasn't — while the identical input errors cleanly on insert.
func TestCOWFixedColumnMirrorsInsertPath(t *testing.T) {
	w := &writer{}
	fixed4 := iceberg.FixedTypeOf(4)

	got, err := w.cowMassage(fixed4, 2, []byte{1, 2, 3, 4}, nil)
	require.NoError(t, err)
	assert.Equal(t, "AQIDBA==", got,
		"[]byte must be base64-encoded so Arrow's FixedSizeBinary JSON reader decodes back the exact bytes")

	_, err = w.cowMassage(fixed4, 2, "AQID", nil)
	require.Error(t, err, "a string into a fixed column must be rejected, mirroring the insert path")
	assert.Contains(t, err.Error(), "cannot convert string to fixed",
		"the rejection must match the insert path's error shape")
}
