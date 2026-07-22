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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file is the CORR-1 proof: a copy-on-write UPSERT carrying a numeric-epoch
// value in a temporal DATA column must be interpreted with the SAME schema
// metadata the insert path uses, landing at the correct instant instead of being
// rejected (the old behaviour) or corrupted into year ~56755 (the seconds
// misinterpretation). The equivalence between the copy-on-write and insert unit
// conversions is pinned at the unit level by
// shredder.TestNumericTemporalToTimeMatchesConvert; here we prove it end-to-end
// through writer.Write.

const cowTSMetaKey = "schema"

// cowWriterWithResolver is cowWriter plus a resolver keyed on cowTSMetaKey, so a
// message's schema_metadata drives the temporal unit interpretation exactly as
// the insert path's shredder does. requireMeta toggles strict mode
// (require_schema_metadata).
func cowWriterWithResolver(t testing.TB, tbl *table.Table, requireMeta bool, idFields ...string) *writer {
	t.Helper()
	w := cowWriter(t, tbl, idFields...)
	w.resolver = newTypeResolver(cowTSMetaKey, nil, true, service.MockResources().Logger())
	w.requireSchemaMetadata = requireMeta
	// messagesToParquet (insert fast path) logs coerce decisions into this map;
	// NewWriter initialises it in production, so mirror that here for safety.
	w.coerceLoggedFieldIDs = map[int]struct{}{}
	return w
}

// tsMillisMeta describes an {id BIGINT, ts timestamp-millis} record.
func tsMillisMeta() *schema.Common {
	return &schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "id", Type: schema.Int64},
			{
				Name: "ts", Type: schema.Timestamp,
				// AdjustToUTC:false so the metadata maps to a plain (non-tz)
				// TIMESTAMP, matching the table column below. The unit scaling —
				// the CORR-1 crux — is independent of AdjustToUTC.
				Logical: &schema.LogicalParams{Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMillis, AdjustToUTC: false}},
			},
		},
	}
}

// cowMsgMeta builds a copy-on-write message with the given row_operation and,
// when meta is non-nil, the schema_metadata that disambiguates numeric temporal
// units.
func cowMsgMeta(t testing.TB, op string, meta *schema.Common, row map[string]any) *service.Message {
	t.Helper()
	msg := cowMsg(t, op, row)
	if meta != nil {
		msg.MetaSetMut(cowTSMetaKey, meta.ToAny())
	}
	return msg
}

// seedCOWRows appends rows encoded exactly as the copy-on-write rewrite would
// (via buildCOWRecordFactory), returning the updated handle. Values are supplied
// as native Go values (e.g. time.Time), matching how a seed insert would arrive.
func seedCOWRows(t testing.TB, ctx context.Context, tbl *table.Table, cat *memCatalog, idField string, rows []map[string]any) *table.Table {
	t.Helper()
	w := cowWriter(t, cat.snapshot(), idField)
	factory, err := w.buildCOWRecordFactory(tbl.Schema(), toBatch(t, rows))
	require.NoError(t, err)
	rdr, err := factory()
	require.NoError(t, err)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.Append(ctx, rdr, nil))
	rdr.Release()
	next, err := tx.Commit(ctx)
	require.NoError(t, err)
	return next
}

// readTimestampMicros returns the raw microseconds-since-epoch stored in an
// Iceberg TIMESTAMP column for the row whose int64 "id" == id. Reading the raw
// Arrow int64 (rather than a formatted string) makes the assertion exact and
// unit-explicit.
func readTimestampMicros(t testing.TB, ctx context.Context, tbl *table.Table, col string, id int64) (int64, bool) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		tsArr := rec.Column(rec.Schema().FieldIndices(col)[0]).(*array.Timestamp)
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) != id {
				continue
			}
			if tsArr.IsNull(r) {
				return 0, false
			}
			return int64(tsArr.Value(r)), true
		}
	}
	return 0, false
}

// TestCOWNumericEpochTimestampDataColumnUpsert is the CORR-1 proof. A timestamp
// DATA column receives a bare numeric millis value under schema_metadata
// declaring timestamp-millis. The copy-on-write upsert must store it as
// millis*1000 microseconds — the exact result the insert path produces for the
// identical input — and must NOT reject it or misread it as unix seconds.
func TestCOWNumericEpochTimestampDataColumnUpsert(t *testing.T) {
	ctx := t.Context()

	const epochMillis = int64(1_730_000_000_000) // 2024-10-27T03:33:20Z
	const wantMicros = epochMillis * 1_000       // correct: millis -> micros
	const secondsMisread = epochMillis * 1_000_000

	meta := tsMillisMeta()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)

	seedTbl, cat := newCOWTable(t, sc)
	sentinel := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = seedCOWRows(t, ctx, seedTbl, cat, "id", []map[string]any{
		{"id": int64(1), "ts": sentinel},
		{"id": int64(2), "ts": sentinel},
	})

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriterWithResolver(t, cat.snapshot(), false, "id")
	w.committer = comm

	// COW UPSERT of id=1 with a numeric millis value + schema metadata.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsgMeta(t, "upsert", meta, map[string]any{"id": int64(1), "ts": epochMillis}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")

	got, present := readTimestampMicros(t, ctx, final, "ts", 1)
	require.True(t, present, "the upserted row must be present (upsert must not be rejected)")
	assert.Equal(t, wantMicros, got, "numeric millis must be stored as millis*1000 micros, matching the insert path")
	assert.NotEqual(t, secondsMisread, got, "must not be misinterpreted as unix seconds (year ~56755 corruption)")
	// Sanity: the stored instant is the intended one.
	assert.Equal(t, time.UnixMilli(epochMillis).UTC(), time.UnixMicro(got).UTC())

	// The untouched row must survive the rewrite unchanged.
	got2, present2 := readTimestampMicros(t, ctx, final, "ts", 2)
	require.True(t, present2, "untouched row must survive")
	assert.Equal(t, sentinel.UnixMicro(), got2)

	// wantMicros (= millis*1000) is exactly what the shredder insert path
	// produces for this input: convertTimestamp scales millis->micros via
	// scaleTimestampNumeric, the very helper NumericTemporalToTime reuses. That
	// insert==copy-on-write equivalence is pinned unit-for-unit by
	// shredder.TestNumericTemporalToTimeMatchesConvert, so asserting wantMicros
	// here is asserting the copy-on-write result mirrors the insert path.
}

// TestCOWNumericEpochTimestampRequireSchemaMetadata pins the
// require_schema_metadata (strict) behaviour for the copy-on-write data path: a
// numeric temporal DATA value is accepted when metadata is present and rejected
// with a coherent error when it is absent — mirroring the shredder insert path.
func TestCOWNumericEpochTimestampRequireSchemaMetadata(t *testing.T) {
	ctx := t.Context()
	const epochMillis = int64(1_730_000_000_000)

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
	)

	t.Run("accepted with metadata", func(t *testing.T) {
		seedTbl, cat := newCOWTable(t, sc)
		sentinel := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		_ = seedCOWRows(t, ctx, seedTbl, cat, "id", []map[string]any{{"id": int64(1), "ts": sentinel}})

		comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriterWithResolver(t, cat.snapshot(), true, "id")
		w.committer = comm

		require.NoError(t, w.Write(ctx, service.MessageBatch{
			cowMsgMeta(t, "upsert", tsMillisMeta(), map[string]any{"id": int64(1), "ts": epochMillis}),
		}))
		got, present := readTimestampMicros(t, ctx, cat.snapshot(), "ts", 1)
		require.True(t, present)
		assert.Equal(t, epochMillis*1_000, got)
	})

	t.Run("rejected without metadata", func(t *testing.T) {
		seedTbl, cat := newCOWTable(t, sc)
		sentinel := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		_ = seedCOWRows(t, ctx, seedTbl, cat, "id", []map[string]any{{"id": int64(1), "ts": sentinel}})

		comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriterWithResolver(t, cat.snapshot(), true, "id")
		w.committer = comm

		// No schema metadata on the message: strict mode must reject the numeric.
		err = w.Write(ctx, service.MessageBatch{
			cowMsgMeta(t, "upsert", nil, map[string]any{"id": int64(1), "ts": epochMillis}),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "require_schema_metadata=true")
		// And the message must not be misleadingly framed as an identifier column.
		assert.NotContains(t, err.Error(), "identifier column")
	})
}
