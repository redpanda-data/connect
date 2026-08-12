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
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/shredder"
	"github.com/redpanda-data/connect/v4/internal/impl/parquet/parquetdecimal"
)

// This file pins the value-encoding parity invariant between the four write
// paths of the iceberg output: for any (iceberg type, Go input shape), the
// insert path (P1, the shredder), the copy-on-write rewrite (P2, cowMassage),
// the merge-on-read equality-delete keys (P3, writeEqualityDeletes), and the
// copy-on-write filter literals (P4, cowKeyLiteral) must all agree with what
// P1 stores — or reject the input loudly and consistently. P2/P3/P4 all
// encode through the shared jsonLeafValue, so the anti-drift table below pins
// jsonLeafValue against the shredder shape by shape, and the end-to-end tests
// drive the real MOR/COW paths for the shapes that used to diverge.

// parityCaptureSink collects shredded leaf values so a test can read exactly
// what the insert path would store.
type parityCaptureSink struct{ values []shredder.ShreddedValue }

func (s *parityCaptureSink) EmitValue(sv shredder.ShreddedValue) error {
	s.values = append(s.values, sv)
	return nil
}

func (*parityCaptureSink) OnNewField(icebergx.Path, string, any) {}

// shredSingleLeaf runs the real insert-path conversion (RecordShredder over a
// single-column schema) and returns the parquet value it would store.
func shredSingleLeaf(t *testing.T, typ iceberg.Type, in any) parquet.Value {
	t.Helper()
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: typ})
	sink := &parityCaptureSink{}
	require.NoError(t, shredder.NewRecordShredder(sc, true).Shred(map[string]any{"v": in}, sink))
	require.Len(t, sink.values, 1)
	return sink.values[0].Value
}

// twosComplementBigInt decodes the big-endian two's-complement byte encoding
// both parquet decimals and the shredder use for unscaled values.
func twosComplementBigInt(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b))*8))
	}
	return n
}

// canonicalFromParquet reduces the insert path's parquet value to a
// type-appropriate comparable form.
func canonicalFromParquet(t *testing.T, typ iceberg.Type, pv parquet.Value) any {
	t.Helper()
	switch typ.(type) {
	case iceberg.StringType, iceberg.BinaryType:
		return string(pv.ByteArray())
	case iceberg.UUIDType:
		u, err := uuid.FromBytes(pv.ByteArray())
		require.NoError(t, err)
		return u.String()
	case iceberg.TimeType, iceberg.TimestampType, iceberg.TimestampTzType, iceberg.Int64Type:
		return pv.Int64()
	case iceberg.Int32Type:
		return int64(pv.Int32())
	case iceberg.DecimalType:
		return twosComplementBigInt(pv.ByteArray()).String()
	case iceberg.BooleanType:
		return pv.Boolean()
	}
	t.Fatalf("no parquet canonicaliser for %s", typ)
	return nil
}

// canonicalFromArrow reduces the mutation paths' Arrow cell (the result of
// jsonLeafValue -> JSON -> RecordFromJSON) to the same comparable form.
func canonicalFromArrow(t *testing.T, col arrow.Array) any {
	t.Helper()
	require.Equal(t, 1, col.Len())
	require.False(t, col.IsNull(0), "the encoded value must not decode as null")
	if ext, ok := col.(array.ExtensionArray); ok {
		// The uuid logical type decodes into an extension array whose storage
		// is the same 16 bytes parquet stores.
		fsb, ok := ext.Storage().(*array.FixedSizeBinary)
		require.True(t, ok, "unexpected extension storage %T", ext.Storage())
		u, err := uuid.FromBytes(fsb.Value(0))
		require.NoError(t, err)
		return u.String()
	}
	switch col := col.(type) {
	case *array.String:
		return col.Value(0)
	case *array.Binary:
		return string(col.Value(0))
	case *array.Time64:
		return int64(col.Value(0))
	case *array.Timestamp:
		return int64(col.Value(0))
	case *array.Int32:
		return int64(col.Value(0))
	case *array.Int64:
		return col.Value(0)
	case *array.Decimal128:
		return col.Value(0).BigInt().String()
	case *array.Boolean:
		return col.Value(0)
	}
	t.Fatalf("no arrow canonicaliser for %T", col)
	return nil
}

// mutationLeafViaArrow drives the mutation-path encoding end to end:
// jsonLeafValue, then the same JSON -> Arrow round trip the copy-on-write
// rewrite and the equality-delete writer perform, returning the decoded cell's
// canonical form.
func mutationLeafViaArrow(t *testing.T, typ iceberg.Type, in any) any {
	t.Helper()
	jv, err := jsonLeafValue(typ, in, nil, false)
	require.NoError(t, err)

	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "v", Type: typ})
	arrowSc, err := table.SchemaToArrowSchema(sc, nil, true, false)
	require.NoError(t, err)
	b, err := json.Marshal([]map[string]any{{"v": jv}})
	require.NoError(t, err)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, bytes.NewReader(b))
	require.NoError(t, err)
	defer rec.Release()
	return canonicalFromArrow(t, rec.Column(0))
}

// TestJSONLeafValueMatchesInsertPath is the anti-drift equivalence table: for
// each (type, input shape) it drives the REAL insert conversion (the shredder)
// and the REAL mutation conversion (jsonLeafValue + the Arrow JSON round trip)
// and asserts the stored values agree exactly. Every row is an input shape the
// audit found (or nearly found) diverging between the paths.
func TestJSONLeafValueMatchesInsertPath(t *testing.T) {
	est := time.FixedZone("EST", -5*3600)
	nsInstant := time.Date(2026, 6, 15, 10, 20, 30, 123456789, time.UTC)
	nsWallEST := time.Date(2026, 3, 4, 14, 30, 59, 987654321, est)
	uid := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")

	dec10p2 := iceberg.DecimalTypeOf(10, 2)
	decBytes := func(unscaled int64) []byte {
		return parquetdecimal.EncodeBytes(big.NewInt(unscaled), 10)
	}

	cases := []struct {
		name string
		typ  iceberg.Type
		in   func() any // fresh value per path ([]byte must not be shared)
	}{
		{"string from []byte", iceberg.PrimitiveTypes.String, func() any { return []byte("raw-bytes") }},
		{"string from ns time.Time keeps full precision", iceberg.PrimitiveTypes.String, func() any { return nsWallEST }},
		{"binary from string", iceberg.PrimitiveTypes.Binary, func() any { return "hello!" }},
		{"binary from base64-looking string", iceberg.PrimitiveTypes.Binary, func() any { return "deadbeef" }},
		{"binary from non-utf8 bytes", iceberg.PrimitiveTypes.Binary, func() any { return []byte{0x00, 0xFF, 0x10} }},
		{"uuid from canonical string", iceberg.PrimitiveTypes.UUID, func() any { return uid.String() }},
		{"uuid from 16 bytes", iceberg.PrimitiveTypes.UUID, func() any { return append([]byte(nil), uid[:]...) }},
		{"time from duration (avro time-micros decode)", iceberg.PrimitiveTypes.Time, func() any {
			return 4*time.Hour + 5*time.Minute + 6*time.Second + 7*time.Millisecond
		}},
		{"time from ns own-location time.Time", iceberg.PrimitiveTypes.Time, func() any { return nsWallEST }},
		{"timestamp from ns time.Time truncates alike", iceberg.PrimitiveTypes.Timestamp, func() any { return nsInstant }},
		{"timestamptz from ns time.Time truncates alike", iceberg.PrimitiveTypes.TimestampTz, func() any { return nsInstant }},
		{"decimal float tie rounds away from zero", dec10p2, func() any { return 0.125 }},
		{"decimal negative float tie", dec10p2, func() any { return -0.125 }},
		{"decimal scale-0 float tie", iceberg.DecimalTypeOf(10, 0), func() any { return 8.5 }},
		{"decimal non-tie float control", dec10p2, func() any { return 123.45 }},
		{"decimal from float32", dec10p2, func() any { return float32(1.5) }},
		{"decimal from exact-width bytes", dec10p2, func() any { return decBytes(12345) }},
		{"decimal from negative exact-width bytes", dec10p2, func() any { return decBytes(-13) }},
		{"decimal from uint64", dec10p2, func() any { return uint64(7) }},
		{"int64 from json.Number beyond 2^53", iceberg.PrimitiveTypes.Int64, func() any { return json.Number("9007199254740993") }},
		{"int32 from int64", iceberg.PrimitiveTypes.Int32, func() any { return int64(-123) }},
		{"boolean from bool", iceberg.PrimitiveTypes.Bool, func() any { return true }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			inserted := canonicalFromParquet(t, c.typ, shredSingleLeaf(t, c.typ, c.in()))
			mutated := mutationLeafViaArrow(t, c.typ, c.in())
			assert.Equal(t, inserted, mutated,
				"the mutation paths must store exactly what the insert path stores for %T input", c.in())
		})
	}
}

// --- end-to-end regressions on the real MOR / COW paths ------------------------

// newMORWriter builds a merge-on-read writer whose row operation comes from the
// "op" metadata key (the shape cowMsg produces).
func newMORWriter(t testing.TB, tbl *table.Table, idFields ...string) *writer {
	t.Helper()
	return &writer{
		table:         tbl,
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			Operation:        mustInterp(t, `${! metadata("op") }`),
			IdentifierFields: idFields,
			MergeStrategy:    mergeStrategyMOR,
		},
		logger: service.MockResources().Logger(),
	}
}

// driveMORKeyed seeds the table with a real INSERT batch through the shredder
// insert path (so the stored bytes are exactly P1's), then applies the mutating
// batch as a second commit, returning the final table for scanning. Scans apply
// the equality deletes, so a key-encoding mismatch shows up as a duplicate row.
func driveMORKeyed(t testing.TB, ctx context.Context, sc *iceberg.Schema, idFields []string, seed []map[string]any, batch service.MessageBatch) *table.Table {
	t.Helper()
	tbl, cat := newCOWTable(t, sc)
	require.NoError(t, os.MkdirAll(filepath.Join(tbl.Location(), "data"), 0o755))

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := newMORWriter(t, cat.snapshot(), idFields...)
	w.committer = comm

	seedBatch := make(service.MessageBatch, 0, len(seed))
	for _, row := range seed {
		seedBatch = append(seedBatch, cowMsg(t, "insert", row))
	}
	require.NoError(t, w.Write(ctx, seedBatch), "seeding via the insert path must succeed")
	require.NoError(t, w.Write(ctx, batch), "the mutating batch must succeed")
	return cat.snapshot()
}

// TestMORUpsertStringKeyBytesInput pins the []byte-into-string-key fix on the
// REAL merge-on-read path: a bloblang mapping like `root.id = content()` feeds
// the identifier as []byte, which the insert path stores as the raw text.
// Before the shared canonicaliser, the equality-delete key went through
// json.Marshal's base64 encoding, matched nothing, and every upsert duplicated
// the row forever.
func TestMORUpsertStringKeyBytesInput(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	final := driveMORKeyed(t, ctx, sc, []string{"k"},
		[]map[string]any{
			{"k": []byte("key-1"), "payload": "old"},
			{"k": []byte("other"), "payload": "untouched"},
		},
		service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": []byte("key-1"), "payload": "NEW"}),
		})

	byPay := invertByPayload(t, scanKeyPayload(t, ctx, final))
	require.Len(t, byPay, 2, "the upsert must replace the keyed row — a base64-encoded delete key would match nothing and leave 3 rows")
	assert.Equal(t, `"key-1"`, byPay["NEW"], "the new row must carry the raw text key")
	assert.Contains(t, byPay, "untouched")
	assert.NotContains(t, byPay, "old", "the prior version must be equality-deleted, not duplicated")
}

// TestMORUpsertDecimalTieFloatKey pins the decimal rounding-mode fix on the
// REAL merge-on-read path: 0.125 at scale 2 is an exact binary tie, which the
// insert path stores as unscaled 13 (half-away-from-zero). The old
// strconv.FormatFloat key encoding rounded half-to-even to "0.12", so the
// delete matched nothing and the upsert duplicated the row.
func TestMORUpsertDecimalTieFloatKey(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.DecimalTypeOf(10, 2), Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seed := []map[string]any{
		{"k": 0.125, "payload": "old"},
		{"k": float64(5), "payload": "untouched"},
	}

	final := driveMORKeyed(t, ctx, sc, []string{"k"}, seed,
		service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": 0.125, "payload": "NEW"}),
		})

	byPay := invertByPayload(t, scanKeyPayload(t, ctx, final))
	require.Len(t, byPay, 2, "the tie-valued key must store AND delete consistently — a half-to-even key encoding would leave 3 rows")
	assert.Contains(t, byPay, "NEW")
	assert.Contains(t, byPay, "untouched")
	assert.NotContains(t, byPay, "old", "the prior version must be equality-deleted, not duplicated")
	assert.Equal(t, `"0.13"`, byPay["NEW"], "the stored key must be the half-away-from-zero rounding the insert path applies")
}

// TestMORUpsertTimestampNsPrecisionKey pins the µs-truncation fix on the REAL
// merge-on-read path: a timestamp merge key carrying nanosecond precision
// (time.Now()-derived, common in CDC decoders) used to be formatted at full
// nanosecond precision, which Arrow's timestamp[us] JSON parser hard-errors on
// — every mutating batch failed deterministically and the pipeline stalled on
// redelivery. The key must instead truncate to microseconds, exactly like the
// insert path's UnixMicro, and therefore match the stored row.
func TestMORUpsertTimestampNsPrecisionKey(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	key := time.Date(2026, 6, 15, 10, 20, 30, 123456789, time.UTC) // ns precision
	other := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	final := driveMORKeyed(t, ctx, sc, []string{"k"},
		[]map[string]any{
			{"k": key, "payload": "old"},
			{"k": other, "payload": "untouched"},
		},
		service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": key, "payload": "NEW"}),
		})

	byPay := invertByPayload(t, scanKeyPayload(t, ctx, final))
	require.Len(t, byPay, 2, "the ns-precision key must both encode (no batch failure) and match its row (no duplicate)")
	assert.Contains(t, byPay, "NEW")
	assert.Contains(t, byPay, "untouched")
	assert.NotContains(t, byPay, "old")
}

// TestStringTimestampMergeKeyEndToEnd pins RFC 3339 string timestamps as merge
// KEYS under both strategies: the insert path accepts them (bloblang parse), so
// a JSON CDC feed whose "updated_at" key arrives as text used to insert fine
// and then stall on the first upsert (both the equality-delete key encoder and
// the copy-on-write filter literal rejected strings).
func TestStringTimestampMergeKeyEndToEnd(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	const keyText = "2026-06-15T10:20:30.123456Z"
	const otherText = "2026-01-01T00:00:00Z"
	seed := []map[string]any{
		{"k": keyText, "payload": "old"},
		{"k": otherText, "payload": "untouched"},
	}
	mutation := func() service.MessageBatch {
		return service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": keyText, "payload": "NEW"}),
		}
	}
	verify := func(t *testing.T, final map[string]string) {
		t.Helper()
		byPay := invertByPayload(t, final)
		require.Len(t, byPay, 2, "the string-keyed upsert must replace the keyed row in place")
		assert.Contains(t, byPay, "NEW")
		assert.Contains(t, byPay, "untouched")
		assert.NotContains(t, byPay, "old")
	}

	t.Run("merge-on-read", func(t *testing.T) {
		final := driveMORKeyed(t, ctx, sc, []string{"k"}, seed, mutation())
		verify(t, scanKeyPayload(t, ctx, final))
	})

	t.Run("copy-on-write", func(t *testing.T) {
		verify(t, driveCOWKeyed(t, ctx, sc, seed, mutation()))
	})
}

// TestIntegerStringRejectedOnMutationPaths pins the deliberate removal of the
// numeric-string leniency: the insert path rejects "42" into an int/long
// column, so every mutation path must reject it identically instead of letting
// mutating batches commit values that the equivalent insert refuses (a
// flip-flopping pipeline that only progresses on keyed batches).
func TestIntegerStringRejectedOnMutationPaths(t *testing.T) {
	ctx := t.Context()
	const wantErr = "unsupported value type string for integer column"

	t.Run("copy-on-write filter literal", func(t *testing.T) {
		sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64, Required: true})
		tbl := newTypedKeyTableFromSchema(t, sc)
		w := cowWriter(t, tbl, "k")
		_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": "42"})})
		require.Error(t, err)
		assert.Contains(t, err.Error(), wantErr)
	})

	t.Run("copy-on-write data column", func(t *testing.T) {
		w := &writer{}
		_, err := w.cowMassage(iceberg.PrimitiveTypes.Int64, 1, "42", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), wantErr)
	})

	t.Run("merge-on-read equality-delete key", func(t *testing.T) {
		tbl, _ := newTestTable(t) // schema: id int64
		w := newDeleteWriter(t, tbl)
		_, err := w.writeEqualityDeletes(ctx, service.MessageBatch{structuredMsg(t, map[string]any{"id": "42"})})
		require.Error(t, err)
		assert.Contains(t, err.Error(), wantErr)
	})
}

// TestMORNsTimestampIdentifierRejected pins the merge-on-read gate on V3
// nanosecond-timestamp identifier fields, mirroring the copy-on-write schema
// gate: the delete-key encoder is microsecond-resolution end to end, so an ns
// key could silently match nothing — it must be refused loudly up front.
func TestMORNsTimestampIdentifierRejected(t *testing.T) {
	for _, typ := range []iceberg.Type{
		iceberg.PrimitiveTypes.TimestampNs,
		iceberg.PrimitiveTypes.TimestampTzNs,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			sc := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "k", Type: typ, Required: true},
			)
			w := &writer{caseSensitive: true, rowOpCfg: RowOpConfig{IdentifierFields: []string{"k"}}}
			_, _, err := w.deleteRecordFields(sc, iceberg.UnpartitionedSpec)
			require.Error(t, err, "an ns-timestamp identifier must be rejected, not silently no-matched")
			assert.Contains(t, err.Error(), typ.String())
			assert.Contains(t, err.Error(), "equality-delete key")
			assert.Contains(t, err.Error(), "microsecond", "the error must point at the fix")
		})
	}
}

// TestCOWMapIntegerKeyColumnRejected pins the map<int, ·> consequence of
// removing the integer string leniency: Go map keys arrive as strings, the
// insert path can never write such a map (its key conversion rejects strings),
// and now the copy-on-write rewrite rejects it identically instead of quietly
// storing what inserts refuse.
func TestCOWMapIntegerKeyColumnRejected(t *testing.T) {
	w := &writer{}
	mt := &iceberg.MapType{
		KeyID: 3, KeyType: iceberg.PrimitiveTypes.Int64,
		ValueID: 4, ValueType: iceberg.PrimitiveTypes.String, ValueRequired: false,
	}
	_, err := w.cowMassage(mt, 2, map[string]any{"5": "x"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `map key "5"`, "the error must name the offending key")
	assert.Contains(t, err.Error(), "unsupported value type string for integer column")
}
