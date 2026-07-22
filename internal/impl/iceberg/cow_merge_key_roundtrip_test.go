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
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file proves the copy-on-write merge-key (filter-literal) path for every
// key type broadened in Tier 1.2 that could be made safe: date, time,
// timestamp, timestamptz, and uuid. Each case drives a REAL copy-on-write
// upsert+delete batch through writer.Write (which builds the overwrite filter
// via buildCOWFilter -> cowKeyLiteral) and asserts that the filter matched the
// INTENDED rows — the upserted key's row was rewritten, the deleted key's row
// was removed, and an untouched key survived unchanged. decimal is proven GATED
// (see TestCOWDecimalMergeKeyGated) because iceberg-go's overwrite filter panics
// on a decimal literal.
//
// The load-bearing guard is against the CON-490 silent-no-match bug: if the
// filter literal's encoding disagreed with the stored value, the overwrite
// would match nothing, leaving a duplicate of the upserted key and failing to
// delete — which these assertions (exact final row set, keyed identity)
// catch. A test that only checked "no error" would NOT catch that.

// seedMergeKeyRows appends the given rows as one plain-data-file snapshot,
// encoding each value exactly as the copy-on-write rewrite would (via
// buildCOWRecordFactory), and returns the updated table handle.
func seedMergeKeyRows(t testing.TB, ctx context.Context, tbl *table.Table, cat *memCatalog, rows []map[string]any) *table.Table {
	t.Helper()
	w := cowWriter(t, cat.snapshot(), "k")
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

// scanKeyPayload scans the table into a map keyed by the canonical JSON form of
// the "k" column, valued by the "payload" string. JSON is a type-agnostic,
// deterministic form for identifying which key each surviving row carries.
func scanKeyPayload(t testing.TB, ctx context.Context, tbl *table.Table) map[string]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	out := map[string]string{}
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		kArr := rec.Column(rec.Schema().FieldIndices("k")[0])
		pArr := rec.Column(rec.Schema().FieldIndices("payload")[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			b, err := json.Marshal(kArr.GetOneForMarshal(r))
			require.NoError(t, err)
			pay := ""
			if pArr.IsValid(r) {
				pay = pArr.Value(r)
			}
			out[string(b)] = pay
		}
	}
	return out
}

// invertByPayload maps payload -> key JSON, asserting payloads are unique.
func invertByPayload(t testing.TB, m map[string]string) map[string]string {
	t.Helper()
	out := make(map[string]string, len(m))
	for k, pay := range m {
		_, dup := out[pay]
		require.False(t, dup, "payloads must be unique to identify rows")
		out[pay] = k
	}
	return out
}

func TestCOWMergeKeyRoundTrip(t *testing.T) {
	ctx := t.Context()

	// timeOf builds a UTC time.Time; date-only / time-only cases just fix the
	// irrelevant component.
	ts := func(y int, mo time.Month, d, h, mi, s, ns int) time.Time {
		return time.Date(y, mo, d, h, mi, s, ns, time.UTC)
	}

	cases := []struct {
		name    string
		keyType iceberg.Type
		k1      any // untouched
		k2      any // upserted (payload one/two/three -> TWO)
		k3      any // deleted
	}{
		{
			name:    "date",
			keyType: iceberg.PrimitiveTypes.Date,
			k1:      ts(2026, 1, 1, 0, 0, 0, 0),
			k2:      ts(2026, 6, 15, 0, 0, 0, 0),
			k3:      ts(2026, 12, 31, 0, 0, 0, 0),
		},
		{
			name:    "time",
			keyType: iceberg.PrimitiveTypes.Time,
			k1:      ts(2000, 1, 1, 1, 2, 3, 0),
			k2:      ts(2000, 1, 1, 12, 13, 14, 500000000), // 12:13:14.5
			k3:      ts(2000, 1, 1, 23, 59, 59, 999999000), // microsecond precision
		},
		{
			name:    "timestamp",
			keyType: iceberg.PrimitiveTypes.Timestamp,
			k1:      ts(2026, 1, 1, 0, 0, 0, 0),
			k2:      ts(2026, 6, 15, 10, 20, 30, 123456000), // microsecond precision
			k3:      ts(2026, 12, 31, 23, 59, 59, 0),
		},
		{
			name:    "timestamptz",
			keyType: iceberg.PrimitiveTypes.TimestampTz,
			k1:      ts(2026, 1, 1, 0, 0, 0, 0),
			k2:      ts(2026, 6, 15, 10, 20, 30, 123456000),
			k3:      ts(2026, 12, 31, 23, 59, 59, 0),
		},
		{
			name:    "uuid",
			keyType: iceberg.PrimitiveTypes.UUID,
			k1:      "f47ac10b-58cc-0372-8567-0e02b2c3d479",
			k2:      "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			k3:      "00000000-0000-0000-0000-000000000001",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sc := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "k", Type: c.keyType, Required: true},
				iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
			)
			tbl, cat := newCOWTable(t, sc)

			// Seed three distinct-key rows.
			tbl = seedMergeKeyRows(t, ctx, tbl, cat, []map[string]any{
				{"k": c.k1, "payload": "one"},
				{"k": c.k2, "payload": "two"},
				{"k": c.k3, "payload": "three"},
			})

			seedMap := scanKeyPayload(t, ctx, tbl)
			require.Len(t, seedMap, 3, "seed must produce three rows")
			seedByPay := invertByPayload(t, seedMap)

			// Drive a real copy-on-write upsert(k2)+delete(k3) batch.
			comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
			require.NoError(t, err)
			defer comm.Close()
			w := cowWriter(t, cat.snapshot(), "k")
			w.committer = comm

			require.NoError(t, w.Write(ctx, service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": c.k2, "payload": "TWO"}),
				cowMsg(t, "delete", map[string]any{"k": c.k3}),
			}))

			final := cat.snapshot()

			// Copy-on-write must never leave delete files.
			assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")

			finalMap := scanKeyPayload(t, ctx, final)
			// Exactly the untouched row and the upserted row remain. If the
			// filter had matched nothing (the silent-no-match bug), we would see
			// a duplicate k2 ("two" AND "TWO") and a surviving k3 ("three").
			require.Len(t, finalMap, 2, "exactly the untouched and upserted rows must remain")
			finalByPay := invertByPayload(t, finalMap)

			// The row now carrying "TWO" must be keyed by k2 (proves the upsert
			// rewrote the intended key, not a different or no row).
			assert.Equal(t, seedByPay["two"], finalByPay["TWO"], "upserted row must carry the k2 key")
			// The untouched row must be exactly k1, still "one".
			assert.Equal(t, seedByPay["one"], finalByPay["one"], "untouched row must keep the k1 key and value")
			// k3 must be gone and the stale k2 value must not linger.
			assert.NotContains(t, finalByPay, "three", "deleted key k3 must be removed")
			assert.NotContains(t, finalByPay, "two", "the pre-upsert k2 value must be overwritten, not duplicated")
		})
	}
}

// driveCOWKeyed seeds a "k"/"payload" table with seed rows, then runs a single
// copy-on-write batch of the given upsert and (optional) delete messages through
// writer.Write, returning the final key-JSON -> payload map. It is the shared
// driver for the merge-key representation and type round-trip tests below.
func driveCOWKeyed(t testing.TB, ctx context.Context, sc *iceberg.Schema, seed []map[string]any, batch service.MessageBatch) map[string]string {
	t.Helper()
	tbl, cat := newCOWTable(t, sc)
	_ = seedMergeKeyRows(t, ctx, tbl, cat, seed)

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "k")
	w.committer = comm
	require.NoError(t, w.Write(ctx, batch))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	return scanKeyPayload(t, ctx, final)
}

// TestCOWInt64KeyRepresentationsRoundTrip (T-2) drives real copy-on-write
// upsert+delete on an int64-keyed table where the mutating message supplies the
// key in each JSON-decoded representation an upstream might produce —
// json.Number beyond 2^53, float64, and string. Each must match the intended
// seeded row (no duplicate, no missed delete), proving cowValueToInt64's
// canonicalisation agrees with the stored integer encoding.
func TestCOWInt64KeyRepresentationsRoundTrip(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)

	cases := []struct {
		name          string
		upKey, delKey any
		upWant        int64 // canonical key expected to carry "UP"
	}{
		// 2^53+1 and 2^53+3, exact only as json.Number/string, never float64.
		{"json.Number beyond 2^53", json.Number("9007199254740993"), json.Number("9007199254740995"), 9007199254740993},
		{"float64 within 2^53", float64(42), float64(43), 42},
		{"string", "100", "101", 100},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Seed the two touched keys plus a distinct untouched key.
			seed := []map[string]any{
				{"k": c.upWant, "payload": "up-old"},
				{"k": mustInt64(t, c.delKey), "payload": "del"},
				{"k": int64(7), "payload": "untouched"},
			}
			final := driveCOWKeyed(t, ctx, sc, seed, service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": c.upKey, "payload": "UP"}),
				cowMsg(t, "delete", map[string]any{"k": c.delKey}),
			})
			byPay := invertByPayload(t, final)
			require.Len(t, final, 2, "exactly the upserted and untouched rows must remain")
			assert.Contains(t, byPay, "UP")
			assert.Contains(t, byPay, "untouched")
			assert.NotContains(t, byPay, "up-old", "the upsert must overwrite in place, not duplicate")
			assert.NotContains(t, byPay, "del", "the delete must remove the intended row")
			assert.JSONEq(t, fmt.Sprintf("%d", c.upWant), byPay["UP"], "the upserted row must carry the intended key")
		})
	}
}

// mustInt64 converts a test key representation to its canonical int64 so the seed
// stores the same logical key the mutating message references.
func mustInt64(t testing.TB, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		require.NoError(t, err)
		return i
	case float64:
		return int64(n)
	case string:
		var i int64
		_, err := fmt.Sscan(n, &i)
		require.NoError(t, err)
		return i
	default:
		t.Fatalf("unsupported key representation %T", v)
		return 0
	}
}

// TestCOWFloatKeyValueRejected pins that a non-integer or out-of-exact-range
// float64 int-key value is rejected loudly by the filter path rather than
// silently corrupting the merge key (cowValueToInt64).
func TestCOWFloatKeyValueRejected(t *testing.T) {
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriter(t, tbl, "k")

	for _, v := range []float64{1.5, 1e300} {
		_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": v})})
		require.Error(t, err, "float64 %v must be rejected as an int64 key", v)
	}
}

// TestCOWInt32KeyRoundTrip proves an int32 merge key round-trips through a real
// copy-on-write upsert+delete (previously untested as a key type).
func TestCOWInt32KeyRoundTrip(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	final := driveCOWKeyed(t, ctx, sc,
		[]map[string]any{
			{"k": int64(1), "payload": "one"},
			{"k": int64(2), "payload": "two"},
			{"k": int64(3), "payload": "three"},
		},
		service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": int64(2), "payload": "TWO"}),
			cowMsg(t, "delete", map[string]any{"k": int64(3)}),
		})
	byPay := invertByPayload(t, final)
	require.Len(t, final, 2)
	assert.Equal(t, "2", byPay["TWO"])
	assert.Equal(t, "1", byPay["one"])
	assert.NotContains(t, byPay, "three")
	assert.NotContains(t, byPay, "two")
}

// TestCOWBooleanMergeKeyGated pins the deliberate gate on boolean merge keys.
// Investigating the "boolean-key round-trip" ask (T-2) surfaced that a boolean
// key cannot round-trip: iceberg-go's copy-on-write rewrite
// (rewriteFilesWithFilter) evaluates the negated key predicate against each data
// file's rows, and that row-level evaluation is unimplemented for BOOL, failing
// the whole overwrite mid-commit ("not implemented: unsupported type BOOL").
// Rather than let that reach a real table, cowKeyLiteral rejects a boolean key
// up front — the same treatment as decimal. boolean remains fine as a non-key
// column (see TestCOWColumnTypeRoundTrip) and as a merge-on-read key.
func TestCOWBooleanMergeKeyGated(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Bool, Required: true},
	)
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriter(t, tbl, "k")
	_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": true})})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boolean is not a supported copy-on-write merge key")
	assert.Contains(t, err.Error(), "merge-on-read")
}

// TestCOWSubMicrosecondTimestampKeyTruncation (CORR-5) documents and verifies the
// truncation behaviour of a timestamp merge key carrying sub-microsecond
// (nanosecond) precision. Iceberg TIMESTAMP is microsecond-resolution, so the
// nanoseconds are dropped — but the invariant that matters is that the filter
// literal and the stored value truncate IDENTICALLY, so the key still matches its
// row. cowKeyLiteral builds the literal from time.Time.UnixMicro (truncating),
// while the stored value goes RFC3339Nano -> Arrow timestamp[us] (also
// truncating); this test proves they agree by matching the intended row exactly.
//
// Consequence (documented, not a bug): two instants differing only below the
// microsecond collapse to the same key. That is inherent to a microsecond column
// and is consistent between filter and storage, so it never causes the CON-490
// silent-no-match — it only means sub-microsecond precision is not part of the
// key identity.
func TestCOWSubMicrosecondTimestampKeyTruncation(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	// A key with nanosecond precision beyond microseconds (…123456789).
	key := time.Date(2026, 6, 15, 10, 20, 30, 123456789, time.UTC)
	other := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	final := driveCOWKeyed(t, ctx, sc,
		[]map[string]any{
			{"k": key, "payload": "old"},
			{"k": other, "payload": "untouched"},
		},
		service.MessageBatch{
			// Upsert the SAME nanosecond-precision instant: it must match the
			// seeded row, proving filter and storage truncate identically.
			cowMsg(t, "upsert", map[string]any{"k": key, "payload": "NEW"}),
		})
	byPay := invertByPayload(t, final)
	require.Len(t, final, 2, "the sub-microsecond key must match its row (no duplicate) — filter and storage truncate identically")
	assert.Contains(t, byPay, "NEW")
	assert.Contains(t, byPay, "untouched")
	assert.NotContains(t, byPay, "old")
	// The stored key is the microsecond truncation of the nanosecond input.
	wantKeyJSON, err := json.Marshal(key.UTC().Truncate(time.Microsecond))
	require.NoError(t, err)
	assert.JSONEq(t, string(wantKeyJSON), byPay["NEW"], "the stored key is the nanosecond input truncated to microseconds")
}

// TestCOWDecimalMergeKeyGated pins the deliberate gate on decimal merge keys.
// iceberg-go's overwrite filter routes a decimal literal through its substrait
// conversion, which panics (toDecimalLiteral asserts *iceberg.DecimalType while
// DecimalLiteral.Type returns a value DecimalType). Rather than let that panic
// reach a real overwrite, cowKeyLiteral rejects a decimal key up front with an
// actionable error. This is the "could not make safe" case for Tier 1.2; the
// decimal *column* type (Tier 1.1) and decimal merge-on-read keys are
// unaffected.
func TestCOWDecimalMergeKeyGated(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.DecimalTypeOf(10, 2), Required: true},
	)
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriter(t, tbl, "k")
	_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": "1.00"})})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decimal is not a supported copy-on-write merge key")
	assert.Contains(t, err.Error(), "merge-on-read")
}
