// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	benthosschema "github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file pins the fixes adopted from the copy-on-write review
// recommendations. Each test is written to FAIL if its fix were reverted, and
// documents the invariant the fix restores.

// --- dedup identity canonicalisation ---------------------------------------

// TestDedupKeyCanonicalisesShapes pins writer.dedupKeyer's identity source:
// the in-batch collapse of keyed operations derives each key's identity from
// jsonLeafValue's CANONICAL encoding — the same encoding matching and storage
// use — never from the raw Go shape. Canonically-equal keys arriving in
// different shapes within one batch ([]byte vs string, a nanosecond time.Time
// vs its microsecond UTC string) must collapse to ONE keyed operation per
// commit. An escape is silent data corruption: equality deletes never remove
// rows committed in the same snapshot, so a same-commit upsert+delete pair
// that escapes the collapse leaves the row alive after its own delete.
func TestDedupKeyCanonicalisesShapes(t *testing.T) {
	kvSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)

	payloadOf := func(msg *service.Message) string {
		v, err := msg.AsStructured()
		require.NoError(t, err)
		return v.(map[string]any)["payload"].(string)
	}

	// (a) Unit level: splitByOperation over two upserts of the SAME key in two
	// shapes ([]byte then string) must collapse to exactly one delete-side
	// entry and one surviving insert-side row — the LATER one. Keying on the
	// raw shapes would base64-encode the []byte spelling and let both survive.
	t.Run("unit collapse across byte and string shapes", func(t *testing.T) {
		tbl, _ := newCOWTable(t, kvSchema)
		w := newMORWriter(t, tbl, "k")

		inserts, deletes, counts, err := w.splitByOperation(service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": []byte("k1"), "payload": "first"}),
			cowMsg(t, "upsert", map[string]any{"k": "k1", "payload": "second"}),
		})
		require.NoError(t, err)
		require.Len(t, deletes, 1, "two shapes of one key must collapse to a single keyed operation")
		require.Len(t, inserts, 1, "only the later upsert may survive the collapse")
		assert.Equal(t, "second", payloadOf(inserts[0]), "the collapse must keep the LAST operation in batch order")
		assert.EqualValues(t, 1, counts.upserted, "metrics count post-collapse operations")

		// Control: genuinely different keys must NOT collapse.
		inserts, deletes, _, err = w.splitByOperation(service.MessageBatch{
			cowMsg(t, "upsert", map[string]any{"k": []byte("a"), "payload": "pa"}),
			cowMsg(t, "upsert", map[string]any{"k": "b", "payload": "pb"}),
		})
		require.NoError(t, err)
		assert.Len(t, deletes, 2, "distinct keys must each keep their operation")
		assert.Len(t, inserts, 2)
	})

	// (b) Merge-on-read end to end: an upsert keyed as []byte followed by a
	// delete keyed as the equal string. The delete is LAST, so after the
	// collapse the row must be GONE entirely. Without the canonical identity
	// both operations survive into one commit and the upserted row survives
	// its own delete (equality deletes never touch same-commit inserts).
	t.Run("merge-on-read delete wins over same-key upsert in another shape", func(t *testing.T) {
		ctx := t.Context()
		final := driveMORKeyed(t, ctx, kvSchema, []string{"k"},
			[]map[string]any{
				{"k": "k1", "payload": "old"},
				{"k": "other", "payload": "untouched"},
			},
			service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": []byte("k1"), "payload": "MID"}),
				cowMsg(t, "delete", map[string]any{"k": "k1"}),
			})

		got := scanKeyPayload(t, ctx, final)
		require.Len(t, got, 1, "the k1 row must be gone: the delete must win the collapse against the byte-shaped upsert")
		byPay := invertByPayload(t, got)
		assert.Contains(t, byPay, "untouched")
		assert.NotContains(t, byPay, "MID", "the same-batch upsert must not survive its own delete")
		assert.NotContains(t, byPay, "old")
	})

	// Temporal shape pair on the same merge-on-read path: an upsert keyed by a
	// nanosecond-precision non-UTC time.Time, then a delete keyed by the
	// canonically-equal microsecond UTC RFC 3339 STRING. Raw-shape identities
	// (json.Marshal's ns offset text vs the plain string) would never collapse.
	t.Run("merge-on-read temporal shapes collapse", func(t *testing.T) {
		ctx := t.Context()
		tsSchema := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.TimestampTz, Required: true},
			iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
		)
		est := time.FixedZone("EST", -5*3600)
		nsKey := time.Date(2026, 6, 15, 10, 20, 30, 123456789, est)
		// The stored (canonical) form: µs truncation, UTC, RFC 3339 — exactly
		// what jsonLeafValue encodes and the insert path stores.
		strKey := nsKey.Truncate(time.Microsecond).UTC().Format(time.RFC3339Nano)
		other := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

		final := driveMORKeyed(t, ctx, tsSchema, []string{"k"},
			[]map[string]any{
				{"k": nsKey, "payload": "old"},
				{"k": other, "payload": "untouched"},
			},
			service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": nsKey, "payload": "MID"}),
				cowMsg(t, "delete", map[string]any{"k": strKey}),
			})

		got := scanKeyPayload(t, ctx, final)
		require.Len(t, got, 1, "the string-keyed delete must collapse against the time.Time-keyed upsert and remove the row")
		byPay := invertByPayload(t, got)
		assert.Contains(t, byPay, "untouched")
		assert.NotContains(t, byPay, "MID", "the same-batch upsert must not survive its own delete")
		assert.NotContains(t, byPay, "old")
	})

	// (c) Copy-on-write end to end: two upserts of one key in two shapes must
	// leave exactly ONE row. Without the fix both survive the collapse and the
	// overwrite appends both new versions — scanKeyPayload hard-fails on the
	// duplicate key, which is the detection mechanism here.
	t.Run("copy-on-write two shapes leave one row", func(t *testing.T) {
		ctx := t.Context()
		final := driveCOWKeyed(t, ctx, kvSchema,
			[]map[string]any{
				{"k": "k1", "payload": "old"},
				{"k": "other", "payload": "untouched"},
			},
			service.MessageBatch{
				cowMsg(t, "upsert", map[string]any{"k": []byte("k1"), "payload": "NEW1"}),
				cowMsg(t, "upsert", map[string]any{"k": "k1", "payload": "NEW2"}),
			})

		require.Len(t, final, 2, "exactly one version of k1 plus the untouched row may remain")
		byPay := invertByPayload(t, final)
		assert.Equal(t, `"k1"`, byPay["NEW2"], "the later upsert must win the collapse")
		assert.NotContains(t, byPay, "NEW1", "the earlier same-key upsert must be collapsed away")
		assert.NotContains(t, byPay, "old")
		assert.Contains(t, byPay, "untouched")
	})
}

// --- explicit write.delete.mode conflict ------------------------------------

// TestCOWExplicitMergeOnReadDeleteModeConflicts pins commitOverwrite's
// handling of the write.delete.mode table property: an EXPLICIT conflicting
// value (merge-on-read) is a hard error, never a silent override — the
// property is table-level state other engines' delete jobs read, so flipping
// it behind the operator's back changes their materialisation. Only the
// property-absent case is stamped to copy-on-write, and an already-pinned
// copy-on-write value is left untouched.
func TestCOWExplicitMergeOnReadDeleteModeConflicts(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedRows := map[int64]string{1: "one", 2: "two"}

	setProp := func(t *testing.T, cat *memCatalog, value string) {
		t.Helper()
		tx := cat.snapshot().NewTransaction()
		require.NoError(t, tx.SetProperties(iceberg.Properties{table.WriteDeleteModeKey: value}))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
	}

	newWriterOn := func(t *testing.T, cat *memCatalog) *writer {
		t.Helper()
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 2}, reloadFn(cat), service.MockResources().Logger())
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		return w
	}

	upsert := func(t *testing.T) service.MessageBatch {
		t.Helper()
		return service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}
	}

	// (a) Explicit merge-on-read: the mutating write must ERROR, naming the
	// property and the conflicting config field, and commit nothing.
	t.Run("explicit merge-on-read errors and commits nothing", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, cat := newCOWTable(t, sc)
		_ = appendCOWRows(t, ctx, seedTbl, seedRows)
		setProp(t, cat, table.WriteModeMergeOnRead)
		w := newWriterOn(t, cat)

		err := w.Write(ctx, upsert(t))
		require.Error(t, err, "an explicitly conflicting write.delete.mode must fail the mutation, not be silently overridden")
		assert.ErrorContains(t, err, table.WriteDeleteModeKey, "the error must name the conflicting property")
		assert.ErrorContains(t, err, "merge_strategy", "the error must point at the connector config that conflicts")

		final := cat.snapshot()
		assert.Zero(t, countSnapshotsWithCommitID(final), "no mutation snapshot may land")
		assert.Equal(t, seedRows, scanRows(t, ctx, final), "the table data must be unchanged")
		assert.Equal(t, table.WriteModeMergeOnRead, final.Properties()[table.WriteDeleteModeKey],
			"the operator's explicit property must be left untouched")
	})

	// (b) Property absent: the mutation succeeds and stamps copy-on-write.
	t.Run("absent property is stamped to copy-on-write", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, cat := newCOWTable(t, sc)
		_ = appendCOWRows(t, ctx, seedTbl, seedRows)
		require.Empty(t, cat.snapshot().Properties()[table.WriteDeleteModeKey], "precondition: property absent")
		w := newWriterOn(t, cat)

		require.NoError(t, w.Write(ctx, upsert(t)))

		final := cat.snapshot()
		assert.Equal(t, table.WriteModeCopyOnWrite, final.Properties()[table.WriteDeleteModeKey],
			"a successful mutation must pin the absent property to copy-on-write")
		assert.Equal(t, map[int64]string{1: "one", 2: "TWO"}, scanRows(t, ctx, final))
	})

	// (c) Property already copy-on-write: the mutation succeeds unchanged.
	t.Run("pre-set copy-on-write succeeds", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, cat := newCOWTable(t, sc)
		_ = appendCOWRows(t, ctx, seedTbl, seedRows)
		setProp(t, cat, table.WriteModeCopyOnWrite)
		w := newWriterOn(t, cat)

		require.NoError(t, w.Write(ctx, upsert(t)))

		final := cat.snapshot()
		assert.Equal(t, table.WriteModeCopyOnWrite, final.Properties()[table.WriteDeleteModeKey])
		assert.Equal(t, map[int64]string{1: "one", 2: "TWO"}, scanRows(t, ctx, final))
	})
}

// --- numeric TIME range gate -------------------------------------------------

// TestNumericTimeOutOfRangeRejected pins shredder.NumericTemporalToTime's TIME
// branch (driven here through the shared jsonLeafValue canonicaliser): a
// post-scaling microsecond value outside [0, 24h) has no wall-clock
// representation, so it must be rejected loudly. Silently wrapping through the
// UnixMicro round trip would encode 24h as 00:00:00 — a value that then fails
// to match the raw (spec-invalid) microseconds the insert path stores, i.e. a
// silent merge-key no-match.
func TestNumericTimeOutOfRangeRejected(t *testing.T) {
	timeType := iceberg.PrimitiveTypes.Time

	t.Run("24h rejected", func(t *testing.T) {
		_, err := jsonLeafValue(timeType, int64(86_400_000_000), nil, false)
		require.Error(t, err, "86400000000 µs is exactly 24h and must not wrap to 00:00:00")
		assert.ErrorContains(t, err, "time-of-day")
	})

	t.Run("negative rejected", func(t *testing.T) {
		_, err := jsonLeafValue(timeType, int64(-1), nil, false)
		require.Error(t, err, "a negative time-of-day has no wall-clock representation")
	})

	t.Run("boundary 24h-1µs accepted", func(t *testing.T) {
		got, err := jsonLeafValue(timeType, int64(86_399_999_999), nil, false)
		require.NoError(t, err)
		assert.Equal(t, "23:59:59.999999", got)
	})

	t.Run("in-range control", func(t *testing.T) {
		got, err := jsonLeafValue(timeType, int64(54_000_000_000), nil, false)
		require.NoError(t, err)
		assert.Equal(t, "15:00:00", got)
	})

	// The range check must run AFTER unit scaling: 86,400,000 DECLARED millis
	// is exactly 24h in microseconds, so a millis-declared schema must reject
	// it even though the raw number is comfortably inside the µs range.
	t.Run("millis metadata judged after scaling", func(t *testing.T) {
		millisMeta := &benthosschema.Common{
			Type: benthosschema.TimeOfDay,
			Logical: &benthosschema.LogicalParams{
				TimeOfDay: &benthosschema.TimeOfDayParams{Unit: benthosschema.TimeUnitMillis},
			},
		}
		_, err := jsonLeafValue(timeType, int64(86_400_000), millisMeta, false)
		require.Error(t, err, "86400000 declared millis scales to exactly 24h and must be rejected post-scaling")
		assert.ErrorContains(t, err, "time-of-day")

		got, err := jsonLeafValue(timeType, int64(86_399_999), millisMeta, false)
		require.NoError(t, err, "24h-1ms is in range after scaling")
		assert.Equal(t, "23:59:59.999", got)
	})
}

// --- early merge-key type validation ------------------------------------------

// TestCOWKeyTypeValidatedEarly pins the copy-on-write merge-key TYPE gate:
// cowKeyFields validates every identifier field's type via cowKeyTypeSupported
// when the fields are resolved, so an unsupported key type (boolean, decimal —
// both upstream iceberg-go limitations) fails at writer creation with an
// actionable error rather than surfacing as a library panic or an
// "unsupported type BOOL" failure on the first mutating batch.
func TestCOWKeyTypeValidatedEarly(t *testing.T) {
	keyedWriter := func(field string) *writer {
		return &writer{
			caseSensitive: true,
			rowOpCfg:      RowOpConfig{IdentifierFields: []string{field}, MergeStrategy: mergeStrategyCOW},
		}
	}
	schemaWithKey := func(typ iceberg.Type) *iceberg.Schema {
		return iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "k", Type: typ, Required: true},
			iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
		)
	}

	t.Run("cowKeyFields rejects decimal key", func(t *testing.T) {
		_, err := keyedWriter("k").cowKeyFields(schemaWithKey(iceberg.DecimalTypeOf(10, 2)))
		require.Error(t, err)
		assert.ErrorContains(t, err, "decimal is not a supported copy-on-write merge key")
	})

	t.Run("cowKeyFields rejects boolean key", func(t *testing.T) {
		_, err := keyedWriter("k").cowKeyFields(schemaWithKey(iceberg.PrimitiveTypes.Bool))
		require.Error(t, err)
		assert.ErrorContains(t, err, "boolean is not a supported copy-on-write merge key")
	})

	t.Run("cowKeyFields accepts string key", func(t *testing.T) {
		fields, err := keyedWriter("k").cowKeyFields(schemaWithKey(iceberg.PrimitiveTypes.String))
		require.NoError(t, err)
		require.Len(t, fields, 1)
		assert.Equal(t, "k", fields[0].Name)
	})

	t.Run("cowKeyTypeSupported table", func(t *testing.T) {
		supported := []iceberg.Type{
			iceberg.PrimitiveTypes.Int32,
			iceberg.PrimitiveTypes.Int64,
			iceberg.PrimitiveTypes.String,
			iceberg.PrimitiveTypes.Date,
			iceberg.PrimitiveTypes.Time,
			iceberg.PrimitiveTypes.Timestamp,
			iceberg.PrimitiveTypes.TimestampTz,
			iceberg.PrimitiveTypes.UUID,
		}
		for _, typ := range supported {
			assert.NoError(t, cowKeyTypeSupported("k", typ), "%s must be a supported copy-on-write merge key", typ)
		}
		rejected := []iceberg.Type{
			iceberg.PrimitiveTypes.Bool,
			iceberg.DecimalTypeOf(10, 2),
			iceberg.PrimitiveTypes.Float64,
		}
		for _, typ := range rejected {
			err := cowKeyTypeSupported("k", typ)
			require.Error(t, err, "%s must be rejected as a copy-on-write merge key", typ)
			assert.ErrorContains(t, err, "merge key column")
		}
	})
}

// --- timestamp-encoding probe negative cache -----------------------------------

// TestProbeErrCache pins tableEntry's probe-error negative cache: ONLY the
// deterministic errTimestampProbeBoundExceeded is ever cached (a transient
// probe failure must retry), the cache is keyed to the snapshot it was
// observed on, and a new snapshot both bypasses AND clears it — files landing
// makes the probe's answer potentially different, so the stale entry must not
// survive to answer for the old snapshot either.
func TestProbeErrCache(t *testing.T) {
	ctx := t.Context()

	t.Run("bound error cached against current snapshot", func(t *testing.T) {
		tbl, _ := newTestTable(t)
		tbl = seedTable(t, ctx, tbl, 1)
		e := &tableEntry{}
		boundErr := fmt.Errorf("%w: boom", errTimestampProbeBoundExceeded)

		e.mu.Lock()
		defer e.mu.Unlock()
		require.True(t, e.noteProbeErr(tbl, boundErr), "the deterministic bound error must be cached")
		cached := e.cachedProbeErr(tbl)
		require.Error(t, cached)
		assert.Equal(t, boundErr, cached, "the same snapshot must be served the cached error verbatim")
	})

	t.Run("plain error never cached", func(t *testing.T) {
		tbl, _ := newTestTable(t)
		tbl = seedTable(t, ctx, tbl, 1)
		e := &tableEntry{}

		e.mu.Lock()
		defer e.mu.Unlock()
		require.False(t, e.noteProbeErr(tbl, fmt.Errorf("transient object-store hiccup")),
			"a transient failure must not be negative-cached — it must retry")
		assert.NoError(t, e.cachedProbeErr(tbl))
	})

	t.Run("new snapshot bypasses and clears the cache", func(t *testing.T) {
		tbl, _ := newTestTable(t)
		oldTbl := seedTable(t, ctx, tbl, 1)
		e := &tableEntry{}
		boundErr := fmt.Errorf("%w: boom", errTimestampProbeBoundExceeded)

		e.mu.Lock()
		defer e.mu.Unlock()
		require.True(t, e.noteProbeErr(oldTbl, boundErr))

		// A second commit moves the current snapshot: the probe must re-run.
		newTbl := seedTable(t, ctx, oldTbl, 1)
		require.NotEqual(t, oldTbl.CurrentSnapshot().SnapshotID, newTbl.CurrentSnapshot().SnapshotID,
			"precondition: seeding again must land a new snapshot")
		assert.NoError(t, e.cachedProbeErr(newTbl), "a new snapshot means files landed; the probe must re-run")

		// And the miss must CLEAR the stale entry: the original snapshot may
		// no longer be answered from the cache either.
		assert.NoError(t, e.cachedProbeErr(oldTbl),
			"the stale entry must be cleared on a snapshot miss, not resurrected for the old snapshot")
	})

	t.Run("no snapshot means nothing to key the cache on", func(t *testing.T) {
		tbl, _ := newTestTable(t) // never seeded: no current snapshot
		require.Nil(t, tbl.CurrentSnapshot(), "precondition: fresh table has no snapshot")
		e := &tableEntry{}

		e.mu.Lock()
		defer e.mu.Unlock()
		assert.False(t, e.noteProbeErr(tbl, fmt.Errorf("%w: boom", errTimestampProbeBoundExceeded)),
			"without a snapshot there is nothing to key the cache on")
		assert.NoError(t, e.cachedProbeErr(tbl))
	})
}

// --- actionable mutation rejections --------------------------------------------

// TestMutationRejectionNamesValue pins the diagnostic content of
// jsonLeafValue's integer and boolean rejections: the error must include the
// offending VALUE (not just its Go type — "42" and "forty-two" need different
// upstream fixes) and the concrete bloblang conversion that repairs the
// pipeline.
func TestMutationRejectionNamesValue(t *testing.T) {
	t.Run("integer column names value and .number()", func(t *testing.T) {
		_, err := jsonLeafValue(iceberg.PrimitiveTypes.Int64, "42", nil, false)
		require.Error(t, err, "the insert path rejects numeric strings, so the mutation paths must too")
		assert.ErrorContains(t, err, "42", "the error must show WHAT arrived, not just its type")
		assert.ErrorContains(t, err, ".number()", "the error must name the bloblang conversion that fixes the upstream")
	})

	t.Run("boolean column names value and .bool()", func(t *testing.T) {
		_, err := jsonLeafValue(iceberg.PrimitiveTypes.Bool, "true", nil, false)
		require.Error(t, err, "the insert path rejects stringified booleans, so the mutation paths must too")
		assert.ErrorContains(t, err, "true", "the error must show WHAT arrived, not just its type")
		assert.ErrorContains(t, err, ".bool()", "the error must name the bloblang conversion that fixes the upstream")
	})
}

// TestDecimalSpellingsShareCanonicalForm pins the decimal arm's rescaling:
// every spelling of one decimal — float 1.5, string "1.5", json.Number "1.5",
// and the already-scaled "1.50" — must canonicalise to the identical string,
// or the in-batch key collapse treats equal merge keys as distinct and a row
// survives its own same-commit delete.
func TestDecimalSpellingsShareCanonicalForm(t *testing.T) {
	dt := iceberg.DecimalTypeOf(10, 2)
	want := "1.50"
	for _, v := range []any{1.5, "1.5", json.Number("1.5"), "1.50"} {
		got, err := jsonLeafValue(dt, v, nil, false)
		require.NoError(t, err, "%T %v", v, v)
		assert.Equal(t, want, got, "spelling %T %v must share the canonical form", v, v)
	}
	// Excess precision follows the insert path's rounding.
	got, err := jsonLeafValue(dt, "1.505", nil, false)
	require.NoError(t, err)
	assert.Equal(t, "1.51", got, "excess digits round exactly as the insert path rounds")
}
