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
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/rest"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// --- merge_strategy config parsing --------------------------------------------

func TestParseMergeStrategyConfig(t *testing.T) {
	t.Run("defaults to merge-on-read", func(t *testing.T) {
		cfg, err := parseTestRowOpConfig(t, "")
		require.NoError(t, err)
		assert.Equal(t, mergeStrategyMOR, cfg.MergeStrategy)
	})

	t.Run("explicit copy-on-write", func(t *testing.T) {
		cfg, err := parseTestRowOpConfig(t, "merge_strategy: copy-on-write\nrow_operation: upsert\nidentifier_fields: [id]\n")
		require.NoError(t, err)
		assert.Equal(t, mergeStrategyCOW, cfg.MergeStrategy)
	})

	t.Run("explicit merge-on-read", func(t *testing.T) {
		cfg, err := parseTestRowOpConfig(t, "merge_strategy: merge-on-read\n")
		require.NoError(t, err)
		assert.Equal(t, mergeStrategyMOR, cfg.MergeStrategy)
	})

	t.Run("invalid value rejected", func(t *testing.T) {
		// FINDING (T-20/G4): the merge_strategy StringEnumField enum is NOT
		// enforced by ParseYAML — parsing an unknown value succeeds without error,
		// so the enum constraint is advisory (surfaced only by a separate config
		// lint pass, which ParseYAML does not run). The only hard rejection at
		// runtime is the defensive switch in parseRowOpConfig. This subtest pins
		// exactly that: ParseYAML admits the bogus value, and parseRowOpConfig is
		// what rejects it. If the StringEnumField ever starts rejecting at
		// ParseYAML time, the first assertion flips and this test should be
		// tightened to require the earlier rejection.
		conf, yamlErr := icebergOutputConfig().ParseYAML(`
catalog:
  url: http://localhost:8181/api/catalog
namespace: ns
table: t
storage:
  aws_s3:
    bucket: bucket
merge_strategy: sideways
`, nil)
		require.NoError(t, yamlErr,
			"the merge_strategy enum is not enforced at ParseYAML time; if this ever changes, tighten this test to require the earlier rejection")

		_, err := parseRowOpConfig(conf)
		require.Error(t, err, "parseRowOpConfig's defensive switch must reject the unknown merge_strategy")
		assert.Contains(t, err.Error(), ioFieldMergeStrategy)
	})
}

// --- conditional identifier-field registration --------------------------------

func cowRouter(strategy mergeStrategy) *Router {
	return &Router{
		caseSensitive: true,
		resolver:      newTypeResolver("", nil, true, nil),
		rowOpCfg:      RowOpConfig{IdentifierFields: []string{"id"}, MergeStrategy: strategy},
	}
}

func TestSchemaWithIdentifierFieldsMORRegisters(t *testing.T) {
	r := cowRouter(mergeStrategyMOR)
	record := map[string]any{"id": int64(1), "name": "a"}
	sc, err := r.buildSchemaWithResolver(record, structuredMsg(t, record), tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	idField, ok := sc.FindFieldByName("id")
	require.True(t, ok)
	assert.Equal(t, []int{idField.ID}, sc.IdentifierFieldIDs, "merge-on-read must register identifier-field-ids")
	assert.True(t, idField.Required, "identifier columns must be marked required under merge-on-read")
}

func TestSchemaWithIdentifierFieldsCOWDoesNotRegister(t *testing.T) {
	r := cowRouter(mergeStrategyCOW)
	record := map[string]any{"id": int64(1), "name": "a"}
	sc, err := r.buildSchemaWithResolver(record, structuredMsg(t, record), tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	assert.Empty(t, sc.IdentifierFieldIDs, "copy-on-write must not register identifier-field-ids")
	idField, ok := sc.FindFieldByName("id")
	require.True(t, ok)
	assert.False(t, idField.Required, "copy-on-write must not force identifier columns required")
}

// --- schema-support gate -------------------------------------------------------

func TestCheckCOWSchemaSupported(t *testing.T) {
	t.Run("flat primitives ok", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.PrimitiveTypes.Timestamp},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("nested struct accepted", func(t *testing.T) {
		// Nested struct/list/map are now supported: the gate recurses the type
		// tree and cowMassage produces the correct JSON shape at every depth. The
		// faithful round-trips live in cow_type_roundtrip_test.go.
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "nested", Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{{ID: 3, Name: "inner", Type: iceberg.PrimitiveTypes.String}},
			}},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("binary and fixed accepted", func(t *testing.T) {
		// binary and fixed are flat primitives that round-trip faithfully through
		// the Arrow JSON base64 encoding (see TestCOWColumnTypeRoundTrip), so the
		// gate accepts them.
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "b", Type: iceberg.PrimitiveTypes.Binary},
			iceberg.NestedField{ID: 2, Name: "f", Type: iceberg.FixedTypeOf(16)},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("nested list accepted", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "l", Type: &iceberg.ListType{
				ElementID: 3, Element: iceberg.PrimitiveTypes.String, ElementRequired: false,
			}},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("map accepted", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "m", Type: &iceberg.MapType{
				KeyID: 3, KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 4, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: false,
			}},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("deeply nested primitives accepted", func(t *testing.T) {
		// struct<list<map<string, struct<...>>>> — every leaf is a supported
		// primitive, so the recursive gate accepts the whole tree.
		sc := iceberg.NewSchema(0,
			cowIDField(),
			iceberg.NestedField{ID: 2, Name: "deep", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 3, Name: "items", Type: &iceberg.ListType{
					ElementID: 4, ElementRequired: false, Element: &iceberg.MapType{
						KeyID: 5, KeyType: iceberg.PrimitiveTypes.String,
						ValueID: 6, ValueRequired: false, ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
							{ID: 7, Name: "n", Type: iceberg.PrimitiveTypes.Int64},
						}},
					},
				}},
			}}},
		)
		require.NoError(t, checkCOWSchemaSupported(sc))
	})

	t.Run("unsupported leaf inside nested type rejected", func(t *testing.T) {
		// A genuinely unsupported leaf (timestamp_ns is not in the supported set)
		// nested inside a list-of-struct still fails loudly, and the error names
		// the dotted path to the offending leaf.
		sc := iceberg.NewSchema(0,
			cowIDField(),
			iceberg.NestedField{ID: 2, Name: "events", Type: &iceberg.ListType{
				ElementID: 3, ElementRequired: false, Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
					{ID: 4, Name: "at", Type: iceberg.PrimitiveTypes.TimestampNs},
				}},
			}},
		)
		err := checkCOWSchemaSupported(sc)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "events.element.at")
		assert.Contains(t, err.Error(), "timestamp_ns")
	})
}

// --- filter construction -------------------------------------------------------

func cowWriter(t testing.TB, tbl *table.Table, idFields ...string) *writer {
	t.Helper()
	return &writer{
		table:         tbl,
		caseSensitive: true,
		rowOpCfg: RowOpConfig{
			// Drive the operation from metadata, as a real change-data-capture
			// mapping does, so the routing field never leaks into the row body.
			Operation:        mustInterp(t, `${! metadata("op") }`),
			IdentifierFields: idFields,
			MergeStrategy:    mergeStrategyCOW,
		},
		logger: service.MockResources().Logger(),
	}
}

// cowWriterCI is cowWriter with case-insensitive matching, for the case-fold
// tests.
func cowWriterCI(t testing.TB, tbl *table.Table, idFields ...string) *writer {
	t.Helper()
	w := cowWriter(t, tbl, idFields...)
	w.caseSensitive = false
	return w
}

// countRowsWithID counts how many rows carry the given int64 key in column
// idCol — used to detect duplicates that an id->value map would silently
// collapse.
func countRowsWithID(t testing.TB, ctx context.Context, tbl *table.Table, idCol string, id int64) int {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	n := 0
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices(idCol)[0]).(*array.Int64)
		for r := 0; r < int(rec.NumRows()); r++ {
			if idArr.Value(r) == id {
				n++
			}
		}
	}
	return n
}

// cowMsg builds a message whose body is the row image and whose "op" metadata
// drives the row_operation.
func cowMsg(t testing.TB, op string, row map[string]any) *service.Message {
	t.Helper()
	msg := structuredMsg(t, row)
	msg.MetaSetMut("op", op)
	return msg
}

func TestBuildCOWFilterSingleKey(t *testing.T) {
	tbl, _ := newTestTable(t) // schema: id int64
	w := cowWriter(t, tbl, "id")

	keyed := service.MessageBatch{
		structuredMsg(t, map[string]any{"id": 2}),
		structuredMsg(t, map[string]any{"id": 4}),
	}
	filter, err := w.buildCOWFilter(tbl.Schema(), keyed)
	require.NoError(t, err)
	// The IN predicate is conjoined with a NotNull guard on the key column,
	// which keeps the negated survivor scan null-safe (see buildCOWFilter's doc
	// comment; pinned end-to-end by cow_null_key_test.go).
	require.Equal(t, iceberg.OpAnd, filter.Op(), "a single-column key builds a null-safe AND(NotNull, IN)")
	assert.Contains(t, filter.String(), "NotNull", "the key column must carry the NotNull guard")
	assert.Contains(t, filter.String(), "In(", "two distinct keys on one column collapse to an IN predicate")
}

func TestBuildCOWFilterCompositeKey(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "tenant", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 2, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
	)
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriter(t, tbl, "tenant", "id")

	keyed := service.MessageBatch{
		structuredMsg(t, map[string]any{"tenant": "a", "id": 1}),
		structuredMsg(t, map[string]any{"tenant": "b", "id": 2}),
	}
	filter, err := w.buildCOWFilter(sc, keyed)
	require.NoError(t, err)
	// Two tuples => OR-of-ANDs; the top-level operator is OR.
	assert.Equal(t, iceberg.OpOr, filter.Op(), "composite key over two tuples must be an OR of ANDs")
}

func TestBuildCOWFilterUnsupportedKeyType(t *testing.T) {
	// binary is a supported COW *column* type but not a sensible merge key, so
	// it must be rejected by the filter path with an actionable error.
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "k", Type: iceberg.PrimitiveTypes.Binary},
	)
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriter(t, tbl, "k")
	_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": []byte{0x01}})})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support merge key column")
}

// TestBuildCOWFilterBareNumberTemporalKeyRejected pins the silent-no-match guard on the
// merge-key path: a temporal key given as a bare number is ambiguous (the data
// path cannot reproduce how a number would be interpreted), so it must be
// rejected loudly rather than silently building a literal that matches nothing.
func TestBuildCOWFilterBareNumberTemporalKeyRejected(t *testing.T) {
	for _, typ := range []iceberg.Type{
		iceberg.PrimitiveTypes.Timestamp,
		iceberg.PrimitiveTypes.TimestampTz,
		iceberg.PrimitiveTypes.Date,
		iceberg.PrimitiveTypes.Time,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "k", Type: typ})
			tbl := newTypedKeyTableFromSchema(t, sc)
			w := cowWriter(t, tbl, "k")
			_, err := w.buildCOWFilter(sc, service.MessageBatch{structuredMsg(t, map[string]any{"k": 1})})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "requires a time value")
		})
	}
}

// --- record factory ------------------------------------------------------------

func TestBuildCOWRecordFactoryRebuildsReader(t *testing.T) {
	tbl, _ := newTestTable(t) // id int64
	w := cowWriter(t, tbl, "id")
	factory, err := w.buildCOWRecordFactory(tbl.Schema(), service.MessageBatch{
		structuredMsg(t, map[string]any{"id": 1}),
		structuredMsg(t, map[string]any{"id": 2}),
	})
	require.NoError(t, err)

	// The factory must return an independent, fully-consumable reader each time
	// (the commit stage can run more than once on retry).
	for attempt := range 2 {
		rdr, err := factory()
		require.NoError(t, err)
		rows := int64(0)
		for rdr.Next() {
			rows += rdr.RecordBatch().NumRows()
		}
		rdr.Release()
		assert.EqualValues(t, 2, rows, "attempt %d must see all rows", attempt)
	}
}

// TestCOWMutationDetectsNewColumn pins the fix for silent schema-evolution data
// loss: a copy-on-write upsert carrying a column absent from the table schema
// must surface a BatchSchemaEvolutionError (so the router evolves the table and
// retries), not silently drop the column via the Arrow projection. No committer
// is wired — detection must fire before any commit.
func TestCOWMutationDetectsNewColumn(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	tbl, _ := newCOWTable(t, sc)
	w := cowWriter(t, tbl, "id")

	err := w.Write(t.Context(), service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "x", "extra": "new-column"}),
	})
	require.Error(t, err)
	var evo *BatchSchemaEvolutionError
	require.ErrorAs(t, err, &evo, "an unknown column must trigger schema evolution, not a silent drop")
	assert.Contains(t, err.Error(), "extra")
}

// TestCommitOverwriteCleansUpOrphansOnFailure pins the fix for orphaned
// copy-on-write files: iceberg-go's Overwrite writes rewritten/new parquet files
// before the catalog commit, so a definitively-failed commit must leave none
// behind. TestCOWUpsertDeleteRoundTrip is the control proving the overwrite path
// does write files, so a return to the seed count here is genuine cleanup rather
// than a vacuous pass.
func TestCommitOverwriteCleansUpOrphansOnFailure(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())
	require.Positive(t, seedCount, "seeding must have written data files")

	// A definitive, non-retryable rejection (400-class) proves the mutation's
	// commit did not land, so the files the overwrite wrote are genuine orphans.
	// (A bare transport-style error would instead be ambiguous — the commit may
	// still land server-side — and cleanup would rightly be skipped; see
	// TestCommitOverwriteTransportErrorIsAmbiguous.)
	fc := &flakyCatalog{memCatalog: cat, failuresLeft: 1 << 30, failErr: fmt.Errorf("commit rejected: %w", rest.ErrBadRequest)}
	comm, err := NewCommitter(fc.snapshot(), fc, CommitConfig{MaxRetries: 2}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, fc.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)

	assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
		"the failed copy-on-write commit's parquet files must be cleaned up, leaving only the seed files")
}

// TestCommitOverwriteSkipsCleanupWhenDisabled is the counterpart to
// TestCommitOverwriteCleansUpOrphansOnFailure: with
// `commit.cleanup_on_failure: false` the same definitively-failed copy-on-write
// commit must leave every file it recorded writing in place. Disabling cleanup
// can only ever leak storage (reclaimed by Iceberg orphan-file maintenance),
// which is what makes the escape hatch safe to expose.
func TestCommitOverwriteSkipsCleanupWhenDisabled(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())
	require.Positive(t, seedCount, "seeding must have written data files")

	// Same definitive (400-class) rejection as the cleanup test, so the files
	// the overwrite wrote are provable orphans and cleanup would otherwise run.
	fc := &flakyCatalog{memCatalog: cat, failuresLeft: 1 << 30, failErr: fmt.Errorf("commit rejected: %w", rest.ErrBadRequest)}
	comm, err := NewCommitter(fc.snapshot(), fc, CommitConfig{MaxRetries: 2, DisableCleanupOnFailure: true}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, fc.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)

	assert.Greater(t, countParquetFiles(t, seedTbl.Location()), seedCount,
		"cleanup_on_failure: false must leave the failed copy-on-write commit's files for Iceberg orphan-file maintenance")
}

// TestCommitOverwriteIdempotentOnUnknownState pins the copy-on-write half of the
// commit-id idempotency guarantee. A copy-on-write overwrite is safe to retry
// after an ambiguous (ErrCommitStateUnknown) catalog response because the
// commit-id stamped into the snapshot summary lets the retry tell a landed
// overwrite from a lost one. Every path must leave the mutation applied exactly
// once — no duplicate snapshot, correct final rows.
func TestCommitOverwriteIdempotentOnUnknownState(t *testing.T) {
	logger := service.MockResources().Logger()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// setup seeds id=1,2,3 through a plain catalog, then wraps it in a
	// scriptedCatalog so only the mutation under test is subject to the scripted
	// outcome. The writer and committer share that scripted catalog.
	setup := func(t *testing.T, outcome commitOutcome) (*scriptedCatalog, *writer) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{outcome}}
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
		require.NoError(t, err)
		t.Cleanup(comm.Close)
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm
		return cat, w
	}

	want := map[int64]string{1: "one", 2: "TWO", 3: "three"}
	upsert := func() service.MessageBatch {
		return service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}
	}

	// (A) landed-but-reported-unknown: the first CommitTable applies the overwrite
	// server-side, then reports ErrCommitStateUnknown. The retry must find the
	// commit-id in the reloaded snapshot and short-circuit to success without
	// re-committing (CommitTable called exactly once).
	t.Run("landed then unknown applies once", func(t *testing.T) {
		ctx := t.Context()
		cat, w := setup(t, commitLandThenUnknown)
		require.NoError(t, w.Write(ctx, upsert()))
		assert.Equal(t, 1, cat.calls, "a landed overwrite must not be re-committed after an unknown-state response")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "overwrite applied exactly once")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))
	})

	// (B) not-landed-unknown: the first CommitTable returns ErrCommitStateUnknown
	// WITHOUT applying, so the commit-id is absent on reload and the retry must
	// re-apply and succeed — still exactly once.
	t.Run("unknown without landing re-applies once", func(t *testing.T) {
		ctx := t.Context()
		cat, w := setup(t, commitUnknownNoLand)
		require.NoError(t, w.Write(ctx, upsert()))
		assert.Equal(t, 2, cat.calls, "an overwrite that did not land must be retried")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "overwrite committed exactly once on the retry")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))
	})

	// Clean conflict (ErrCommitFailed, nothing landed): the commit-id is absent on
	// reload, so the genuine-conflict retry still re-applies exactly once — the
	// idempotency check must not over-filter a legitimate retry.
	t.Run("clean conflict re-applies once", func(t *testing.T) {
		ctx := t.Context()
		cat, w := setup(t, commitConflict)
		require.NoError(t, w.Write(ctx, upsert()))
		assert.Equal(t, 2, cat.calls, "a genuine conflict must be retried")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "overwrite committed exactly once after the conflict")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))
	})

	// (T-13) landed-but-reported-failed (a lost ack on a 409): the first CommitTable
	// applies the overwrite server-side, then reports ErrCommitFailed as if it had
	// been a clean conflict. The retry must find the commit-id in the reloaded
	// snapshot and short-circuit to success WITHOUT re-committing — exactly one
	// CommitTable call, one snapshot with the token, correct rows.
	t.Run("landed then failed applies once", func(t *testing.T) {
		ctx := t.Context()
		cat, w := setup(t, commitLandThenFail)
		require.NoError(t, w.Write(ctx, upsert()))
		assert.Equal(t, 1, cat.calls, "a landed overwrite must not be re-committed after a lost-ack conflict")
		assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "overwrite applied exactly once")
		assert.Equal(t, want, scanRows(t, ctx, cat.snapshot()))
	})
}

// --- committer-level round trip ------------------------------------------------

// newTypedKeyTableFromSchema builds an unpartitioned v2 table for the given
// schema, backed by an in-memory catalog and the local filesystem.
func newTypedKeyTableFromSchema(t testing.TB, sc *iceberg.Schema) *table.Table {
	t.Helper()
	tbl, _ := newCOWTable(t, sc)
	return tbl
}

func newCOWTable(t testing.TB, sc *iceberg.Schema) (*table.Table, *memCatalog) {
	t.Helper()
	return newAmpTableWithSchema(t, sc)
}

// TestCOWv1TableStaysV1 pins Tier 3.1: copy-on-write writes only plain data
// files, so it works on a v1 table and must NOT trigger the irreversible v1->v2
// upgrade the merge-on-read path forces. The mutation must still round-trip.
func TestCOWv1TableStaysV1(t *testing.T) {
	ctx := t.Context()
	location := filepath.ToSlash(t.TempDir())
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder,
		location, iceberg.Properties{table.PropertyFormatVersion: "1"})
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "t"},
		location:         location,
	}
	tbl := cat.snapshot()
	require.EqualValues(t, 1, tbl.Metadata().Version(), "precondition: table starts at v1")

	tbl = appendCOWRows(t, ctx, tbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	require.EqualValues(t, 1, cat.snapshot().Metadata().Version(), "seeding must not upgrade the table")

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3, SkipFormatUpgrade: true}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, tbl, "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"}),
		cowMsg(t, "delete", map[string]any{"id": 3}),
	}))

	final := cat.snapshot()
	assert.EqualValues(t, 1, final.Metadata().Version(), "copy-on-write must not upgrade a v1 table to v2")
	assertAllManifestsData(t, ctx, final)
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO"}, scanRows(t, ctx, final))
}

// TestCOWUpsertDeleteRoundTrip drives a full copy-on-write upsert+delete batch
// through the writer and committer against an in-memory catalog, then asserts
// (a) the resulting table has the correct final rows and (b) the table contains
// ONLY plain data files — zero delete files — which is what makes it readable by
// engine-backed catalogs.
func TestCOWUpsertDeleteRoundTrip(t *testing.T) {
	ctx := t.Context()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)

	// Seed rows id=1,2,3.
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})

	// Build a writer whose committer shares the catalog.
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	// upsert id=2 (payload->TWO), delete id=3, upsert id=4 (new row).
	batch := service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"}),
		cowMsg(t, "delete", map[string]any{"id": 3}),
		cowMsg(t, "upsert", map[string]any{"id": 4, "payload": "FOUR"}),
	}
	require.NoError(t, w.Write(ctx, batch))

	final := cat.snapshot()

	// (a) zero delete files: every manifest must be data content.
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assertAllManifestsData(t, ctx, final)

	// (b) correct final state.
	got := scanRows(t, ctx, final)
	want := map[int64]string{1: "one", 2: "TWO", 4: "FOUR"}
	assert.Equal(t, want, got)
}

func TestCOWOnlyDeletesFastPath(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two"})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "delete", map[string]any{"id": 1}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "delete-only copy-on-write must leave no delete files")
	assert.Equal(t, table.OpDelete, final.CurrentSnapshot().Summary.Operation)
	assert.Equal(t, map[int64]string{2: "two"}, scanRows(t, ctx, final))
}

func TestCOWOnlyInsertsUsesAppend(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	// The shredder append path writes into a data/ subdir that LocalFS will not
	// create implicitly.
	require.NoError(t, os.MkdirAll(filepath.Join(seedTbl.Location(), "data"), 0o755))

	// A batch of only inserts (unkeyed) must take the plain append path.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "insert", map[string]any{"id": 10, "payload": "ten"}),
		cowMsg(t, "insert", map[string]any{"id": 11, "payload": "eleven"}),
	}))

	final := cat.snapshot()
	require.NotNil(t, final.CurrentSnapshot())
	assert.Equal(t, table.OpAppend, final.CurrentSnapshot().Summary.Operation, "insert-only batch must append, not overwrite")
	assert.Equal(t, map[int64]string{10: "ten", 11: "eleven"}, scanRows(t, ctx, final))
}

// --- partitioned copy-on-write --------------------------------------------------

// newPartitionedCOWTable builds a partitioned v2 table for the given schema and
// spec, backed by an in-memory catalog and the local filesystem. It mirrors
// newAmpTableWithSchema but installs a real partition spec so
// tbl.Spec().NumFields() > 0.
func newPartitionedCOWTable(t testing.TB, sc *iceberg.Schema, spec iceberg.PartitionSpec) (*table.Table, *memCatalog) {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(sc, &spec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "2"})
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "cow_partitioned"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// appendPartitionedCOWRows appends (id, region, payload) rows as one plain-data-
// file snapshot, routed to partitions by iceberg-go's partitioned fanout writer,
// and returns the updated table handle.
func appendPartitionedCOWRows(t testing.TB, ctx context.Context, tbl *table.Table, rows []map[string]any) *table.Table {
	t.Helper()
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	b, err := json.Marshal(rows)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, bytes.NewReader(b))
	require.NoError(t, err)
	rdr, err := array.NewRecordReader(arrowSc, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	rec.Release()
	defer rdr.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.Append(ctx, rdr, nil))
	next, err := tx.Commit(ctx)
	require.NoError(t, err)
	return next
}

// scanPartitionedRows scans the table into id -> {region, payload}, honouring any
// deletes.
func scanPartitionedRows(t testing.TB, ctx context.Context, tbl *table.Table) map[int64][2]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	out := map[int64][2]string{}
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		regArr := rec.Column(rec.Schema().FieldIndices("region")[0]).(*array.String)
		payArr := rec.Column(rec.Schema().FieldIndices("payload")[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			pay := ""
			if payArr.IsValid(r) {
				pay = payArr.Value(r)
			}
			out[idArr.Value(r)] = [2]string{regArr.Value(r), pay}
		}
	}
	return out
}

// TestCOWPartitionedUpsertDeleteRoundTrip proves copy-on-write works end-to-end
// on a partitioned table (partition by region, merge key id — a NON-partition
// column). A single mutating batch touches multiple partitions: it updates a row
// in eu, deletes a row in eu, and inserts a new row in apac. The result must have
// the correct per-partition state AND zero delete files (the copy-on-write
// invariant). The merge key is not the partition column, which merge-on-read
// could not support (equality deletes are partition-scoped) — copy-on-write can,
// because it rewrites whole files by filter and re-routes appended rows by value.
func TestCOWPartitionedUpsertDeleteRoundTrip(t *testing.T) {
	ctx := t.Context()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	// Partition by region (identity transform) so spec.NumFields() > 0.
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	seedTbl, cat := newPartitionedCOWTable(t, sc, spec)
	seedSpec := seedTbl.Spec()
	require.Positive(t, seedSpec.NumFields(), "table must be partitioned for this test to be meaningful")

	// Seed rows across three partitions.
	seedTbl = appendPartitionedCOWRows(t, ctx, seedTbl, []map[string]any{
		{"id": 1, "region": "us", "payload": "one"},
		{"id": 2, "region": "eu", "payload": "two"},
		{"id": 3, "region": "eu", "payload": "three"},
		{"id": 4, "region": "apac", "payload": "four"},
	})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	// One batch spanning multiple partitions: upsert id=2 (eu), delete id=3 (eu),
	// upsert id=5 (new row in apac).
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "region": "eu", "payload": "TWO"}),
		cowMsg(t, "delete", map[string]any{"id": 3, "region": "eu"}),
		cowMsg(t, "upsert", map[string]any{"id": 5, "region": "apac", "payload": "FIVE"}),
	}))

	final := cat.snapshot()

	// (a) zero delete files: the copy-on-write invariant.
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assertAllManifestsData(t, ctx, final)

	// (b) correct final state, per partition.
	got := scanPartitionedRows(t, ctx, final)
	want := map[int64][2]string{
		1: {"us", "one"},    // untouched
		2: {"eu", "TWO"},    // upserted in place (no duplicate)
		4: {"apac", "four"}, // untouched
		5: {"apac", "FIVE"}, // inserted into a different partition
	}
	assert.Equal(t, want, got, "id=3 must be deleted; id=2 updated once; id=5 landed in apac")
}

// TestCOWPartitionKeyChangeRoundTrip covers the case merge-on-read cannot: an
// upsert that moves a keyed row to a DIFFERENT partition. Copy-on-write deletes
// the old row wherever it lives (the filter matches across all partitions) and
// appends the new row into its new partition, leaving exactly one row.
func TestCOWPartitionKeyChangeRoundTrip(t *testing.T) {
	ctx := t.Context()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region", Transform: iceberg.IdentityTransform{},
	})
	seedTbl, cat := newPartitionedCOWTable(t, sc, spec)

	seedTbl = appendPartitionedCOWRows(t, ctx, seedTbl, []map[string]any{
		{"id": 1, "region": "us", "payload": "one"},
	})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	// Move id=1 from us to eu via upsert.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 1, "region": "eu", "payload": "ONE"}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	got := scanPartitionedRows(t, ctx, final)
	assert.Equal(t, map[int64][2]string{1: {"eu", "ONE"}}, got,
		"the row must move to eu with no stale copy left in us")
}

// TestCOWBucketPartitionRoundTrip exercises a NON-order-preserving transform
// (bucket) on the copy-on-write write path. The partitioned fanout writer derives
// each row's partition from Transform.Apply on the actual value, so bucket works
// exactly like identity — this is distinct from the stats-inference path
// (fileToDataFile), which panics on non-order-preserving transforms but is only
// used by AddFiles, never by the record-writing path copy-on-write uses.
func TestCOWBucketPartitionRoundTrip(t *testing.T) {
	ctx := t.Context()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "region", Type: iceberg.PrimitiveTypes.String, Required: true},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	// Partition by bucket(4, region) — a non-order-preserving transform.
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{2}, FieldID: 1000, Name: "region_bucket", Transform: iceberg.BucketTransform{NumBuckets: 4},
	})
	seedTbl, cat := newPartitionedCOWTable(t, sc, spec)

	seedTbl = appendPartitionedCOWRows(t, ctx, seedTbl, []map[string]any{
		{"id": 1, "region": "us", "payload": "one"},
		{"id": 2, "region": "eu", "payload": "two"},
		{"id": 3, "region": "apac", "payload": "three"},
	})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "region": "eu", "payload": "TWO"}),
		cowMsg(t, "delete", map[string]any{"id": 3, "region": "apac"}),
		cowMsg(t, "upsert", map[string]any{"id": 4, "region": "us", "payload": "FOUR"}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assertAllManifestsData(t, ctx, final)
	got := scanPartitionedRows(t, ctx, final)
	want := map[int64][2]string{
		1: {"us", "one"},
		2: {"eu", "TWO"},
		4: {"us", "FOUR"},
	}
	assert.Equal(t, want, got, "id=3 deleted; id=2 updated; id=4 inserted — all bucket-partitioned")
}

// TestCOWCaseInsensitiveUpsert (T-3) proves copy-on-write matches a merge key
// case-insensitively end-to-end: the table column is "Id", the mutating messages
// key it as "ID" and "id", and caseSensitive is false. The upsert must rewrite
// the intended row (no duplicate, no missed match), exercising both cowKeyFields
// (filter side) and lookupField in buildCOWRecordFactory (storage side) under
// case folding.
func TestCOWCaseInsensitiveUpsert(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "Id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)

	// Seed id=1,2 (canonical "Id" casing) via a case-insensitive writer.
	seedW := cowWriterCI(t, cat.snapshot(), "Id")
	factory, err := seedW.buildCOWRecordFactory(seedTbl.Schema(), toBatch(t, []map[string]any{
		{"Id": int64(1), "payload": "one"},
		{"Id": int64(2), "payload": "two"},
	}))
	require.NoError(t, err)
	rdr, err := factory()
	require.NoError(t, err)
	tx := seedTbl.NewTransaction()
	require.NoError(t, tx.Append(ctx, rdr, nil))
	rdr.Release()
	_, err = tx.Commit(ctx)
	require.NoError(t, err)

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriterCI(t, cat.snapshot(), "Id")
	w.committer = comm

	// Upsert keyed as "ID", another as "id" — both must fold onto column "Id".
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"ID": int64(1), "payload": "ONE"}),
		cowMsg(t, "upsert", map[string]any{"id": int64(2), "payload": "TWO"}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assert.Equal(t, 1, countRowsWithID(t, ctx, final, "Id", 1), "id=1 must be rewritten in place, not duplicated")
	assert.Equal(t, 1, countRowsWithID(t, ctx, final, "Id", 2), "id=2 must be rewritten in place, not duplicated")
	assert.Equal(t, map[int64]string{1: "ONE", 2: "TWO"}, scanRowsBy(t, ctx, final, "Id"))
}

// scanRowsBy scans an {idCol int64, payload string} table into id->payload.
func scanRowsBy(t testing.TB, ctx context.Context, tbl *table.Table, idCol string) map[int64]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()
	out := map[int64]string{}
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices(idCol)[0]).(*array.Int64)
		payArr := rec.Column(rec.Schema().FieldIndices("payload")[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			pay := ""
			if payArr.IsValid(r) {
				pay = payArr.Value(r)
			}
			out[idArr.Value(r)] = pay
		}
	}
	return out
}

// TestCOWDetectNewColumnsCaseFold (T-3) proves cowDetectNewColumns folds case: a
// message field "Extra" against table column "extra" is NOT flagged as new,
// while a genuinely absent "brand_new" IS flagged for schema evolution.
func TestCOWDetectNewColumnsCaseFold(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "Id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "extra", Type: iceberg.PrimitiveTypes.String},
	)
	tbl := newTypedKeyTableFromSchema(t, sc)
	w := cowWriterCI(t, tbl, "Id")

	// "Extra" folds onto "extra": not a new column.
	require.NoError(t, w.cowDetectNewColumns(sc, service.MessageBatch{
		structuredMsg(t, map[string]any{"Id": int64(1), "Extra": "x"}),
	}))

	// "brand_new" has no case-folded match: flagged for evolution.
	err := w.cowDetectNewColumns(sc, service.MessageBatch{
		structuredMsg(t, map[string]any{"Id": int64(1), "brand_new": "y"}),
	})
	require.Error(t, err)
	var evo *BatchSchemaEvolutionError
	require.ErrorAs(t, err, &evo)
	assert.Contains(t, err.Error(), "brand_new")
}

// TestCOWMassageMalformedInput (T-18) pins cowMassage's shape-mismatch branches:
// a scalar where a struct is expected, a map where a list is expected, and a
// scalar where a map is expected each return a descriptive error rather than
// panicking or silently mis-encoding.
func TestCOWMassageMalformedInput(t *testing.T) {
	w := &writer{caseSensitive: true}

	structT := &iceberg.StructType{FieldList: []iceberg.NestedField{{ID: 3, Name: "a", Type: iceberg.PrimitiveTypes.Int64}}}
	_, err := w.cowMassage(structT, 2, "scalar-not-object", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "struct value must be an object")

	listT := &iceberg.ListType{ElementID: 3, Element: iceberg.PrimitiveTypes.String, ElementRequired: false}
	_, err = w.cowMassage(listT, 2, map[string]any{"x": 1}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list value must be an array")

	mapT := &iceberg.MapType{KeyID: 3, KeyType: iceberg.PrimitiveTypes.String, ValueID: 4, ValueType: iceberg.PrimitiveTypes.Int64, ValueRequired: false}
	_, err = w.cowMassage(mapT, 2, "scalar-not-object", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "map value must be an object")

	// A nested malformed leaf is reported with its path context.
	_, err = w.cowMassage(structT, 2, map[string]any{"a": []any{"list-into-int"}}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "struct field \"a\"")
}

// TestCOWWriteEmptyBatchNoOp (T-19) proves an empty batch through writeCOW is a
// no-op: it must not create a snapshot or touch the committer (nil here would
// panic if used).
func TestCOWWriteEmptyBatchNoOp(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	tbl, cat := newCOWTable(t, sc)
	w := cowWriter(t, tbl, "id") // no committer wired

	require.NoError(t, w.writeCOW(ctx, service.MessageBatch{}))
	assert.Nil(t, cat.snapshot().CurrentSnapshot(), "an empty batch must not produce a snapshot")
}

// TestCOWUnsupportedColumnGateThroughWrite (T-19) proves a copy-on-write mutating
// write against a table with an unsupported column type surfaces the schema-gate
// error through Write (before any commit), rather than corrupting or dropping the
// column. timestamp_ns is a valid Iceberg type but outside the copy-on-write
// supported set.
func TestCOWUnsupportedColumnGateThroughWrite(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "at", Type: iceberg.PrimitiveTypes.TimestampNs},
	)
	// timestamp_ns is only valid in a v3 table, so build one directly (the v2
	// helper would reject the schema before we could reach the copy-on-write gate).
	location := filepath.ToSlash(t.TempDir())
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location,
		iceberg.Properties{table.PropertyFormatVersion: "3"})
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "cow_ns"},
		location:         location,
	}
	tbl := cat.snapshot()
	w := cowWriter(t, tbl, "id") // no committer: the gate must fire first

	err = w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": int64(1), "at": time.Now()}),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support column")
	assert.Contains(t, err.Error(), "timestamp_ns")
}

// TestCOWInsertPlusUpsertSameKeyDuplicates (CORR-6) pins the documented contract
// that an insert and an upsert of the SAME key in one batch produce a duplicate:
// insert is an unconditional append and is deliberately not keyed, so it is not
// collapsed against the upsert (splitByOperation). Operators must map create
// events to upsert, not insert, for keyed data. This test fixes that behaviour
// so a future change to it is a conscious decision.
func TestCOWInsertPlusUpsertSameKeyDuplicates(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, seedTbl, "id")
	w.committer = comm

	// insert id=1 and upsert id=1 in the same batch.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "insert", map[string]any{"id": int64(1), "payload": "A"}),
		cowMsg(t, "upsert", map[string]any{"id": int64(1), "payload": "B"}),
	}))

	final := cat.snapshot()
	assert.Equal(t, 2, countRowsWithID(t, ctx, final, "id", 1),
		"insert + upsert of the same key in one batch is not de-duplicated — the contract in splitByOperation")
}

// --- test helpers --------------------------------------------------------------

// newAmpTableWithSchema builds an unpartitioned v2 table for the given schema,
// backed by an in-memory catalog and the local filesystem. write.delete.mode is
// deliberately left unset, mirroring a connector-auto-created table, so the
// committer's explicit copy-on-write property-set path is exercised.
func newAmpTableWithSchema(t testing.TB, sc *iceberg.Schema) (*table.Table, *memCatalog) {
	t.Helper()
	location := filepath.ToSlash(t.TempDir())
	props := iceberg.Properties{
		table.PropertyFormatVersion: "2",
	}
	meta, err := table.NewMetadata(sc, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, props)
	require.NoError(t, err)
	cat := &memCatalog{
		meta:             meta,
		metadataLocation: fmt.Sprintf("%s/metadata/00001-%s.metadata.json", location, uuid.New()),
		ident:            table.Identifier{"default", "cow"},
		location:         location,
	}
	return cat.snapshot(), cat
}

// appendCOWRows appends a batch of (id, payload) rows as one plain-data-file
// snapshot and returns the updated table handle.
func appendCOWRows(t testing.TB, ctx context.Context, tbl *table.Table, rows map[int64]string) *table.Table {
	t.Helper()
	arrowSc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	list := make([]map[string]any, 0, len(rows))
	for id, pay := range rows {
		list = append(list, map[string]any{"id": strconv.FormatInt(id, 10), "payload": pay})
	}
	b, err := json.Marshal(list)
	require.NoError(t, err)

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrowSc, bytes.NewReader(b))
	require.NoError(t, err)
	rdr, err := array.NewRecordReader(arrowSc, []arrow.RecordBatch{rec})
	require.NoError(t, err)
	rec.Release()
	defer rdr.Release()

	tx := tbl.NewTransaction()
	require.NoError(t, tx.Append(ctx, rdr, nil))
	next, err := tx.Commit(ctx)
	require.NoError(t, err)
	return next
}

// scanRows scans the table into an id->payload map, honouring any deletes.
func scanRows(t testing.TB, ctx context.Context, tbl *table.Table) map[int64]string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	out := map[int64]string{}
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idIdx := rec.Schema().FieldIndices("id")[0]
		payIdx := rec.Schema().FieldIndices("payload")[0]
		idArr := rec.Column(idIdx).(*array.Int64)
		payArr := rec.Column(payIdx).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			pay := ""
			if payArr.IsValid(r) {
				pay = payArr.Value(r)
			}
			out[idArr.Value(r)] = pay
		}
	}
	return out
}

// TestCommitOverwriteNoLeakOnConflictThenSuccess (T-7, CORR-2) proves the fix for
// the orphan-parquet leak on a clean-conflict-then-success retry. commitLocked
// re-runs the overwrite stage on every attempt, writing a fresh set of parquet
// files each time; here attempt 1 loses a clean 409 (nothing lands) and attempt 2
// succeeds. The winning snapshot's files must survive while attempt 1's files are
// cleaned, so the final on-disk parquet count is exactly the seed files (protected
// because they existed before the commit) plus the winning snapshot's files — no
// leak. Before CORR-2 (cleanup ran only on the error path) attempt 1's files
// leaked and this count was higher.
func TestCommitOverwriteNoLeakOnConflictThenSuccess(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())
	require.Positive(t, seedCount)

	// attempt 1 = clean conflict (nothing lands), attempt 2 = success.
	cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitConflict}}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))
	assert.Equal(t, 2, cat.calls, "a clean conflict must force a second attempt")

	final := cat.snapshot()
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, final))

	// On-disk parquet must be exactly the seed files (in the before-snapshot, so
	// protected) plus the files the winning snapshot references (disjoint from the
	// seed, since the overwrite rewrote them into fresh files). If attempt 1's
	// files had leaked, the count would be strictly larger.
	referenced, err := comm.referencedDataFilePaths(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, referenced)
	assert.Equal(t, seedCount+len(referenced), countParquetFiles(t, final.Location()),
		"attempt 1's orphaned parquet must have been cleaned even though the overall commit succeeded")
}

// TestCommitOverwritePreservesFilesOnTerminalUnknown (T-6, CORR-2) proves the
// durability-critical skip: when a copy-on-write commit exhausts its retries with
// ErrCommitStateUnknown, commitOverwrite must return the wrapped unknown error AND
// leave the written parquet files in place. A possibly-landed commit's files may
// belong to a snapshot that committed server-side, so deleting them could corrupt
// the table — they are left for Iceberg orphan-file maintenance instead.
func TestCommitOverwritePreservesFilesOnTerminalUnknown(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())

	// Every attempt returns ErrCommitStateUnknown WITHOUT landing, so the commit-id
	// never appears on reload and the loop runs to exhaustion. (A landed unknown
	// would instead be detected by the commit-id and short-circuit to success, so
	// it could not exhaust — hence commitUnknownNoLand here.)
	const maxRetries = 3
	outcomes := make([]commitOutcome, maxRetries)
	for i := range outcomes {
		outcomes[i] = commitUnknownNoLand
	}
	cat := &scriptedCatalog{memCatalog: mem, outcomes: outcomes}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: maxRetries},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)
	assert.ErrorIs(t, err, rest.ErrCommitStateUnknown, "the terminal error must be ErrCommitStateUnknown")
	assert.Equal(t, maxRetries, cat.calls, "the commit must exhaust every retry")
	assert.Greater(t, countParquetFiles(t, seedTbl.Location()), seedCount,
		"the overwrite's parquet files must be preserved (NOT cleaned) on a possibly-landed unknown state")
}

// TestCommitOverwriteUnknownStateGuardIsStickyAcrossRetries pins the STICKY
// half of the unknown-state cleanup guard: cleanup must be skipped if ANY
// attempt's outcome remains ambiguous, not just the last. Attempt 1 lands the
// overwrite server-side but reports ErrCommitStateUnknown; the reload that
// could resolve the ambiguity fails persistently (the correlated outage — the
// same catalog that returned the ambiguous response cannot serve fresh
// metadata either), so committedSnapshotHasID never observes the landed
// commit; every later attempt re-stages against the stale table and cleanly
// conflicts until retries exhaust. The LAST attempt's error is a plain
// ErrCommitFailed — before the sticky fix commitLocked wrapped only that,
// cleanup saw no unknown state, and its reference scan (run against the stale
// pre-commit c.table, so it COMPLETES and slips past the fail-closed guard)
// deleted the landed snapshot's data files: table corruption. The ambiguity
// must instead stay sticky: the returned error is unknown-class, cleanup is
// skipped entirely, every recorded file survives, and the committed table
// still scans correctly with the mutation applied exactly once.
func TestCommitOverwriteUnknownStateGuardIsStickyAcrossRetries(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})

	// Attempt 1 applies the overwrite but reports unknown; attempts 2 and 3
	// cleanly conflict (nothing lands), so the loop exhausts with a terminal
	// error that is NOT unknown-class on its own.
	const maxRetries = 3
	cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitLandThenUnknown, commitConflict, commitConflict}}
	reload := func(context.Context) (*table.Table, error) {
		return nil, errors.New("catalog reload unavailable")
	}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: maxRetries}, reload, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)
	assert.ErrorIs(t, err, rest.ErrCommitStateUnknown,
		"an ambiguous attempt that was never resolved must keep the terminal error unknown-class, even though the last attempt failed cleanly")
	assert.Equal(t, maxRetries, cat.calls, "the commit must exhaust every retry")

	// Cleanup must have been skipped entirely: every recorded file — attempt
	// 1's landed files included — must still be on disk.
	written := comm.writes.snapshot()
	require.NotEmpty(t, written, "the overwrite attempts must have recorded written files")
	for p := range written {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr, "no recorded file may be removed while an attempt's outcome is ambiguous: %s", p)
	}

	// The landed snapshot is intact: the mutation applied exactly once and the
	// table scans correctly (a deleted data file would fail the scan).
	final := cat.snapshot()
	assert.Equal(t, 1, countSnapshotsWithCommitID(final), "the overwrite must have landed exactly once")
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, final))
}

// TestCommitOverwriteCleanupRunsOnDefinitiveFailures is the counterweight to
// the ambiguity guard: cleanup requires proof that no attempt could still
// land, and a retry history made up ONLY of definitive server-side rejections
// (clean 409 conflicts) plus at most a final success is exactly that proof.
// The hygiene path must therefore still run for these shapes — the allowlist
// fix must not turn every failed commit into an orphan leak. (Since token
// absence no longer counts as resolving an ambiguous attempt, these
// no-ambiguity-anywhere histories are the ONLY failure shapes where cleanup
// runs; an unknown or transport-failed attempt anywhere in the history keeps
// it skipped — see TestCommitOverwriteAmbiguityNotClearedByTokenAbsence.)
func TestCommitOverwriteCleanupRunsOnDefinitiveFailures(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	// Every attempt loses a clean 409 (nothing lands anywhere): all attempts'
	// files are provable orphans and cleanup reclaims them all.
	t.Run("exhausted conflicts", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		seedCount := countParquetFiles(t, seedTbl.Location())
		require.Positive(t, seedCount, "seeding must have written data files")

		const maxRetries = 3
		cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitConflict, commitConflict, commitConflict}}
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: maxRetries},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm

		err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
		require.Error(t, err)
		assert.ErrorIs(t, err, rest.ErrCommitFailed, "the terminal error must surface the exhausted conflicts")
		assert.NotErrorIs(t, err, rest.ErrCommitStateUnknown,
			"clean 409s are definitive rejections; no ambiguity marker may be attached")
		assert.Equal(t, maxRetries, cat.calls, "the commit must exhaust every retry")

		assert.Zero(t, countSnapshotsWithCommitID(cat.snapshot()), "nothing may have landed")
		assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
			"cleanup must reclaim every attempt's files: definitive rejections prove none of them can land")
	})

	// Attempt 1 loses a clean 409, attempt 2 lands: the loser's files are
	// provable orphans and the retried-success trigger reclaims them, while the
	// winner's referenced files survive.
	t.Run("conflict then success", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		seedCount := countParquetFiles(t, seedTbl.Location())
		require.Positive(t, seedCount, "seeding must have written data files")

		cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitConflict}}
		comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm

		require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))
		require.Equal(t, 2, cat.calls, "the conflict must force a second attempt")

		referenced, err := comm.referencedDataFilePaths(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, referenced)
		assert.Equal(t, seedCount+len(referenced), countParquetFiles(t, seedTbl.Location()),
			"attempt 1's orphans must be reclaimed on the retried success; the winner's files survive")
		assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, cat.snapshot()))
	})
}

// lateLandingOutcome scripts one CommitTable call of a lateLandingCatalog.
type lateLandingOutcome struct {
	err     error // non-nil: return this error WITHOUT applying the update
	capture bool  // stash the call's updates so landPending can apply them later
}

// lateLandingCatalog models the ambiguous commit whose server-side apply
// finishes only after the client has given up: a scripted CommitTable call
// returns its error without applying, but captures the update so the test can
// land it "server-side" later via landPending. Calls beyond the script
// succeed normally through the embedded memCatalog.
type lateLandingCatalog struct {
	*memCatalog
	outcomes []lateLandingOutcome
	calls    int
	pending  [][]table.Update
}

func (c *lateLandingCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	var o lateLandingOutcome
	if c.calls < len(c.outcomes) {
		o = c.outcomes[c.calls]
	}
	c.calls++
	if o.err == nil {
		return c.memCatalog.CommitTable(ctx, ident, reqs, updates)
	}
	if o.capture {
		c.pending = append(c.pending, updates)
	}
	return nil, "", o.err
}

// landPending applies every captured update, oldest first — the server
// finishing the ambiguous requests after the client stopped waiting.
func (c *lateLandingCatalog) landPending(t testing.TB, ctx context.Context) {
	t.Helper()
	for _, updates := range c.pending {
		_, _, err := c.memCatalog.CommitTable(ctx, c.ident, nil, updates)
		require.NoError(t, err, "the late landing must apply cleanly")
	}
	c.pending = nil
}

func (c *lateLandingCatalog) snapshot() *table.Table {
	return table.New(c.ident, c.meta, c.metadataLocation,
		func(context.Context) (iceio.IO, error) { return iceio.LocalFS{}, nil }, c)
}

// TestCommitOverwriteTransportErrorIsAmbiguous pins hole (a) of the cleanup
// ambiguity guard: iceberg-go's rest catalog maps only HTTP 500/502/503/504
// to ErrCommitStateUnknown, so a client-side transport failure on the commit
// POST — timeout, connection reset, EOF — surfaces as a raw error. That is
// the textbook ambiguous outcome (the server may finish applying after the
// client stops waiting), so commitOverwrite must treat it exactly like an
// unknown state: surface an unknown-class error and skip the destructive
// orphan cleanup, while retry policy stays unchanged (the raw error is still
// not retried). The commit then lands late, AFTER the write returned; had
// cleanup run, the landed snapshot would reference deleted files.
func TestCommitOverwriteTransportErrorIsAmbiguous(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"url error wrapping a context deadline", &url.Error{
			Op:  "Post",
			URL: "https://catalog.example/v1/namespaces/ns/tables/t",
			Err: context.DeadlineExceeded,
		}},
		{"bare connection reset", errors.New("read tcp 10.1.2.3:52341->10.9.8.7:443: read: connection reset by peer")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			seedTbl, mem := newCOWTable(t, sc)
			seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
			seedCount := countParquetFiles(t, seedTbl.Location())
			require.Positive(t, seedCount, "seeding must have written data files")

			cat := &lateLandingCatalog{memCatalog: mem, outcomes: []lateLandingOutcome{{err: tc.err, capture: true}}}
			comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
				func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
			require.NoError(t, err)
			defer comm.Close()
			w := cowWriter(t, cat.snapshot(), "id")
			w.committer = comm

			err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
			require.Error(t, err)
			assert.ErrorIs(t, err, rest.ErrCommitStateUnknown,
				"a raw transport failure on the commit request must surface as unknown-class: no verdict ever arrived")
			assert.Equal(t, 1, cat.calls, "retry policy is unchanged: a raw transport error is not retried")

			// Cleanup must have been skipped: every recorded file is still on
			// disk, because the ambiguous attempt may still land.
			written := comm.writes.snapshot()
			require.NotEmpty(t, written, "the overwrite attempt must have recorded written files")
			for p := range written {
				_, statErr := os.Stat(p)
				assert.NoError(t, statErr, "no recorded file may be removed after an ambiguous transport failure: %s", p)
			}
			assert.Greater(t, countParquetFiles(t, seedTbl.Location()), seedCount,
				"the ambiguous attempt's parquet files must be preserved")

			// The server finishes applying afterwards. The landed snapshot must
			// be complete: exactly one application, all referenced files intact.
			cat.landPending(t, ctx)
			assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "the ambiguous attempt landed exactly once")
			assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, cat.snapshot()),
				"the late-landed snapshot must scan cleanly: none of its files were deleted")
		})
	}
}

// TestCommitOverwriteAmbiguityNotClearedByTokenAbsence pins hole (b) of the
// cleanup ambiguity guard: a successful reload that does NOT show the
// commit-id token proves only that the ambiguous attempt has not landed YET —
// the server may still be mid-apply — so it must not clear the guard.
// Attempt 1 returns a 5xx unknown without applying (the server holds the
// request); the reload succeeds and finds no token; attempt 2 loses a
// definitive 409 and retries exhaust. The guard must stay sticky — the
// terminal error is unknown-class and cleanup is skipped — because attempt 1
// then lands late. Before the fix, the token-absent reload cleared the guard,
// the definitive 409 unlocked cleanup, and the late-landing snapshot's files
// were deleted: table corruption.
func TestCommitOverwriteAmbiguityNotClearedByTokenAbsence(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})

	const maxRetries = 2
	cat := &lateLandingCatalog{memCatalog: mem, outcomes: []lateLandingOutcome{
		{err: rest.ErrCommitStateUnknown, capture: true}, // ambiguous, lands late
		{err: rest.ErrCommitFailed},                      // definitive clean 409
	}}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: maxRetries},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)
	assert.ErrorIs(t, err, rest.ErrCommitFailed, "the terminal error must surface the exhausted conflict")
	assert.ErrorIs(t, err, rest.ErrCommitStateUnknown,
		"a token-absent reload must not clear the ambiguity: the terminal error must stay unknown-class")
	assert.Equal(t, maxRetries, cat.calls, "the commit must exhaust every retry")

	// Cleanup must have been skipped entirely — attempt 1's files included.
	written := comm.writes.snapshot()
	require.NotEmpty(t, written, "the overwrite attempts must have recorded written files")
	for p := range written {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr, "no recorded file may be removed while an attempt may still land: %s", p)
	}

	// Attempt 1 lands late. Its snapshot must be complete and scan cleanly.
	cat.landPending(t, ctx)
	assert.Equal(t, 1, countSnapshotsWithCommitID(cat.snapshot()), "the ambiguous attempt landed exactly once")
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, cat.snapshot()),
		"the late-landed snapshot must scan cleanly: none of its files were deleted")
}

// TestCommitOverwriteProhibitedKeysRejectionIsDefinitive pins that a
// catalog's prohibited-property rejection stays on the definitive allowlist:
// it is a validation verdict (a 400 in the wild), so the terminal-failure
// hygiene path must still reclaim the failed overwrite's files — the
// ambiguity guard must not turn engine-catalog rejections into orphan leaks.
// The key named here was never sent, so nothing is learned and the rejection
// is terminal on the first attempt. (The learn-strip-retry-succeed flow is
// pinned by TestCOWOverwriteStripsProhibitedKeys.)
func TestCommitOverwriteProhibitedKeysRejectionIsDefinitive(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())
	require.Positive(t, seedCount, "seeding must have written data files")

	cat := &alwaysProhibitingCatalog{memCatalog: mem, keys: []string{"some.innocent.key"}}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)
	assert.NotErrorIs(t, err, rest.ErrCommitStateUnknown,
		"a prohibited-keys rejection is a definitive catalog verdict, not an ambiguity")
	assert.Equal(t, 1, cat.commits, "an unlearnable prohibited-keys rejection must be terminal on the first attempt")
	assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
		"the definitive rejection must still unlock cleanup: the failed overwrite's files are reclaimed")
}

// TestCommitOverwriteResumesAfterReloadFailures (T-12, CORR-3) proves the commit-id
// idempotency check resumes correctly even when the reload after a lost-ack
// conflict fails several times before recovering. Attempt 1 lands the overwrite
// server-side but reports a conflict; the next reloads fail, and each retry cleanly
// conflicts (nothing lands) so no double-apply is possible. Once a reload finally
// succeeds, the token is found in the reloaded snapshot and the commit returns
// success — the mutation lands exactly once, with no duplicate rows.
func TestCommitOverwriteResumesAfterReloadFailures(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	_ = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})

	// attempt 1 lands but reports ErrCommitFailed; later attempts cleanly conflict.
	cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitLandThenFail, commitConflict, commitConflict}}

	// Reload fails its first two calls, then recovers. Commits are serialized under
	// the committer's lock and reload runs inside it, so a plain counter is safe.
	var reloadCalls int
	reload := func(context.Context) (*table.Table, error) {
		reloadCalls++
		if reloadCalls <= 2 {
			return nil, errors.New("catalog reload unavailable")
		}
		return cat.snapshot(), nil
	}
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 5}, reload, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))

	final := cat.snapshot()
	assert.Equal(t, 1, countSnapshotsWithCommitID(final),
		"the mutation must land exactly once even though the first reloads failed")
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, final),
		"no duplicate rows: the landed commit was detected once reload recovered")
}

// nonListableFS forwards reads and writes to the local filesystem but deliberately
// omits WalkDir, so it satisfies iceio.IO / WriteFileIO but NOT iceio.ListableIO.
// It lets a copy-on-write commit write real parquet on a filesystem that cannot
// be listed, proving recording-based orphan cleanup needs no directory walking
// (and, via newRecordingIO's fidelity rules, that the committer's wrapper does
// not invent a ListableIO the underlying FS lacks).
type nonListableFS struct{ inner iceio.LocalFS }

func (f nonListableFS) Open(name string) (iceio.File, error)         { return f.inner.Open(name) }
func (f nonListableFS) Create(name string) (iceio.FileWriter, error) { return f.inner.Create(name) }
func (f nonListableFS) WriteFile(name string, p []byte) error        { return f.inner.WriteFile(name, p) }
func (f nonListableFS) Remove(name string) error                     { return f.inner.Remove(name) }

// TestCommitOverwriteCleansUpWithoutListableFS (T-14, repurposed) proves orphan
// cleanup no longer depends on the filesystem being listable. The original test
// pinned a graceful degradation — cleanup used a WalkDir-based directory diff,
// so a non-listable FS had to skip it, leaking the failed commit's files. With
// authorship tracking the committer records the paths it writes at Create/
// WriteFile time (writeRecorder), which works on any WriteFileIO, so a failing
// copy-on-write commit on a non-listable FS now cleans its own files too.
func TestCommitOverwriteCleansUpWithoutListableFS(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	// Seed via the normal (listable) handle so real data files exist to rewrite.
	seedTbl, mem := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())

	// A catalog that always fails the commit with a definitive, non-retryable
	// rejection (so cleanup may run), and a table handle whose FS is writable
	// but non-listable.
	fc := &flakyCatalog{memCatalog: mem, failuresLeft: 1 << 30, failErr: fmt.Errorf("commit rejected: %w", rest.ErrBadRequest)}
	nlSnap := table.New(fc.ident, fc.meta, fc.metadataLocation,
		func(context.Context) (iceio.IO, error) { return nonListableFS{}, nil }, fc)
	comm, err := NewCommitter(nlSnap, fc, CommitConfig{MaxRetries: 2},
		func(context.Context) (*table.Table, error) { return nlSnap, nil }, service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, nlSnap, "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err, "the failing commit must surface an error, not panic")

	// Recording-based cleanup needs no WalkDir: the failed commit's files are
	// reclaimed even though the FS cannot be listed, leaving only the seed.
	assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
		"a non-listable FS must still clean the failed commit's recorded files")
}

// TestCleanupOverwriteReferenceGuard (T-15) proves cleanup never deletes a file
// the current snapshot still references, even when that file is in the recorded
// (authored-by-us) set handed to it. That is exactly the retried-success shape:
// the winning attempt's files are recorded alongside the losing attempts', and
// only the referenced[p] guard tells them apart — the winners must survive
// while a genuinely unreferenced recorded orphan is removed.
func TestCleanupOverwriteReferenceGuard(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two"})

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 1}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()

	referenced, err := comm.referencedDataFilePaths(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, referenced, "the seeded snapshot must reference at least one data file")

	// A genuine orphan under data/ that no snapshot references.
	orphan := filepath.Join(seedTbl.Location(), "data", "orphan-"+uuid.NewString()+".parquet")
	require.NoError(t, os.WriteFile(orphan, []byte("not a real parquet"), 0o644))

	// Hand cleanup a recorded set containing BOTH the live (referenced) files
	// and the orphan: only the reference guard can save the live files.
	written := map[string]struct{}{orphan: {}}
	for p := range referenced {
		written[p] = struct{}{}
	}
	comm.cleanupOrphanedOverwriteFiles(ctx, written)

	for p := range referenced {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr, "a file referenced by the current snapshot must survive cleanup: %s", p)
	}
	_, statErr := os.Stat(orphan)
	assert.True(t, os.IsNotExist(statErr), "an unreferenced recorded orphan must be removed")
}

// failingOpenFS forwards to the local filesystem but, while armed, fails every
// Open of a path with the configured suffix. Arming it with suffix ".avro"
// after a commit has landed simulates a transient storage outage that hits
// exactly the post-commit reference scan (manifest lists and manifests are
// avro) while leaving parquet reads and all writes untouched.
type failingOpenFS struct {
	inner  iceio.LocalFS
	armed  *atomic.Bool
	suffix string
}

func (f failingOpenFS) Open(name string) (iceio.File, error) {
	if f.armed.Load() && strings.HasSuffix(name, f.suffix) {
		return nil, fmt.Errorf("simulated storage outage opening %s", name)
	}
	return f.inner.Open(name)
}
func (f failingOpenFS) Create(name string) (iceio.FileWriter, error) { return f.inner.Create(name) }
func (f failingOpenFS) WriteFile(name string, p []byte) error        { return f.inner.WriteFile(name, p) }
func (f failingOpenFS) Remove(name string) error                     { return f.inner.Remove(name) }

// TestCommitOverwriteCleanupFailsClosedOnIncompleteReferenceScan pins the
// fail-closed semantics of orphan cleanup: when the post-commit reference scan
// cannot be completed, cleanup must delete NOTHING. The scenario is a
// retried-success copy-on-write commit (attempt 1 loses a clean conflict,
// attempt 2 lands), so the recorded set holds BOTH attempts' files — including
// the files the landed snapshot references. A transient manifest-read failure
// is injected AFTER the commit lands (the failing FS is armed at the start of
// attempt 2's CommitTable, by which point the stage has done all its reads):
// with the old best-effort scan, `referenced` came back empty and cleanup
// deleted the landed snapshot's data files, corrupting the table. Now the scan
// error must skip cleanup entirely, leaving every recorded file on disk and
// the committed snapshot fully readable.
func TestCommitOverwriteCleanupFailsClosedOnIncompleteReferenceScan(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, mem := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
	seedCount := countParquetFiles(t, seedTbl.Location())

	// attempt 1 = clean conflict, attempt 2 = success; the hook arms the failing
	// FS when attempt 2's CommitTable is in flight, so the outage begins only
	// once the commit lands and the first reads it can hit are cleanup's.
	armed := &atomic.Bool{}
	cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitConflict}}
	hooked := &commitHookCatalog{CatalogIO: cat, hook: func(call int) {
		if call == 2 {
			armed.Store(true)
		}
	}}
	// Every table handle the committer uses (initial and reloaded) carries the
	// failing FS, mirroring how a real committer sees one filesystem throughout.
	snapFail := func() *table.Table {
		return table.New(mem.ident, mem.meta, mem.metadataLocation,
			func(context.Context) (iceio.IO, error) {
				return failingOpenFS{armed: armed, suffix: ".avro"}, nil
			}, hooked)
	}

	var logBuf bytes.Buffer
	logger := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&logBuf, nil)))
	comm, err := NewCommitter(snapFail(), hooked, CommitConfig{MaxRetries: 3},
		func(context.Context) (*table.Table, error) { return snapFail(), nil }, logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}),
		"the retried commit must succeed; only cleanup is hit by the outage")
	require.Equal(t, 2, cat.calls, "the conflict must force a second attempt")
	assert.True(t, armed.Load(), "the manifest-read outage must have been armed mid-commit")

	// Fail closed: every recorded file — both attempts' — must still be on disk.
	written := comm.writes.snapshot()
	require.NotEmpty(t, written, "the retried commit must have recorded written files")
	for p := range written {
		_, statErr := os.Stat(p)
		assert.NoError(t, statErr, "no recorded file may be removed when the reference scan is incomplete: %s", p)
	}
	assert.Greater(t, countParquetFiles(t, seedTbl.Location()), seedCount,
		"the landed snapshot's files (and attempt 1's leftovers) must remain on disk")
	assert.Contains(t, logBuf.String(), "could not verify which files the current snapshot references",
		"skipping cleanup must be logged as a warning")

	// With the outage over, the committed snapshot must still scan correctly —
	// i.e. nothing it references was deleted.
	armed.Store(false)
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, cat.snapshot()))
}

// commitHookCatalog wraps a table.CatalogIO, invoking hook (with the 1-based
// call number) at the start of every CommitTable before delegating. It lets a
// test inject a side effect at the exact moment a commit is in flight — after
// the copy-on-write stage has written its files but before the outcome is
// known — which is when a concurrent writer's files can appear.
type commitHookCatalog struct {
	table.CatalogIO
	hook  func(call int)
	mu    sync.Mutex
	calls int
}

func (h *commitHookCatalog) CommitTable(ctx context.Context, ident table.Identifier, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	h.mu.Lock()
	h.calls++
	n := h.calls
	h.mu.Unlock()
	if h.hook != nil {
		h.hook(n)
	}
	return h.CatalogIO.CommitTable(ctx, ident, reqs, updates)
}

// TestCommitOverwriteCleanupSparesForeignFiles pins the concurrent-writer
// safety of authorship-tracked orphan cleanup on BOTH of its triggers. A
// foreign parquet file — standing in for another committer's written-but-not-
// yet-committed data file — appears in the table's data directory while our
// commit is in flight (planted by the catalog hook, i.e. after our stage wrote
// its files). It is referenced by no snapshot, so the old directory-diff
// design deleted it, corrupting the other writer's pending commit; cleanup
// must now leave it untouched because we did not author it, while still
// reclaiming our own orphans.
func TestCommitOverwriteCleanupSparesForeignFiles(t *testing.T) {
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	logger := service.MockResources().Logger()

	// plantOnFirstCall writes the foreign file when the first commit attempt is
	// in flight.
	plantOnFirstCall := func(t *testing.T, foreign string) func(int) {
		return func(call int) {
			if call == 1 {
				require.NoError(t, os.WriteFile(foreign, []byte("another writer's in-flight parquet"), 0o644))
			}
		}
	}

	t.Run("on terminal failure", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		seedCount := countParquetFiles(t, seedTbl.Location())
		foreign := filepath.Join(seedTbl.Location(), "data", "inflight-"+uuid.NewString()+".parquet")

		// Definitive rejection so failure-path cleanup runs at all.
		fc := &flakyCatalog{memCatalog: mem, failuresLeft: 1 << 30, failErr: fmt.Errorf("commit rejected: %w", rest.ErrBadRequest)}
		hooked := &commitHookCatalog{CatalogIO: fc, hook: plantOnFirstCall(t, foreign)}
		comm, err := NewCommitter(fc.snapshot(), hooked, CommitConfig{MaxRetries: 2},
			func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, fc.snapshot(), "id")
		w.committer = comm

		err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
		require.Error(t, err)

		_, statErr := os.Stat(foreign)
		assert.NoError(t, statErr, "a foreign in-flight file must survive failure-path cleanup")
		assert.Equal(t, seedCount+1, countParquetFiles(t, seedTbl.Location()),
			"our failed attempts' files must be cleaned while the foreign file is spared")
	})

	t.Run("on retried success", func(t *testing.T) {
		ctx := t.Context()
		seedTbl, mem := newCOWTable(t, sc)
		seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})
		seedCount := countParquetFiles(t, seedTbl.Location())
		foreign := filepath.Join(seedTbl.Location(), "data", "inflight-"+uuid.NewString()+".parquet")

		// Attempt 1 is a clean conflict, attempt 2 succeeds — the retried-success
		// cleanup trigger.
		cat := &scriptedCatalog{memCatalog: mem, outcomes: []commitOutcome{commitConflict}}
		hooked := &commitHookCatalog{CatalogIO: cat, hook: plantOnFirstCall(t, foreign)}
		comm, err := NewCommitter(cat.snapshot(), hooked, CommitConfig{MaxRetries: 3},
			func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
		require.NoError(t, err)
		defer comm.Close()
		w := cowWriter(t, cat.snapshot(), "id")
		w.committer = comm

		require.NoError(t, w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})}))
		require.Equal(t, 2, cat.calls, "the conflict must force a second attempt")

		_, statErr := os.Stat(foreign)
		assert.NoError(t, statErr, "a foreign in-flight file must survive retried-success cleanup")

		// Final on-disk parquet: the protected seed, the spared foreign file, and
		// the winning attempt's referenced files — attempt 1's orphans cleaned.
		referenced, err := comm.referencedDataFilePaths(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, referenced)
		assert.Equal(t, seedCount+1+len(referenced), countParquetFiles(t, seedTbl.Location()),
			"attempt 1's orphans must be cleaned while the foreign file and winner survive")
		assert.Equal(t, map[int64]string{1: "one", 2: "TWO", 3: "three"}, scanRows(t, ctx, cat.snapshot()))
	})
}

// TestCommitOverwriteReturnsNewReaderError (T-17) proves a factory error from
// OverwriteInput.NewReader is surfaced by commitOverwrite (the stage fails before
// any file is written), and because that error is not an ambiguous unknown state,
// the cleanup path runs — with no new files written it leaves the seed untouched.
func TestCommitOverwriteReturnsNewReaderError(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two"})
	seedCount := countParquetFiles(t, seedTbl.Location())

	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	defer comm.Close()

	readerErr := errors.New("reader factory boom")
	err = comm.commitOverwrite(ctx, OverwriteInput{
		Filter:    nil, // unused: NewReader errors before the filter is applied
		NewReader: func() (array.RecordReader, error) { return nil, readerErr },
		SchemaID:  comm.currentSchemaID(),
	})
	require.ErrorIs(t, err, readerErr, "the NewReader factory error must propagate")
	assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
		"cleanup runs on a non-unknown failure; with no new files written the seed is untouched")
}

// assertAllManifestsData asserts every manifest in the current snapshot is
// data-content (no delete manifests), i.e. the table holds only plain data
// files.
func assertAllManifestsData(t testing.TB, ctx context.Context, tbl *table.Table) {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap)
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(t, err)
	for _, m := range manifests {
		assert.Equal(t, iceberg.ManifestContentData, m.ManifestContent(), "expected only data manifests")
	}
}
