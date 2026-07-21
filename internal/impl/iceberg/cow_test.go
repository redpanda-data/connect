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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
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
		// An unknown enum value is rejected somewhere along the parse pipeline
		// (spec validation or the defensive switch in parseRowOpConfig).
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
		if yamlErr != nil {
			return // rejected at spec-validation time
		}
		_, err := parseRowOpConfig(conf)
		require.Error(t, err)
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
	assert.Equal(t, iceberg.OpIn, filter.Op(), "two distinct keys on one column is an IN predicate")
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

// TestBuildCOWFilterBareNumberTemporalKeyRejected pins the CON-490 guard on the
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

	// A non-retryable failure guarantees the mutation's commit does not land, so
	// the files the overwrite wrote are genuine orphans.
	fc := &flakyCatalog{memCatalog: cat, failuresLeft: 1 << 30, failErr: errors.New("storage unavailable")}
	comm, err := NewCommitter(fc.snapshot(), CommitConfig{MaxRetries: 2}, func(context.Context) (*table.Table, error) { return fc.snapshot(), nil }, logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, fc.snapshot(), "id")
	w.committer = comm

	err = w.Write(ctx, service.MessageBatch{cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"})})
	require.Error(t, err)

	assert.Equal(t, seedCount, countParquetFiles(t, seedTbl.Location()),
		"the failed copy-on-write commit's parquet files must be cleaned up, leaving only the seed files")
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
		comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, func(context.Context) (*table.Table, error) { return cat.snapshot(), nil }, logger)
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3, SkipFormatUpgrade: true}, reloadFn(cat), service.MockResources().Logger())
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
	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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

	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
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
