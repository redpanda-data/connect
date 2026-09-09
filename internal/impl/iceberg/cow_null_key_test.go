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
	"fmt"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// These tests pin the null-safety of the copy-on-write rewrite filter.
//
// iceberg-go's copy-on-write keeps survivor rows by scanning each rewritten
// file with NOT(filter) (transaction.go rewriteFilesWithFilter), executed
// through arrow's compute.Filter with DefaultFilterOptions, whose zero-value
// NullSelectionBehavior is DropNulls. A null-PROPAGATING key predicate (the
// EqualTo a single-distinct-key batch collapses to, and the EqualTo conjuncts
// of every composite-key clause) evaluates to NULL on a NULL-key row, so
// NOT(filter) is NULL and the row is silently DROPPED from the rewritten file
// — data loss for every legitimately-null-keyed bystander. buildCOWFilter
// therefore conjoins NotNull on every key column so the negation is null-safe
// under Kleene logic: NOT(AND(false, NULL)) = NOT(false) = true.

// appendNullKeyRows appends arbitrary rows (which may carry NULL key columns)
// as ONE plain-data-file snapshot, so keyed rows and NULL-key bystanders share
// a data file and a mutation of the keyed rows forces a rewrite of the file
// containing the bystanders.
func appendNullKeyRows(t testing.TB, ctx context.Context, tbl *table.Table, rows []map[string]any) *table.Table {
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

// scanRowsAndNullKeyPayloads scans the table into an id->payload map for rows
// with a non-null id, plus the sorted payloads of every NULL-id row. Unlike
// scanRows it checks id validity, so NULL-key rows are surfaced instead of
// being silently read as id=0.
func scanRowsAndNullKeyPayloads(t testing.TB, ctx context.Context, tbl *table.Table) (map[int64]string, []string) {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	keyed := map[int64]string{}
	var nullKeyed []string
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		idArr := rec.Column(rec.Schema().FieldIndices("id")[0]).(*array.Int64)
		payArr := rec.Column(rec.Schema().FieldIndices("payload")[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			pay := ""
			if payArr.IsValid(r) {
				pay = payArr.Value(r)
			}
			if idArr.IsNull(r) {
				nullKeyed = append(nullKeyed, pay)
				continue
			}
			keyed[idArr.Value(r)] = pay
		}
	}
	slices.Sort(nullKeyed)
	return keyed, nullKeyed
}

// nullableKeyCOWSchema is a table schema whose merge-key column is nullable —
// exactly what the copy-on-write table-creation path produces (identifier
// columns are deliberately left optional) — so NULL-key rows legitimately
// exist alongside keyed rows.
func nullableKeyCOWSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
}

func nullKeyCommitter(t testing.TB, cat *memCatalog) *committer {
	t.Helper()
	comm, err := NewCommitter(cat.snapshot(), cat, CommitConfig{MaxRetries: 3}, reloadFn(cat), service.MockResources().Logger())
	require.NoError(t, err)
	t.Cleanup(comm.Close)
	return comm
}

// TestCOWNullKeyRowsSurviveSingleKeyMutation pins the exact data-loss scenario:
// a data file holding both keyed rows and NULL-key rows is rewritten by a
// mutating batch touching exactly ONE distinct key — the shape SetPredicate
// collapses to a bare (null-propagating) EqualTo and the most common CDC shape.
// The NULL-key bystanders must survive the rewrite, the touched key must be
// mutated, and the table must still contain zero delete files.
func TestCOWNullKeyRowsSurviveSingleKeyMutation(t *testing.T) {
	ctx := t.Context()
	seedTbl, cat := newCOWTable(t, nullableKeyCOWSchema())

	// Seed keyed rows AND NULL-key rows in one snapshot so they share a file.
	seedTbl = appendNullKeyRows(t, ctx, seedTbl, []map[string]any{
		{"id": "1", "payload": "one"},
		{"id": "2", "payload": "two"},
		{"id": nil, "payload": "ghost-1"},
		{"id": nil, "payload": "ghost-2"},
	})

	w := cowWriter(t, seedTbl, "id")
	w.committer = nullKeyCommitter(t, cat)

	// One distinct key: upsert id=2. Pre-fix the filter collapsed to
	// EqualTo(id, 2) and the survivor scan dropped both ghost rows.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO"}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assertAllManifestsData(t, ctx, final)

	keyed, nullKeyed := scanRowsAndNullKeyPayloads(t, ctx, final)
	assert.Equal(t, map[int64]string{1: "one", 2: "TWO"}, keyed, "the touched key must be mutated and untouched keys kept")
	assert.Equal(t, []string{"ghost-1", "ghost-2"}, nullKeyed, "NULL-key rows must survive the copy-on-write rewrite")
}

// TestCOWNullKeyRowsSurviveMultiKeyInMutation is the regression pin for the
// 2+ distinct-key shape: SetPredicate stays an IN predicate, which arrow
// evaluates null-safely (null -> false, so NOT keeps the row) — it was
// accidentally safe pre-fix and must remain safe with the NotNull conjunct.
func TestCOWNullKeyRowsSurviveMultiKeyInMutation(t *testing.T) {
	ctx := t.Context()
	seedTbl, cat := newCOWTable(t, nullableKeyCOWSchema())

	seedTbl = appendNullKeyRows(t, ctx, seedTbl, []map[string]any{
		{"id": "1", "payload": "one"},
		{"id": "2", "payload": "two"},
		{"id": "3", "payload": "three"},
		{"id": nil, "payload": "ghost-1"},
		{"id": nil, "payload": "ghost-2"},
	})

	w := cowWriter(t, seedTbl, "id")
	w.committer = nullKeyCommitter(t, cat)

	// Two distinct keys: the single-column filter stays an IN.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 1, "payload": "ONE"}),
		cowMsg(t, "delete", map[string]any{"id": 3}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")

	keyed, nullKeyed := scanRowsAndNullKeyPayloads(t, ctx, final)
	assert.Equal(t, map[int64]string{1: "ONE", 2: "two"}, keyed)
	assert.Equal(t, []string{"ghost-1", "ghost-2"}, nullKeyed, "NULL-key rows must survive an IN-shaped rewrite")
}

// TestCOWNullKeyRowsSurviveDeleteOnlyBatch drives the delete-only path
// (txn.Delete, no rows to write) with a single distinct key — the EqualTo
// collapse again — over a file that also holds NULL-key bystanders.
func TestCOWNullKeyRowsSurviveDeleteOnlyBatch(t *testing.T) {
	ctx := t.Context()
	seedTbl, cat := newCOWTable(t, nullableKeyCOWSchema())

	seedTbl = appendNullKeyRows(t, ctx, seedTbl, []map[string]any{
		{"id": "1", "payload": "one"},
		{"id": "2", "payload": "two"},
		{"id": nil, "payload": "ghost-1"},
		{"id": nil, "payload": "ghost-2"},
	})

	w := cowWriter(t, seedTbl, "id")
	w.committer = nullKeyCommitter(t, cat)

	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "delete", map[string]any{"id": 1}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "delete-only copy-on-write must leave no delete files")
	assert.Equal(t, table.OpDelete, final.CurrentSnapshot().Summary.Operation)

	keyed, nullKeyed := scanRowsAndNullKeyPayloads(t, ctx, final)
	assert.Equal(t, map[int64]string{2: "two"}, keyed, "only the addressed key may be deleted")
	assert.Equal(t, []string{"ghost-1", "ghost-2"}, nullKeyed, "NULL-key rows must survive a delete-only rewrite")
}

// --- composite key -------------------------------------------------------------

// scanCompositeRows scans a (a int64, b string, payload string) table into a
// sorted list of "a|b|payload" strings, rendering NULL as "∅" so NULL-key rows
// are distinguishable from zero values.
func scanCompositeRows(t testing.TB, ctx context.Context, tbl *table.Table) []string {
	t.Helper()
	at, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer at.Release()

	var out []string
	tr := array.NewTableReader(at, 0)
	defer tr.Release()
	for tr.Next() {
		rec := tr.RecordBatch()
		aArr := rec.Column(rec.Schema().FieldIndices("a")[0]).(*array.Int64)
		bArr := rec.Column(rec.Schema().FieldIndices("b")[0]).(*array.String)
		payArr := rec.Column(rec.Schema().FieldIndices("payload")[0]).(*array.String)
		for r := 0; r < int(rec.NumRows()); r++ {
			a, b, pay := "∅", "∅", "∅"
			if aArr.IsValid(r) {
				a = fmt.Sprintf("%d", aArr.Value(r))
			}
			if bArr.IsValid(r) {
				b = bArr.Value(r)
			}
			if payArr.IsValid(r) {
				pay = payArr.Value(r)
			}
			out = append(out, a+"|"+b+"|"+pay)
		}
	}
	slices.Sort(out)
	return out
}

// TestCOWNullKeyRowsSurviveCompositeKeyMutation pins the composite-key shape:
// the filter is an OR of per-tuple ANDs of EqualTo, every conjunct
// null-propagating pre-fix, so bystander rows with NULL in one or both key
// columns were dropped from any rewritten file. They must all survive.
func TestCOWNullKeyRowsSurviveCompositeKeyMutation(t *testing.T) {
	ctx := t.Context()
	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64, Required: false},
		iceberg.NestedField{ID: 2, Name: "b", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "payload", Type: iceberg.PrimitiveTypes.String, Required: false},
	)
	seedTbl, cat := newCOWTable(t, sc)

	// Bystanders carry NULL in one or both key columns; all rows share a file.
	seedTbl = appendNullKeyRows(t, ctx, seedTbl, []map[string]any{
		{"a": "1", "b": "x", "payload": "p1"},
		{"a": "2", "b": "y", "payload": "p2"},
		{"a": "3", "b": "z", "payload": "p3"},
		{"a": nil, "b": "y", "payload": "ghost-a"},
		{"a": "1", "b": nil, "payload": "ghost-b"},
		{"a": nil, "b": nil, "payload": "ghost-ab"},
	})

	w := cowWriter(t, seedTbl, "a", "b")
	w.committer = nullKeyCommitter(t, cat)

	// Two keyed tuples: OR of ANDs, exercising the multi-clause composite path.
	require.NoError(t, w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"a": 1, "b": "x", "payload": "P1"}),
		cowMsg(t, "delete", map[string]any{"a": 3, "b": "z"}),
	}))

	final := cat.snapshot()
	assert.Zero(t, countDeleteManifestFiles(t, ctx, final), "copy-on-write must leave no delete files")
	assertAllManifestsData(t, ctx, final)

	want := []string{
		"1|x|P1",
		"1|∅|ghost-b",
		"2|y|p2",
		"∅|y|ghost-a",
		"∅|∅|ghost-ab",
	}
	slices.Sort(want)
	assert.Equal(t, want, scanCompositeRows(t, ctx, final), "rows with NULL in any key column must survive a composite-key rewrite")
}
