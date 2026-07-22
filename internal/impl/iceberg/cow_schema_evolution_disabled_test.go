// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestCOWUpsertUnknownColumnSchemaEvolutionDisabled is the copy-on-write
// analogue of the merge-on-read "schema evolution disabled" case in
// integration/schema_evolution_test.go (SchemaEvolutionDisabled_FailsOnMissingTable):
// with schema evolution off, an inbound record carrying something the table
// schema does not have must fail loudly and leave the table byte-for-byte
// unchanged rather than silently dropping data or half-writing an overwrite.
//
// This is driven at the writer + in-memory-catalog seam rather than through the
// Router: the Router only ever talks to a live catalog via
// catalogx.NewCatalogClient (there is no in-memory catalog seam on the Router),
// and schema evolution is purely a Router concern — the writer's job is to
// surface a *BatchSchemaEvolutionError, and the Router either recovers from it
// (schemaEvoCfg.Enabled) or propagates it untouched (disabled). So a writer that
// returns the evolution error *before committing anything* is exactly what the
// disabled Router surfaces to the pipeline. We wire a real committer against a
// seeded table so we can additionally prove the "leaves the table unchanged"
// half of the guarantee: no schema change, no new snapshot, no orphaned
// overwrite parquet files.
func TestCOWUpsertUnknownColumnSchemaEvolutionDisabled(t *testing.T) {
	ctx := t.Context()
	logger := service.MockResources().Logger()

	sc := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "payload", Type: iceberg.PrimitiveTypes.String},
	)
	seedTbl, cat := newCOWTable(t, sc)
	seedTbl = appendCOWRows(t, ctx, seedTbl, map[int64]string{1: "one", 2: "two", 3: "three"})

	// Capture the pre-write state to compare against afterward.
	seedRows := scanRows(t, ctx, cat.snapshot())
	require.Equal(t, map[int64]string{1: "one", 2: "two", 3: "three"}, seedRows,
		"precondition: seed rows are present")
	seedSnapshot := cat.snapshot().CurrentSnapshot()
	require.NotNil(t, seedSnapshot, "precondition: seeding produced a snapshot")
	seedParquet := countParquetFiles(t, seedTbl.Location())
	require.Positive(t, seedParquet, "precondition: seeding wrote data files")

	// A real committer over the in-memory catalog — so if the write erroneously
	// reached the overwrite commit, it would land a new snapshot we could detect.
	comm, err := NewCommitter(cat.snapshot(), CommitConfig{MaxRetries: 3, SkipFormatUpgrade: true}, reloadFn(cat), logger)
	require.NoError(t, err)
	defer comm.Close()
	w := cowWriter(t, cat.snapshot(), "id")
	w.committer = comm

	// A copy-on-write upsert carrying the column "extra", which the table schema
	// does not contain. With schema evolution disabled this must not be admitted.
	err = w.Write(ctx, service.MessageBatch{
		cowMsg(t, "upsert", map[string]any{"id": 2, "payload": "TWO", "extra": "surprise"}),
	})

	// 1. Fails loudly, surfacing the unknown column and the need to evolve.
	require.Error(t, err, "an unknown column with evolution disabled must fail, not silently drop the column")
	var evo *BatchSchemaEvolutionError
	require.ErrorAs(t, err, &evo, "the failure must be a schema-evolution error naming the unknown column")
	assert.Contains(t, err.Error(), "extra", "the error must name the offending column")

	// 2. Table schema unchanged — the unknown column was not silently added.
	after := cat.snapshot()
	assert.Len(t, after.Schema().Fields(), 2, "schema must not have gained a column")
	_, hasExtra := after.Schema().FindFieldByName("extra")
	assert.False(t, hasExtra, "the unknown column must not have been added to the schema")

	// 3. No new snapshot — no partial write and no orphaned overwrite snapshot.
	nowSnapshot := after.CurrentSnapshot()
	require.NotNil(t, nowSnapshot)
	assert.Equal(t, seedSnapshot.SnapshotID, nowSnapshot.SnapshotID,
		"the failed write must not have committed a new snapshot")

	// 4. Row data unchanged — id=2 still holds its seeded value, nothing dropped.
	assert.Equal(t, seedRows, scanRows(t, ctx, after),
		"table rows must be exactly as seeded (in particular id=2 keeps payload \"two\")")

	// 5. No orphaned overwrite parquet files left on disk.
	assert.Equal(t, seedParquet, countParquetFiles(t, seedTbl.Location()),
		"a failed copy-on-write write must not leave orphaned parquet files")

	// Sanity: the operation record snapshot's op is still whatever seeding left,
	// never an overwrite from this failed write.
	assert.NotEqual(t, table.OpOverwrite, nowSnapshot.Summary.Operation,
		"the current snapshot must not be an overwrite produced by the failed write")
}
