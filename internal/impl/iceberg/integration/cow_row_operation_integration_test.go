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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// countManifestsByContent loads the table's current snapshot and tallies its
// manifests by content kind. copy-on-write must leave only data manifests and
// zero delete manifests, which is what makes the result readable by
// engine-backed catalogs (Snowflake, Databricks Unity Catalog) that cannot
// apply Iceberg v2 delete files.
func countManifestsByContent(t *testing.T, ctx context.Context, tbl *table.Table) (dataManifests, deleteManifests int) {
	t.Helper()
	snap := tbl.CurrentSnapshot()
	require.NotNil(t, snap, "table must have a current snapshot")

	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(t, err)

	for _, m := range manifests {
		if m.ManifestContent() == iceberg.ManifestContentDeletes {
			deleteManifests++
		} else {
			dataManifests++
		}
	}
	return dataManifests, deleteManifests
}

// TestCOWRowOperationsIntegration drives an insert -> upsert -> delete round
// trip through the iceberg output configured with merge_strategy:
// copy-on-write, then asserts against a real Iceberg REST catalog that (a) the
// final table state is correct (id=3 deleted, id=2 updated, id=4 inserted, id=1
// untouched) via DuckDB, and (b) the table holds ONLY plain data files — zero
// delete files. The zero-delete-files property is the entire point of
// copy-on-write: a merge-on-read run of the same operations would leave
// equality-delete manifests, so this assertion is what distinguishes the two.
func TestCOWRowOperationsIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_row_ops_ns", "cow_row_ops_test"

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)

	router := infra.NewRouter(t, ns, tbl,
		WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}),
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			// The feature under test: rewrite whole data files instead of
			// writing Iceberg v2 delete files.
			MergeStrategy: icebergimpl.MergeStrategyCOW,
		}),
	)

	// Seed three rows. id is a string so the auto-created column is a valid
	// (non-floating-point) copy-on-write merge key.
	produceMessages(t, ctx, router, service.MessageBatch{
		opMsg(t, "insert", `{"id": "1", "value": "one"}`),
		opMsg(t, "insert", `{"id": "2", "value": "two"}`),
		opMsg(t, "insert", `{"id": "3", "value": "three"}`),
	})

	// One mutating batch: upsert id=2 (new value), delete id=3, and upsert id=4
	// (a brand-new row). This exercises the combined overwrite+delete path — the
	// interesting copy-on-write path that rewrites data files in a single atomic
	// snapshot.
	produceMessages(t, ctx, router, service.MessageBatch{
		opMsg(t, "upsert", `{"id": "2", "value": "two-updated"}`),
		opMsg(t, "delete", `{"id": "3"}`),
		opMsg(t, "upsert", `{"id": "4", "value": "four"}`),
	})

	// (a) Final state via DuckDB. Select the key column per the DuckDB Iceberg
	// projection quirk (a projection that omits the key can misread deletes).
	type row struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		`SELECT id, value FROM iceberg_cat."cow_row_ops_ns"."cow_row_ops_test" ORDER BY id;`)

	require.Len(t, rows, 3, "expected id=1, id=2, id=4 (id=3 deleted, id=2 not duplicated)")
	assert.Equal(t, "1", rows[0].ID)
	assert.Equal(t, "one", rows[0].Value, "id=1 must be untouched")
	assert.Equal(t, "2", rows[1].ID)
	assert.Equal(t, "two-updated", rows[1].Value, "upsert must replace the prior value for id=2")
	assert.Equal(t, "4", rows[2].ID)
	assert.Equal(t, "four", rows[2].Value, "upsert of a new key must insert id=4")

	// (b) Zero delete files. Load the committed table through the REST catalog
	// and inspect its snapshot manifests directly. The assertion is non-vacuous:
	// we require at least one data manifest (proving we actually read real
	// manifest content off MinIO) AND exactly zero delete manifests. A
	// merge-on-read run of the identical upsert/delete batch would have produced
	// delete-content manifests here.
	client := infra.NewCatalogClient(t, ns)
	loaded, err := client.LoadTable(ctx, tbl)
	require.NoError(t, err)

	dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
	assert.Positive(t, dataManifests, "expected at least one data manifest to inspect")
	assert.Zero(t, deleteManifests, "copy-on-write must leave zero delete manifests")

	// Belt and braces: the mutating batch must have landed as an overwrite (whole
	// data-file rewrite), not as a delete-file append. This proves the mutation
	// was materialised the copy-on-write way rather than the table merely
	// happening to have no delete files.
	require.NotNil(t, loaded.CurrentSnapshot())
	assert.Equal(t, table.OpOverwrite, loaded.CurrentSnapshot().Summary.Operation,
		"the upsert+delete batch must commit as an overwrite under copy-on-write")
}
