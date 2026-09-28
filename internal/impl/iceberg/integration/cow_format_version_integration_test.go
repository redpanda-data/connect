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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWFormatVersion1Integration creates a format-version-1 table through the
// REST catalog and runs a copy-on-write upsert+delete against it, asserting the
// table is NOT force-upgraded to v2.
//
// This is the whole point of copy-on-write for legacy tables: it only ever
// writes plain data files (no Iceberg v2 delete files), so it can operate on a
// v1 table and must leave it at v1. The merge-on-read path, by contrast,
// requires equality-delete files and so irreversibly upgrades v1 -> v2. The
// production router already wires this: it sets CommitConfig.SkipFormatUpgrade
// whenever merge_strategy is copy-on-write (router.go), and the test harness
// exercises that same production NewRouter, so no test-only wiring is needed
// here — this test guards that behaviour end-to-end.
//
// Asserted: the loaded table is still v1, DuckDB reads the correct final state,
// and there are zero delete files.
func TestCOWFormatVersion1Integration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_v1_ns", "cow_v1_test"
	infra.CreateNamespace(t, ns)

	// Create an explicit format-version-1 table via the REST catalog.
	client := infra.NewCatalogClient(t, ns)
	created, err := client.CreateTable(ctx, tbl,
		iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "value", Type: iceberg.StringType{}, Required: false},
		),
		catalog.WithProperties(iceberg.Properties{table.PropertyFormatVersion: "1"}),
	)
	require.NoError(t, err)
	require.Equal(t, 1, created.Metadata().Version(), "table must be created at format version 1")

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Seed three rows (append fast path).
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "value": "one"}),
		opStructMsg("insert", map[string]any{"id": "2", "value": "two"}),
		opStructMsg("insert", map[string]any{"id": "3", "value": "three"}),
	})

	// One mutating batch: upsert id=2, delete id=3 — a copy-on-write overwrite.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "2", "value": "two-updated"}),
		opStructMsg("delete", map[string]any{"id": "3"}),
	})

	// The table must STILL be v1 — copy-on-write must not trigger the v1->v2
	// upgrade the merge-on-read path needs.
	loaded, err := client.LoadTable(ctx, tbl)
	require.NoError(t, err)
	assert.Equal(t, 1, loaded.Metadata().Version(),
		"copy-on-write must leave the table at format version 1 (no forced v2 upgrade)")

	// Correct final state via DuckDB.
	type row struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
	require.Len(t, rows, 2, "id=3 deleted, id=2 not duplicated")
	assert.Equal(t, row{"1", "one"}, rows[0])
	assert.Equal(t, row{"2", "two-updated"}, rows[1])

	// Copy-on-write invariant: zero delete files, overwrite op.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
}
