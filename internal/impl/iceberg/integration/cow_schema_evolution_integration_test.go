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

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWSchemaEvolutionIntegration drives schema evolution through the
// copy-on-write mutation path: batch 1 seeds {id,value}; batch 2 upserts an
// existing key carrying an extra new column. The copy-on-write path detects the
// unknown column (cowDetectNewColumns), returns a schema-evolution error, and
// the router evolves the table and retries the overwrite. This proves the
// evolve-and-retry loop works for the whole-file-rewrite path, not only the
// append path.
//
// Asserted via DuckDB (an independent reader):
//   - the table evolved (the new column is present and selectable),
//   - the upserted row carries the new value,
//   - the prior row is intact (its new column reads back null),
//   - zero delete files and the mutation committed as an overwrite.
func TestCOWSchemaEvolutionIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_evo_ns", "cow_evo_test"
	infra.CreateNamespace(t, ns)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{Enabled: true}),
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Batch 1: auto-create the table with {id, value} and seed two rows. id is a
	// string so the auto-created column is a valid copy-on-write merge key.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "value": "one"}),
		opStructMsg("insert", map[string]any{"id": "2", "value": "two"}),
	})

	// Confirm the seeded schema has exactly {id, value} before evolution.
	cols := querySQL[ColumnInfo](t, ctx, infra,
		fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))
	require.Len(t, cols, 2, "table should start with {id, value}")

	// Batch 2: upsert id=2 carrying a brand-new column `extra`. Under
	// copy-on-write this is an overwrite (delete old id=2 + append the rewritten
	// row); the new column forces a schema-evolution round trip first.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "2", "value": "two-v2", "extra": "hello"}),
	})

	// The table must have evolved: `extra` is now present.
	cols = querySQL[ColumnInfo](t, ctx, infra,
		fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tbl))
	colNames := make(map[string]string, len(cols))
	for _, c := range cols {
		colNames[c.ColumnName] = c.ColumnType
	}
	require.Contains(t, colNames, "extra", "table must have evolved to add the `extra` column")

	// Final state via DuckDB, selecting the evolved column too.
	type row struct {
		ID    string  `json:"id"`
		Value string  `json:"value"`
		Extra *string `json:"extra"`
	}
	rows := querySQL[row](t, ctx, infra,
		fmt.Sprintf(`SELECT id, value, extra FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
	require.Len(t, rows, 2, "id=2 must be upserted in place, not duplicated")

	assert.Equal(t, "1", rows[0].ID)
	assert.Equal(t, "one", rows[0].Value, "prior row must be intact")
	assert.Nil(t, rows[0].Extra, "the pre-evolution row reads back null for the new column")

	assert.Equal(t, "2", rows[1].ID)
	assert.Equal(t, "two-v2", rows[1].Value, "upsert must replace id=2's value")
	require.NotNil(t, rows[1].Extra)
	assert.Equal(t, "hello", *rows[1].Extra, "the evolved column value must read back via DuckDB")

	// Copy-on-write invariant.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
}
