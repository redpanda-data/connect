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
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWTemporalDataColumnIntegration proves the no-tz `timestamp` on-disk-format
// fix end-to-end for temporal DATA columns (as opposed to merge keys, which
// TestCOWRowOperationKeyTypesIntegration/timestamp covers). A single table carries
// BOTH a no-timezone `timestamp` column and a `timestamptz` column so DuckDB — an
// independent reader — can confirm the two are stored and typed distinctly.
//
// The flow: append two rows, then a single copy-on-write batch upserts id=1 (new
// temporal values in both columns) and deletes id=2. Before the fix, the append
// path wrote the no-tz `timestamp` column with the parquet annotation
// isAdjustedToUTC=true, so iceberg-go read the existing data file back as
// `timestamptz` and the copy-on-write file rewrite failed with "cannot promote
// timestamptz to timestamp". With the fix the no-tz column is written with
// isAdjustedToUTC=false and the rewrite succeeds.
//
// The assertions:
//   - exactly one surviving row (id=2 deleted, id=1 not duplicated);
//   - DuckDB matches the surviving row using a no-tz TIMESTAMP literal for the
//     `ts` column and a TIMESTAMPTZ literal for the `tstz` column — i.e. the
//     instant round-trips through the copy-on-write rewrite;
//   - DuckDB reports the two columns with distinct SQL types (TIMESTAMP vs
//     TIMESTAMP WITH TIME ZONE), proving the annotation is honoured on read.
func TestCOWTemporalDataColumnIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_temporal_datacol_ns", "cow_temporal_datacol_test"
	infra.CreateNamespace(t, ns)

	client := infra.NewCatalogClient(t, ns)
	_, err := client.CreateTable(ctx, tbl, iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		// A no-timezone `timestamp` data column: this is the column whose append
		// encoding used to break copy-on-write.
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.TimestampType{}, Required: false},
		// A `timestamptz` column in the SAME table, so DuckDB can show the two are
		// typed distinctly and the regression case stays covered.
		iceberg.NestedField{ID: 3, Name: "tstz", Type: iceberg.TimestampTzType{}, Required: false},
	))
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	tsSeed := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)
	tsUpsert := time.Date(2024, 3, 20, 8, 15, 0, 0, time.UTC)

	// Seed id=1, id=2 via the append fast path (no keyed ops in this batch).
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "1", "ts": tsSeed, "tstz": tsSeed}),
		opStructMsg("insert", map[string]any{"id": "2", "ts": tsSeed, "tstz": tsSeed}),
	})

	// One mutating batch: upsert id=1 with new temporal values and delete id=2.
	// This lands as a single copy-on-write overwrite that must rewrite the
	// surviving rows of the existing data file — the step that previously failed
	// for the no-tz `timestamp` column.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "1", "ts": tsUpsert, "tstz": tsUpsert}),
		opStructMsg("delete", map[string]any{"id": "2"}),
	})

	// (a) Exactly one surviving row (id=2 deleted, id=1 not duplicated).
	type idRow struct {
		ID string `json:"id"`
	}
	rows := querySQL[idRow](t, ctx, infra,
		fmt.Sprintf(`SELECT id FROM iceberg_cat."%s"."%s";`, ns, tbl))
	require.Lenf(t, rows, 1, "expected exactly one surviving row; got %d", len(rows))
	assert.Equal(t, "1", rows[0].ID, "surviving row must be the upserted id=1")

	// (b) The surviving row's temporal values round-tripped through the rewrite,
	// per DuckDB's own typed comparison: a no-tz TIMESTAMP literal for `ts` and a
	// TIMESTAMPTZ literal for `tstz`.
	match := querySQL[countResult](t, ctx, infra, fmt.Sprintf(
		`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s" `+
			`WHERE ts = TIMESTAMP '2024-03-20 08:15:00' AND tstz = TIMESTAMPTZ '2024-03-20 08:15:00+00';`,
		ns, tbl))
	require.Len(t, match, 1)
	assert.Equal(t, 1, match[0].Count,
		"DuckDB must find the surviving row at the exact upserted instant in both temporal columns")

	// (c) DuckDB reports the two columns with distinct SQL types: the no-tz column
	// as TIMESTAMP and the tz column as TIMESTAMP WITH TIME ZONE. This is the
	// direct proof that the isAdjustedToUTC annotation is honoured on read.
	type typeRow struct {
		TSType   string `json:"ts_type"`
		TSTZType string `json:"tstz_type"`
	}
	types := querySQL[typeRow](t, ctx, infra, fmt.Sprintf(
		`SELECT typeof(ts) AS ts_type, typeof(tstz) AS tstz_type FROM iceberg_cat."%s"."%s";`, ns, tbl))
	require.Len(t, types, 1)
	assert.Equal(t, "TIMESTAMP", types[0].TSType, "no-tz column must read back as a no-timezone TIMESTAMP")
	assert.Equal(t, "TIMESTAMP WITH TIME ZONE", types[0].TSTZType, "tz column must read back as TIMESTAMP WITH TIME ZONE")

	// (d) Copy-on-write invariant: zero delete manifests + overwrite op.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
}
