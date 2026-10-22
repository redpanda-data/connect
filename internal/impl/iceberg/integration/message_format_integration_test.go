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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

const arrayTimestampLayout = "2006-01-02 15:04:05.999999999"

// arrayMsg builds a message whose structured payload is a positional array.
func arrayMsg(op string, values ...any) *service.Message {
	m := service.NewMessage(nil)
	m.SetStructured(values)
	if op != "" {
		m.MetaSetMut("op", op)
	}
	return m
}

// TestArrayMessageFormatIntegration proves, end-to-end against a real REST
// catalog + S3 + DuckDB, that message_format: array writes all-string
// positional arrays into a pre-created table, parsing numeric and timestamp
// strings into the column types, and that upserts keyed on a parsed numeric
// string match the existing row under both merge strategies.
func TestArrayMessageFormatIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()
	ctx := context.Background()
	infra := setupTestInfra(t)

	const ns = "array_format_ns"
	infra.CreateNamespace(t, ns)
	client := infra.NewCatalogClient(t, ns)

	createTable := func(t *testing.T, name string, opts ...catalog.CreateTableOpt) {
		t.Helper()
		_, err := client.CreateTable(ctx, name, iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "symbol", Type: iceberg.StringType{}, Required: false},
			iceberg.NestedField{ID: 3, Name: "price", Type: iceberg.DecimalTypeOf(18, 6), Required: false},
			iceberg.NestedField{ID: 4, Name: "ts", Type: iceberg.TimestampType{}, Required: false},
			iceberg.NestedField{ID: 5, Name: "qty", Type: iceberg.Int32Type{}, Required: false},
		), opts...)
		require.NoError(t, err)
	}

	type row struct {
		ID     int64  `json:"id"`
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
		TS     string `json:"ts"`
		Qty    int    `json:"qty"`
	}
	readRows := func(t *testing.T, name string) []row {
		t.Helper()
		return querySQL[row](t, ctx, infra, fmt.Sprintf(
			`SELECT id, symbol, CAST(price AS VARCHAR) AS price, CAST(ts AS VARCHAR) AS ts, qty `+
				`FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, name))
	}

	strategies := []struct {
		name string
		cow  bool
	}{
		{"merge-on-read", false},
		{"copy-on-write", true},
	}
	for _, tc := range strategies {
		t.Run(tc.name, func(t *testing.T) {
			tblName := "array_" + tc.name
			createTable(t, tblName)

			router := infra.NewRouter(t, ns, tblName,
				WithMessageFormat(icebergimpl.MessageFormatArray),
				WithTimestampLayout(arrayTimestampLayout))
			produceMessages(t, ctx, router, service.MessageBatch{
				arrayMsg("", "1", "IBM", "123.456789", "2025-05-29 11:54:26.721451319", "100"),
				arrayMsg("", "2", "MSFT", "10.5", "2025-05-29 11:54:27.000000000", "200"),
				arrayMsg("", "3", "AAPL", "7", "2025-05-29 11:54:28.5", "300"),
			})

			rows := readRows(t, tblName)
			require.Len(t, rows, 3)
			assert.Equal(t, row{1, "IBM", "123.456789", "2025-05-29 11:54:26.721451", 100}, rows[0])
			assert.Equal(t, int64(2), rows[1].ID)
			assert.Equal(t, "MSFT", rows[1].Symbol)
			assert.Equal(t, "10.500000", rows[1].Price)
			assert.Equal(t, 200, rows[1].Qty)
			assert.Equal(t, int64(3), rows[2].ID)
			assert.Equal(t, "7.000000", rows[2].Price)
			assert.Equal(t, 300, rows[2].Qty)

			// Upsert keyed on id: "1" must parse to the same long key as the
			// stored row, so the old row is replaced rather than duplicated.
			operation, err := service.NewInterpolatedString(`${! meta("op") }`)
			require.NoError(t, err)
			rowOp := icebergimpl.RowOpConfig{
				Operation:        operation,
				IdentifierFields: []string{"id"},
			}
			if tc.cow {
				rowOp.MergeStrategy = icebergimpl.MergeStrategyCOW
			}
			upsertRouter := infra.NewRouter(t, ns, tblName,
				WithMessageFormat(icebergimpl.MessageFormatArray),
				WithTimestampLayout(arrayTimestampLayout),
				WithRowOperation(rowOp))
			produceMessages(t, ctx, upsertRouter, service.MessageBatch{
				arrayMsg("upsert", "1", "IBM", "200.5", "2025-05-29 12:00:00.000000001", "5"),
			})

			rows = readRows(t, tblName)
			require.Len(t, rows, 3, "upsert must replace id 1, not add a row")
			assert.Equal(t, int64(1), rows[0].ID)
			assert.Equal(t, "200.500000", rows[0].Price)
			assert.Equal(t, 5, rows[0].Qty)
		})
	}

	// Partition values are computed from the row arrayToRow builds, so an
	// identity(symbol) table must route each array to its symbol's partition.
	t.Run("partitioned-insert", func(t *testing.T) {
		const tblName = "array_partitioned"
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "symbol", Transform: iceberg.IdentityTransform{},
		})
		createTable(t, tblName, catalog.WithPartitionSpec(&spec))

		router := infra.NewRouter(t, ns, tblName,
			WithMessageFormat(icebergimpl.MessageFormatArray),
			WithTimestampLayout(arrayTimestampLayout))
		produceMessages(t, ctx, router, service.MessageBatch{
			arrayMsg("", "1", "IBM", "1.5", "2025-05-29 11:54:26", "10"),
			arrayMsg("", "2", "MSFT", "2.5", "2025-05-29 11:54:27", "20"),
			arrayMsg("", "3", "IBM", "3.5", "2025-05-29 11:54:28", "30"),
		})

		assert.Equal(t, []row{
			{1, "IBM", "1.500000", "2025-05-29 11:54:26", 10},
			{2, "MSFT", "2.500000", "2025-05-29 11:54:27", 20},
			{3, "IBM", "3.500000", "2025-05-29 11:54:28", 30},
		}, readRows(t, tblName))

		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		fsys, err := tbl.FS(ctx)
		require.NoError(t, err)
		manifests, err := tbl.CurrentSnapshot().Manifests(fsys)
		require.NoError(t, err)
		filesPerSymbol := map[any]int{}
		for _, m := range manifests {
			for entry, err := range m.Entries(fsys, true) {
				require.NoError(t, err)
				filesPerSymbol[entry.DataFile().Partition()[1000]]++
			}
		}
		assert.Equal(t, map[any]int{"IBM": 1, "MSFT": 1}, filesPerSymbol)
	})

	t.Run("missing-table", func(t *testing.T) {
		router := infra.NewRouter(t, ns, "array_no_such_table",
			WithMessageFormat(icebergimpl.MessageFormatArray))
		err := router.Route(ctx, service.MessageBatch{arrayMsg("", "1", "IBM", "1", "2025-05-29 11:54:26", "1")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "message_format is array, so the table must be created before writing")
	})
}
