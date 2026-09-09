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
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWNestedSchemaIntegration pre-creates a table with a nested struct (that
// itself contains a nested int64 field), a list, and a map<string,struct>, seeds
// a row, then copy-on-write-upserts the same key with entirely new nested values
// and reads the result back through DuckDB — which unnests struct/list/map
// independently of iceberg-go.
//
// This closes the second-highest-risk gap: the recursive cowMassage projection
// is what re-encodes nested struct/list/map values into the JSON shape
// array.RecordFromJSON expects at every depth. All unit coverage of cowMassage
// reads back through iceberg-go's own Arrow scan (writer and reader are the same
// library), so a self-consistent-but-wrong nested encoding — most dangerously
// the historical silent truncation of an integer nested beyond 2^53, which
// cowMassage fixes by emitting integers as JSON strings at every leaf — would
// pass. DuckDB parses the parquet itself, so a wrong nested encoding surfaces
// here as a wrong nested value or a truncated nested int.
//
// Both the struct's `big` field and the map value struct's `score` field carry
// values > 2^53 to prove the nested-int fix end-to-end through an independent
// reader.
func TestCOWNestedSchemaIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tbl = "cow_nested_ns", "cow_nested_test"
	infra.CreateNamespace(t, ns)

	// id (merge key), a nested struct, a list, and a map<string,struct>.
	client := infra.NewCatalogClient(t, ns)
	sc := iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "info", Required: false, Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 10, Name: "name", Type: iceberg.StringType{}, Required: false},
				{ID: 11, Name: "big", Type: iceberg.Int64Type{}, Required: false},
			},
		}},
		iceberg.NestedField{ID: 3, Name: "tags", Required: false, Type: &iceberg.ListType{
			ElementID: 20, Element: iceberg.StringType{}, ElementRequired: false,
		}},
		iceberg.NestedField{ID: 4, Name: "attrs", Required: false, Type: &iceberg.MapType{
			KeyID: 30, KeyType: iceberg.StringType{},
			ValueID: 31, ValueRequired: false,
			ValueType: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 40, Name: "score", Type: iceberg.Int64Type{}, Required: false},
			}},
		}},
	)
	_, err := client.CreateTable(ctx, tbl, sc)
	require.NoError(t, err)

	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	router := infra.NewRouter(t, ns, tbl,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))

	// Seed id=1 (append fast path).
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("insert", map[string]any{
			"id":   "1",
			"info": map[string]any{"name": "alice", "big": int64(9007199254740993)},
			"tags": []any{"a", "b"},
			"attrs": map[string]any{
				"x": map[string]any{"score": int64(100)},
			},
		}),
	})

	// Copy-on-write upsert of id=1 changing every nested value. The new nested
	// ints are > 2^53 to prove the nested-int encoding survives an independent
	// reader.
	produceMessages(t, ctx, router, service.MessageBatch{
		opStructMsg("upsert", map[string]any{
			"id":   "1",
			"info": map[string]any{"name": "alice2", "big": int64(9007199254740995)},
			"tags": []any{"c", "d", "e"},
			"attrs": map[string]any{
				"y": map[string]any{"score": int64(9007199254740997)},
			},
		}),
	})

	// Read the nested values back through DuckDB, which unnests the struct/list/
	// map itself. Selecting the leaf columns flattens the projection so it parses
	// into a Go struct.
	type nestedRow struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Big    int64  `json:"big"`
		NTags  int    `json:"ntags"`
		Tag0   string `json:"tag0"`
		AKey   string `json:"akey"`
		AScore int64  `json:"ascore"`
	}
	rows := querySQL[nestedRow](t, ctx, infra, fmt.Sprintf(`
		SELECT
			id,
			info.name AS name,
			info.big AS big,
			len(tags) AS ntags,
			tags[1] AS tag0,
			map_keys(attrs)[1] AS akey,
			(map_values(attrs)[1]).score AS ascore
		FROM iceberg_cat."%s"."%s";`, ns, tbl))

	require.Len(t, rows, 1, "the upsert must replace id=1, not duplicate it")
	got := rows[0]
	assert.Equal(t, "1", got.ID)
	assert.Equal(t, "alice2", got.Name, "nested struct string must be the upserted value")
	assert.Equal(t, int64(9007199254740995), got.Big,
		"nested int64 > 2^53 must survive the copy-on-write rewrite without truncation")
	assert.Equal(t, 3, got.NTags, "list must hold the upserted three elements")
	assert.Equal(t, "c", got.Tag0, "first list element must be the upserted value")
	assert.Equal(t, "y", got.AKey, "map key must be the upserted key")
	assert.Equal(t, int64(9007199254740997), got.AScore,
		"nested int64 > 2^53 inside a map<string,struct> value must survive without truncation")

	// Copy-on-write invariant.
	assertCOWSnapshot(t, ctx, infra, ns, tbl, table.OpOverwrite)
}
