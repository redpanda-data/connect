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
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
)

// TestCOWPartitionTransformExtrasIntegration exercises copy-on-write upsert/
// delete on tables partitioned by transforms that Tier 1.3 reasoned equivalent
// to identity/bucket (same PartitionField.Transform.Apply code path through the
// partitioned fanout writer) but had not been driven end-to-end: a truncate
// transform and the temporal day and month transforms.
//
// Each sub-test seeds rows across several partitions, then applies one mutating
// batch (upsert an existing key in place, delete a key, upsert a brand-new key
// in another partition) and asserts:
//   - the correct final per-partition row set via DuckDB (deleted key gone,
//     upserted key updated exactly once with no duplicate, new key present), and
//   - ZERO delete files — the copy-on-write invariant that makes the table
//     readable by engine-backed catalogs — plus the batch committing as an
//     overwrite.
//
// The merge key is `id`, deliberately NOT the partition source column, which is
// only possible under copy-on-write.
func TestCOWPartitionTransformExtrasIntegration(t *testing.T) {
	integration.CheckSkip(t)

	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	// t15/t16/t17/t18 are distinct days within Jan 2024; m1/m2/m3 are distinct
	// months. Seeds pass timestamps as numeric microseconds (the append/shredder
	// path), mutations as time.Time (the copy-on-write rewrite path requires a
	// real time value for temporal columns).
	day := func(d int) time.Time { return time.Date(2024, 1, d, 12, 0, 0, 0, time.UTC) }
	month := func(m int) time.Time { return time.Date(2024, time.Month(m), 10, 12, 0, 0, 0, time.UTC) }

	newRouter := func(t *testing.T, ns, tbl string) *icebergimpl.Router {
		operation, err := service.NewInterpolatedString(`${! meta("op") }`)
		require.NoError(t, err)
		return infra.NewRouter(t, ns, tbl, WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))
	}

	// assertCOWClean asserts the committed table holds zero delete files and that
	// the mutating batch landed as an overwrite (a whole-file rewrite).
	assertCOWClean := func(t *testing.T, ns, tbl string) {
		client := infra.NewCatalogClient(t, ns)
		loaded, err := client.LoadTable(ctx, tbl)
		require.NoError(t, err)
		dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
		assert.Positive(t, dataManifests, "expected at least one data manifest to inspect")
		assert.Zero(t, deleteManifests, "copy-on-write must leave zero delete manifests")
		require.NotNil(t, loaded.CurrentSnapshot())
		assert.Equal(t, table.OpOverwrite, loaded.CurrentSnapshot().Summary.Operation,
			"the upsert+delete batch must commit as an overwrite under copy-on-write")
	}

	type idVal struct {
		ID    string `json:"id"`
		Value string `json:"value"`
	}

	t.Run("truncate", func(t *testing.T) {
		const ns, tbl = "cow_trunc_ns", "cow_trunc_test"
		infra.CreateNamespace(t, ns)

		client := infra.NewCatalogClient(t, ns)
		sc := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "code", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.StringType{}, Required: false},
		)
		// Partition by truncate(3, code): the first three characters bucket rows.
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "code_trunc", Transform: iceberg.TruncateTransform{Width: 3},
		})
		_, err := client.CreateTable(ctx, tbl, sc, catalog.WithPartitionSpec(&spec))
		require.NoError(t, err)

		router := newRouter(t, ns, tbl)

		// Seed: AAA partition has id=1,2; BBB has id=3; CCC has id=4.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "1", "code": "AAA111", "value": "one"}),
			opStructMsg("insert", map[string]any{"id": "2", "code": "AAA222", "value": "two"}),
			opStructMsg("insert", map[string]any{"id": "3", "code": "BBB111", "value": "three"}),
			opStructMsg("insert", map[string]any{"id": "4", "code": "CCC111", "value": "four"}),
		})

		// Mutate: upsert id=2 in place (AAA), delete id=3 (BBB), upsert new id=5 (AAA).
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("upsert", map[string]any{"id": "2", "code": "AAA222", "value": "two-updated"}),
			opStructMsg("delete", map[string]any{"id": "3", "code": "BBB111"}),
			opStructMsg("upsert", map[string]any{"id": "5", "code": "AAA333", "value": "five"}),
		})

		rows := querySQL[idVal](t, ctx, infra,
			fmt.Sprintf(`SELECT id, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
		require.Len(t, rows, 4, "id=3 deleted, id=2 not duplicated, id=5 added")
		assert.Equal(t, []idVal{
			{"1", "one"}, {"2", "two-updated"}, {"4", "four"}, {"5", "five"},
		}, rows)

		// Per-partition check: the AAA truncate partition now holds id=1,2,5.
		aaa := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."%s" WHERE code LIKE 'AAA%%';`, ns, tbl))
		require.Len(t, aaa, 1)
		assert.Equal(t, 3, aaa[0].Count, "the AAA truncate partition must contain id=1,2,5")

		assertCOWClean(t, ns, tbl)
	})

	// temporal runs the same upsert/delete shape against a table partitioned by a
	// temporal transform on a timestamptz column, parameterised by the transform
	// and the three timestamps (existing-key, deleted-key, new-key partitions).
	temporal := func(t *testing.T, ns, tbl string, transform iceberg.Transform, tsExisting, tsDeleted, tsNew time.Time) {
		infra.CreateNamespace(t, ns)

		client := infra.NewCatalogClient(t, ns)
		sc := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
			iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.TimestampTzType{}, Required: false},
			iceberg.NestedField{ID: 3, Name: "value", Type: iceberg.StringType{}, Required: false},
		)
		spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
			SourceIDs: []int{2}, FieldID: 1000, Name: "ts_part", Transform: transform,
		})
		_, err := client.CreateTable(ctx, tbl, sc, catalog.WithPartitionSpec(&spec))
		require.NoError(t, err)

		router := newRouter(t, ns, tbl)

		// Seed (numeric micros): id=1,2 in the existing-key partition, id=3 in the
		// deleted-key partition, id=4 in the existing-key partition.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("insert", map[string]any{"id": "1", "ts": tsExisting.UnixMicro(), "value": "one"}),
			opStructMsg("insert", map[string]any{"id": "2", "ts": tsExisting.UnixMicro(), "value": "two"}),
			opStructMsg("insert", map[string]any{"id": "3", "ts": tsDeleted.UnixMicro(), "value": "three"}),
			opStructMsg("insert", map[string]any{"id": "4", "ts": tsExisting.UnixMicro(), "value": "four"}),
		})

		// Mutate (time.Time, as the copy-on-write path requires for temporals):
		// upsert id=2 in place, delete id=3, upsert new id=5 into a new partition.
		produceMessages(t, ctx, router, service.MessageBatch{
			opStructMsg("upsert", map[string]any{"id": "2", "ts": tsExisting, "value": "two-updated"}),
			opStructMsg("delete", map[string]any{"id": "3"}),
			opStructMsg("upsert", map[string]any{"id": "5", "ts": tsNew, "value": "five"}),
		})

		rows := querySQL[idVal](t, ctx, infra,
			fmt.Sprintf(`SELECT id, value FROM iceberg_cat."%s"."%s" ORDER BY id;`, ns, tbl))
		require.Len(t, rows, 4, "id=3 deleted, id=2 not duplicated, id=5 added")
		assert.Equal(t, []idVal{
			{"1", "one"}, {"2", "two-updated"}, {"4", "four"}, {"5", "five"},
		}, rows)

		// Per-partition check: the existing-key partition holds id=1,2,4; the new
		// partition holds id=5; the deleted partition is empty.
		existing := querySQL[countResult](t, ctx, infra,
			fmt.Sprintf(`SELECT COUNT(*) as count FROM iceberg_cat."%s"."%s" WHERE value IN ('one','two-updated','four');`, ns, tbl))
		require.Len(t, existing, 1)
		assert.Equal(t, 3, existing[0].Count)

		assertCOWClean(t, ns, tbl)
	}

	t.Run("day", func(t *testing.T) {
		// id=1,2,4 on Jan 15; id=3 (deleted) on Jan 16; new id=5 on Jan 18.
		temporal(t, "cow_day_ns", "cow_day_test", iceberg.DayTransform{}, day(15), day(16), day(18))
	})

	t.Run("month", func(t *testing.T) {
		// id=1,2,4 in January; id=3 (deleted) in February; new id=5 in March.
		temporal(t, "cow_month_ns", "cow_month_test", iceberg.MonthTransform{}, month(1), month(2), month(3))
	})
}
