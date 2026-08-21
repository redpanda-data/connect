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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// tsAnnotationsPerDataFile opens every current-snapshot data file's parquet
// footer via the table's own filesystem and returns, per file path, fieldID ->
// isAdjustedToUTC for each leaf annotated with a TIMESTAMP logical type.
func tsAnnotationsPerDataFile(t *testing.T, ctx context.Context, tbl *table.Table) map[string]map[int]bool {
	t.Helper()
	out := map[string]map[int]bool{}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return out
	}
	fsys, err := tbl.FS(ctx)
	require.NoError(t, err)
	manifests, err := snap.Manifests(fsys)
	require.NoError(t, err)
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range m.Entries(fsys, true) {
			require.NoError(t, err)
			path := entry.DataFile().FilePath()
			f, err := fsys.Open(path)
			require.NoError(t, err)
			info, err := f.Stat()
			require.NoError(t, err)
			pf, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
			require.NoError(t, err)
			ann := map[int]bool{}
			for _, el := range pf.Metadata().Schema {
				if lt, ok := el.LogicalType.Get(); ok && lt.Timestamp != nil {
					ann[int(el.FieldID)] = lt.Timestamp.IsAdjustedToUTC
				}
			}
			require.NoError(t, f.Close())
			out[path] = ann
		}
	}
	return out
}

// removeTableProperty removes a table property through the raw REST commit
// endpoint (iceberg-go's Transaction has no remove-properties surface). Used
// to turn a table stamped by this test suite back into a faithful simulation
// of a pre-upgrade table: legacy-annotated data files, no pinning property.
func removeTableProperty(t *testing.T, infra *testInfrastructure, ns, tblName, prop string) {
	t.Helper()
	body := fmt.Sprintf(`{"requirements":[],"updates":[{"action":"remove-properties","removals":[%q]}]}`, prop)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", infra.RestURL, ns, tblName), strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "remove-properties commit failed")
}

// TestTimestampEncodingLegacyTableIntegration proves, end-to-end against a real
// REST catalog + S3 + DuckDB, that an EXISTING table whose data files carry the
// legacy UTC-adjusted `timestamp` annotation is (a) probe-detected and pinned
// `legacy` when the property is absent, (b) kept uniformly legacy-annotated by
// every subsequent append (never mixed), (c) still correct to an independent
// reader, and (d) protected from mutating copy-on-write by the upfront guard
// error instead of iceberg-go's cryptic mid-commit failure.
func TestTimestampEncodingLegacyTableIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tblName = "ts_enc_legacy_ns", "ts_enc_legacy_test"
	infra.CreateNamespace(t, ns)
	client := infra.NewCatalogClient(t, ns)

	// A pre-existing table (not created by the connector): id + a no-timezone
	// `timestamp` column + a `timestamptz` column, no pinning property.
	_, err := client.CreateTable(ctx, tblName, iceberg.NewSchema(1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.StringType{}, Required: true},
		iceberg.NestedField{ID: 2, Name: "ts", Type: iceberg.TimestampType{}, Required: false},
		iceberg.NestedField{ID: 3, Name: "tstz", Type: iceberg.TimestampTzType{}, Required: false},
	))
	require.NoError(t, err)

	seed := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)
	rowMsg := func(id string) *service.Message {
		m := service.NewMessage(nil)
		m.SetStructured(map[string]any{"id": id, "ts": seed, "tstz": seed})
		return m
	}

	// --- Produce genuine legacy files via the connector's own legacy mode. ---
	// Pin the table `legacy`, append through the router (the property-present
	// resolution path), and confirm the produced file is annotated exactly as
	// pre-fix releases wrote it: isAdjustedToUTC=true on BOTH columns.
	{
		tbl, err := client.LoadTable(ctx, tblName)
		require.NoError(t, err)
		txn := tbl.NewTransaction()
		require.NoError(t, txn.SetProperties(iceberg.Properties{
			icebergx.TimestampEncodingProperty: "legacy",
		}))
		_, err = txn.Commit(ctx)
		require.NoError(t, err)
	}
	routerA := infra.NewRouter(t, ns, tblName)
	produceMessages(t, ctx, routerA, service.MessageBatch{rowMsg("1")})

	tbl, err := client.LoadTable(ctx, tblName)
	require.NoError(t, err)
	for path, ann := range tsAnnotationsPerDataFile(t, ctx, tbl) {
		assert.Equal(t, map[int]bool{2: true, 3: true}, ann,
			"legacy-pinned append must write the pre-fix annotation: %s", path)
	}

	// --- Simulate the real upgrade scenario. ---
	// Strip the property so the table looks exactly like one written entirely
	// by a pre-upgrade connector: legacy files, no pin. A FRESH router (new
	// process) must footer-probe the file, resolve `legacy`, stamp the
	// property, and keep appending the legacy annotation — uniform, not mixed.
	removeTableProperty(t, infra, ns, tblName, icebergx.TimestampEncodingProperty)
	tbl, err = client.LoadTable(ctx, tblName)
	require.NoError(t, err)
	require.NotContains(t, tbl.Properties(), icebergx.TimestampEncodingProperty,
		"precondition: the pinning property must be absent before the bootstrap")

	routerB := infra.NewRouter(t, ns, tblName)
	produceMessages(t, ctx, routerB, service.MessageBatch{rowMsg("2")})

	tbl, err = client.LoadTable(ctx, tblName)
	require.NoError(t, err)
	assert.Equal(t, "legacy", tbl.Properties()[icebergx.TimestampEncodingProperty],
		"the footer-probe bootstrap must stamp the resolved encoding onto the table")

	// --- An insert-only copy-on-write batch is fine on a legacy table. ---
	operation, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	cowRouter := infra.NewRouter(t, ns, tblName,
		WithRowOperation(icebergimpl.RowOpConfig{
			Operation:        operation,
			IdentifierFields: []string{"id"},
			MergeStrategy:    icebergimpl.MergeStrategyCOW,
		}))
	produceMessages(t, ctx, cowRouter, service.MessageBatch{
		opStructMsg("insert", map[string]any{"id": "3", "ts": seed, "tstz": seed}),
	})

	// --- Uniformity: EVERY data file (pre-existing, probed append, copy-on-
	// write insert) carries the legacy annotation. ---
	tbl, err = client.LoadTable(ctx, tblName)
	require.NoError(t, err)
	anns := tsAnnotationsPerDataFile(t, ctx, tbl)
	require.Len(t, anns, 3, "expected one data file per appended batch")
	for path, ann := range anns {
		assert.Equal(t, map[int]bool{2: true, 3: true}, ann,
			"a legacy table's files must stay uniformly legacy-annotated: %s", path)
	}

	// --- DuckDB (an independent reader) sees all rows at the correct instant.
	// Under the legacy annotation both columns read as UTC-adjusted timestamps,
	// exactly as they did from pre-fix releases. ---
	match := querySQL[countResult](t, ctx, infra, fmt.Sprintf(
		`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s" `+
			`WHERE ts = TIMESTAMPTZ '2024-01-15 12:30:45+00' AND tstz = TIMESTAMPTZ '2024-01-15 12:30:45+00';`,
		ns, tblName))
	require.Len(t, match, 1)
	assert.Equal(t, 3, match[0].Count, "every row must be stored at the exact seeded instant")

	// --- Mutating copy-on-write must fail upfront with the actionable guard
	// error, not iceberg-go's "cannot promote timestamptz to timestamp". ---
	err = cowRouter.Route(ctx, service.MessageBatch{
		opStructMsg("upsert", map[string]any{"id": "1", "ts": seed.Add(time.Hour), "tstz": seed.Add(time.Hour)}),
	})
	require.Error(t, err, "a copy-on-write upsert on a legacy table must be rejected")
	assert.Contains(t, err.Error(), "legacy UTC-adjusted parquet encoding")
	assert.Contains(t, err.Error(), icebergx.TimestampEncodingProperty+"=spec")
	assert.NotContains(t, err.Error(), "cannot promote", "the guard must fire before the library's cryptic failure")
}

// TestTimestampEncodingNewTableIntegration proves a table auto-created by the
// connector is pinned to the spec encoding at birth: the creation commit
// carries redpanda-connect.timestamp-encoding=spec, its no-tz `timestamp`
// column is annotated isAdjustedToUTC=false on disk, and DuckDB types the
// column as a plain TIMESTAMP holding the exact written instant.
func TestTimestampEncodingNewTableIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)

	const ns, tblName = "ts_enc_new_ns", "ts_enc_new_test"
	infra.CreateNamespace(t, ns)
	client := infra.NewCatalogClient(t, ns)

	// Schema metadata declaring ts as a NO-timezone timestamp, so the
	// auto-created column is `timestamp` rather than the `timestamptz` that
	// bare time.Time inference produces.
	commonSchema := schema.Common{
		Type: schema.Object, Name: "Event",
		Children: []schema.Common{
			{Name: "id", Type: schema.String},
			{
				Name: "ts", Optional: true, Type: schema.Timestamp,
				Logical: &schema.LogicalParams{
					Timestamp: &schema.TimestampParams{Unit: schema.TimeUnitMicros, AdjustToUTC: false},
				},
			},
		},
	}

	seed := time.Date(2024, 3, 20, 8, 15, 0, 0, time.UTC)
	msg := service.NewMessage(nil)
	msg.SetStructured(map[string]any{"id": "1", "ts": seed})
	msg.MetaSetMut("schema", commonSchema.ToAny())

	router := infra.NewRouter(t, ns, tblName,
		WithSchemaEvolution(icebergimpl.SchemaEvolutionConfig{
			Enabled:        true,
			SchemaMetadata: "schema",
		}))
	produceMessages(t, ctx, router, service.MessageBatch{msg})

	tbl, err := client.LoadTable(ctx, tblName)
	require.NoError(t, err)

	// The pin must be present from creation.
	assert.Equal(t, "spec", tbl.Properties()[icebergx.TimestampEncodingProperty],
		"tables created by the connector must be pinned spec at creation")

	// The no-tz column must be spec-annotated on disk.
	tsField, ok := tbl.Schema().FindFieldByName("ts")
	require.True(t, ok)
	require.IsType(t, iceberg.TimestampType{}, tsField.Type, "schema metadata must yield a no-tz timestamp column")
	anns := tsAnnotationsPerDataFile(t, ctx, tbl)
	require.Len(t, anns, 1)
	for path, ann := range anns {
		assert.Equal(t, map[int]bool{tsField.ID: false}, ann,
			"a spec table's no-tz timestamp column must be isAdjustedToUTC=false: %s", path)
	}

	// DuckDB types the column as plain TIMESTAMP and reads the exact instant.
	type typeRow struct {
		ColumnName string `json:"column_name"`
		ColumnType string `json:"column_type"`
	}
	cols := querySQL[typeRow](t, ctx, infra,
		fmt.Sprintf(`DESCRIBE iceberg_cat."%s"."%s";`, ns, tblName))
	typeOf := map[string]string{}
	for _, c := range cols {
		typeOf[c.ColumnName] = c.ColumnType
	}
	assert.Equal(t, "TIMESTAMP", typeOf["ts"], "spec encoding must read as a plain (no-tz) TIMESTAMP")

	match := querySQL[countResult](t, ctx, infra, fmt.Sprintf(
		`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s" WHERE ts = TIMESTAMP '2024-03-20 08:15:00';`,
		ns, tblName))
	require.Len(t, match, 1)
	assert.Equal(t, 1, match[0].Count, "the written instant must round-trip exactly")
}
