// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package databrickse2e validates the iceberg output's copy-on-write
// (merge_strategy: copy-on-write) feature against a REAL Databricks Unity
// Catalog, reached over its Iceberg REST endpoint
// (https://<host>/api/2.1/unity-catalog/iceberg-rest). Written rows are read
// back through a serverless SQL warehouse via the SQL Statement Execution API
// (implemented with plain net/http — no extra module dependencies).
//
// Infrastructure (catalog, schema, SQL warehouse, grants) is provisioned by
// ./terraform — see README.md. The UC schema is pre-created there because
// client-side namespace creation against UC's Iceberg REST catalog is
// unverified; the tests never call CreateNamespace.
//
// These tests have run green against a live Databricks Unity Catalog
// (serverless workspace, customer-owned S3 via create_storage): CREATE TABLE
// acceptance, the identifier-field-ids rejection (verbatim ErrorCode 2014),
// the set-properties commit for the timestamp-encoding pin, equality-delete
// commit handling (rejected with ErrorCode 2013), and the commit-latency
// bench are all live-confirmed. Still unexercised: OAuth M2M auth for the
// Iceberg REST client (PAT was used throughout) and the explicit
// storage_root / metastore-root-inherit table-storage variants.
package databrickse2e

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	icebergimpl "github.com/redpanda-data/connect/v4/internal/impl/iceberg"
	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/catalogx"
)

var (
	dbxHost        = flag.String("databricks.host", "", "Databricks workspace host, e.g. dbc-abc123.cloud.databricks.com (scheme optional)")
	dbxCatalog     = flag.String("databricks.catalog", "", "Unity Catalog catalog name (used as the Iceberg REST warehouse)")
	dbxSchema      = flag.String("databricks.schema", "e2e", "Unity Catalog schema (the Iceberg namespace); must be pre-created by terraform")
	dbxWarehouseID = flag.String("databricks.warehouse-id", "", "SQL warehouse ID used to query written data back")
	dbxBench       = flag.Bool("databricks.bench", false, "run the commit latency bench test (costs a little warehouse time)")
)

// dbxToken returns the PAT bearer token. It is deliberately sourced from the
// environment only — never a flag, terraform variable, or output — so it can
// never end up in logs, task output, or terraform state.
func dbxToken() string {
	return os.Getenv("DATABRICKS_TOKEN")
}

func skipIfNotConfigured(t *testing.T) {
	t.Helper()
	if *dbxHost == "" || *dbxCatalog == "" || *dbxWarehouseID == "" {
		t.Skip("set -databricks.host, -databricks.catalog, -databricks.warehouse-id flags to run Databricks e2e tests")
	}
	if dbxToken() == "" {
		t.Skip("set the DATABRICKS_TOKEN environment variable to run Databricks e2e tests")
	}
}

// normalizeHost accepts a bare host or a full https:// URL and returns the
// bare host without scheme or trailing slash.
func normalizeHost(h string) string {
	h = strings.TrimPrefix(h, "https://")
	h = strings.TrimPrefix(h, "http://")
	return strings.TrimSuffix(h, "/")
}

// redact removes the bearer token from a string before it is logged or
// embedded in an error. The token only ever travels in Authorization headers
// (which are never logged), so this is belt-and-braces for response bodies.
func redact(s string) string {
	if tok := dbxToken(); tok != "" {
		return strings.ReplaceAll(s, tok, "[REDACTED]")
	}
	return s
}

// buildCatalogConfig points catalogx at the Unity Catalog Iceberg REST
// endpoint. The UC catalog name is passed as the Iceberg `warehouse`; UC's
// /v1/config response supplies the prefix. Auth is a PAT bearer token —
// recommended over OAuth2 for this endpoint (community reports intermittent
// 500s with OAuth2 M2M against the UC IRC).
func buildCatalogConfig() catalogx.Config {
	return catalogx.Config{
		URL:         fmt.Sprintf("https://%s/api/2.1/unity-catalog/iceberg-rest", normalizeHost(*dbxHost)),
		Warehouse:   *dbxCatalog,
		AuthType:    "bearer",
		BearerToken: dbxToken(),
	}
}

func newCatalogClient(t *testing.T, ctx context.Context) *catalogx.Client {
	t.Helper()
	client, err := catalogx.NewCatalogClient(ctx, buildCatalogConfig(), []string{*dbxSchema})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// newRouter mirrors the polaris-aws sibling but takes a RowOpConfig so both
// copy-on-write and merge-on-read modes are testable.
func newRouter(t *testing.T, namespace, tableName string, schemaEvo bool, rowOp icebergimpl.RowOpConfig) *icebergimpl.Router {
	t.Helper()
	namespaceStr, err := service.NewInterpolatedString(namespace)
	require.NoError(t, err)
	tableStr, err := service.NewInterpolatedString(tableName)
	require.NoError(t, err)

	logger := service.MockResources().Logger()
	commitCfg := icebergimpl.CommitConfig{
		ManifestMergeEnabled: true,
		MaxSnapshotAge:       24 * time.Hour,
		MaxRetries:           3,
	}
	schemaEvoCfg := icebergimpl.SchemaEvolutionConfig{
		Enabled: schemaEvo,
	}
	router := icebergimpl.NewRouter(buildCatalogConfig(), namespaceStr, tableStr, true, schemaEvoCfg, commitCfg, rowOp, nil, logger)
	t.Cleanup(func() { router.Close() })
	return router
}

// cowRowOp builds the copy-on-write row-operation config mirroring the YAML
//
//	row_operation: ${! meta("op") }
//	identifier_fields: [id]
//	merge_strategy: copy-on-write
func cowRowOp(t *testing.T) icebergimpl.RowOpConfig {
	t.Helper()
	op, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	return icebergimpl.RowOpConfig{
		Operation:        op,
		IdentifierFields: []string{"id"},
		MergeStrategy:    icebergimpl.MergeStrategyCOW,
	}
}

// morRowOp is the same but with the default merge-on-read strategy (the zero
// MergeStrategy value behaves as merge-on-read).
func morRowOp(t *testing.T) icebergimpl.RowOpConfig {
	t.Helper()
	op, err := service.NewInterpolatedString(`${! meta("op") }`)
	require.NoError(t, err)
	return icebergimpl.RowOpConfig{
		Operation:        op,
		IdentifierFields: []string{"id"},
	}
}

// opRow builds a structured message whose `op` metadata drives the
// row_operation interpolation, mirroring how a CDC source would map its
// operation onto the iceberg output. Structured (not raw JSON) so integer ids
// stay int64 and time.Time values map onto timestamp columns.
func opRow(op string, fields map[string]any) *service.Message {
	m := service.NewMessage(nil)
	m.SetStructured(fields)
	m.MetaSetMut("op", op)
	return m
}

func produce(t *testing.T, ctx context.Context, router *icebergimpl.Router, batch service.MessageBatch) {
	t.Helper()
	require.NoError(t, router.Route(ctx, batch))
	time.Sleep(2 * time.Second)
}

func uniqueTableName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// fqTable returns the backtick-quoted three-level Databricks SQL name.
func fqTable(tableName string) string {
	return fmt.Sprintf("`%s`.`%s`.`%s`", *dbxCatalog, *dbxSchema, tableName)
}

// --- SQL Statement Execution API (query-back through the warehouse) ---

// sqlPollTimeout bounds polling after the initial server-side wait. Statement
// submission auto-starts a stopped serverless warehouse, and serverless
// cold-start is typically a few seconds, so 30s wait + 60s poll is generous.
const sqlPollTimeout = 60 * time.Second

type sqlStatementResponse struct {
	StatementID string `json:"statement_id"`
	Status      struct {
		State string `json:"state"`
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"status"`
	Manifest struct {
		Schema struct {
			Columns []struct {
				Name     string `json:"name"`
				TypeName string `json:"type_name"`
			} `json:"columns"`
		} `json:"schema"`
	} `json:"manifest"`
	Result struct {
		DataArray [][]*string `json:"data_array"`
	} `json:"result"`
}

func dbxSQLRequest(ctx context.Context, method, url string, payload any) (*sqlStatementResponse, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+dbxToken())
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, url, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s %s: reading body: %w", method, url, err)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s failed (%d): %s", method, url, resp.StatusCode, redact(string(respBody)))
	}
	var out sqlStatementResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("%s %s: decoding response: %w", method, url, err)
	}
	return &out, nil
}

// runSQL executes a statement on the configured SQL warehouse and returns the
// result rows keyed by column name (NULL values become ""). It submits with a
// 30s server-side wait, then polls until SUCCEEDED/FAILED or sqlPollTimeout.
func runSQL(ctx context.Context, statement string) ([]map[string]string, error) {
	base := "https://" + normalizeHost(*dbxHost) + "/api/2.0/sql/statements"
	resp, err := dbxSQLRequest(ctx, http.MethodPost, base, map[string]any{
		"statement":       statement,
		"warehouse_id":    *dbxWarehouseID,
		"wait_timeout":    "30s",
		"on_wait_timeout": "CONTINUE",
	})
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(sqlPollTimeout)
	for resp.Status.State == "PENDING" || resp.Status.State == "RUNNING" {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("statement %s still %s after %v: %q", resp.StatementID, resp.Status.State, sqlPollTimeout, statement)
		}
		time.Sleep(2 * time.Second)
		resp, err = dbxSQLRequest(ctx, http.MethodGet, base+"/"+resp.StatementID, nil)
		if err != nil {
			return nil, err
		}
	}
	if resp.Status.State != "SUCCEEDED" {
		return nil, fmt.Errorf("statement %s finished %s: %s (statement: %q)",
			resp.StatementID, resp.Status.State, redact(resp.Status.Error.Message), statement)
	}

	rows := make([]map[string]string, 0, len(resp.Result.DataArray))
	for _, raw := range resp.Result.DataArray {
		row := make(map[string]string, len(resp.Manifest.Schema.Columns))
		for i, col := range resp.Manifest.Schema.Columns {
			if i < len(raw) && raw[i] != nil {
				row[col.Name] = *raw[i]
			} else {
				row[col.Name] = ""
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// sqlQuery is runSQL with a hard failure on error.
func sqlQuery(t *testing.T, ctx context.Context, statement string) []map[string]string {
	t.Helper()
	rows, err := runSQL(ctx, statement)
	require.NoError(t, err)
	return rows
}

// dropTable best-effort drops a test table through the warehouse so repeated
// runs never need a terraform re-apply.
func dropTable(t *testing.T, tableName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := runSQL(ctx, "DROP TABLE IF EXISTS "+fqTable(tableName)); err != nil {
		t.Logf("warning: failed to drop table %s: %v", tableName, err)
	}
}

// countManifestsByContent loads the table's current snapshot and tallies its
// manifests by content kind — copy-on-write must leave only data manifests
// and zero delete manifests, which is what makes the result readable by the
// Unity Catalog (which cannot apply Iceberg v2 delete files).
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

// TestDatabricksE2E_COWRoundTrip is THE release-gate proof: an insert →
// upsert → delete round trip through merge_strategy: copy-on-write against a
// real Unity Catalog, read back through a real Databricks SQL warehouse.
//
// The table is pre-created via the Iceberg REST catalog with an explicit
// schema (id long, name string, ts timestamp, tstz timestamptz) so the
// no-timezone `timestamp` column exercises the spec timestamp encoding
// against Databricks' reader — auto-created columns from time.Time values
// would all be timestamptz. Keep it UNPARTITIONED: UC ignores/re-clusters
// Iceberg partition specs.
//
// UNVERIFIED-WITHOUT-LIVE-ACCESS: the first write also stamps the
// redpanda-connect.timestamp-encoding table property through a set-properties
// commit; UC accepting that commit is one of the behaviours this test proves.
func TestDatabricksE2E_COWRoundTrip(t *testing.T) {
	skipIfNotConfigured(t)
	ctx := t.Context()

	tableName := uniqueTableName("cow_e2e")
	t.Cleanup(func() { dropTable(t, tableName) })

	client := newCatalogClient(t, ctx)
	// All columns optional and NO identifier-field-ids — the same shape the
	// router's copy-on-write auto-create produces, and the shape UC accepts.
	_, err := client.CreateTable(ctx, tableName, iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}},
		iceberg.NestedField{ID: 3, Name: "ts", Type: iceberg.TimestampType{}},
		iceberg.NestedField{ID: 4, Name: "tstz", Type: iceberg.TimestampTzType{}},
	))
	require.NoError(t, err, "CREATE TABLE via the UC Iceberg REST catalog must succeed")

	seed := time.Date(2026, 1, 15, 12, 30, 45, 0, time.UTC)
	row := func(op string, id int64, name string, at time.Time) *service.Message {
		return opRow(op, map[string]any{"id": id, "name": name, "ts": at, "tstz": at})
	}

	router := newRouter(t, *dbxSchema, tableName, true, cowRowOp(t))

	// Seed three rows.
	produce(t, ctx, router, service.MessageBatch{
		row("insert", 1, "one", seed),
		row("insert", 2, "two", seed),
		row("insert", 3, "three", seed),
	})

	// One mutating batch: upsert id=2, delete id=3, upsert id=4 (new row) —
	// the combined overwrite+delete path that rewrites data files in a single
	// atomic snapshot.
	produce(t, ctx, router, service.MessageBatch{
		row("upsert", 2, "two-updated", seed.Add(time.Hour)),
		opRow("delete", map[string]any{"id": int64(3)}),
		row("upsert", 4, "four", seed.Add(2*time.Hour)),
	})

	// Read back THROUGH THE WAREHOUSE — this is Databricks' own reader
	// consuming what the connector wrote. ts (TIMESTAMP_NTZ) round-trips as a
	// naive wall-clock string; tstz (TIMESTAMP) is compared as an epoch so the
	// assertion is independent of the warehouse session timezone.
	rows := sqlQuery(t, ctx, fmt.Sprintf(
		"SELECT id, name, date_format(ts, 'yyyy-MM-dd HH:mm:ss') AS ts, CAST(unix_timestamp(tstz) AS STRING) AS tstz_unix FROM %s ORDER BY id",
		fqTable(tableName)))
	t.Logf("warehouse read-back: %v", rows)

	require.Len(t, rows, 3, "expected id=1, id=2, id=4 (id=3 deleted, id=2 not duplicated)")
	expect := []struct {
		id, name string
		at       time.Time
	}{
		{"1", "one", seed},
		{"2", "two-updated", seed.Add(time.Hour)},
		{"4", "four", seed.Add(2 * time.Hour)},
	}
	for i, e := range expect {
		assert.Equal(t, e.id, rows[i]["id"])
		assert.Equal(t, e.name, rows[i]["name"])
		assert.Equal(t, e.at.UTC().Format("2006-01-02 15:04:05"), rows[i]["ts"], "ts (timestamp_ntz) must round-trip naively for id=%s", e.id)
		assert.Equal(t, strconv.FormatInt(e.at.Unix(), 10), rows[i]["tstz_unix"], "tstz (timestamptz) must round-trip as the same instant for id=%s", e.id)
	}

	// Log and assert how Databricks reports the two timestamp flavours.
	descRows := sqlQuery(t, ctx, "DESCRIBE TABLE "+fqTable(tableName))
	colTypes := map[string]string{}
	for _, r := range descRows {
		colTypes[r["col_name"]] = r["data_type"]
	}
	t.Logf("DESCRIBE TABLE column types: %v", colTypes)
	assert.Equal(t, "timestamp_ntz", strings.ToLower(colTypes["ts"]), "no-tz iceberg timestamp should surface as TIMESTAMP_NTZ")
	assert.Equal(t, "timestamp", strings.ToLower(colTypes["tstz"]), "iceberg timestamptz should surface as TIMESTAMP")

	// Zero delete files, via the catalog's own manifests: non-vacuous (at
	// least one data manifest) AND exactly zero delete manifests. A
	// merge-on-read run of the same batch would have left delete manifests —
	// this property is the entire point of copy-on-write on Databricks.
	loaded, err := client.LoadTable(ctx, tableName)
	require.NoError(t, err)
	dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
	assert.Positive(t, dataManifests, "expected at least one data manifest to inspect")
	assert.Zero(t, deleteManifests, "copy-on-write must leave zero delete manifests")

	require.NotNil(t, loaded.CurrentSnapshot())
	assert.Equal(t, table.OpOverwrite, loaded.CurrentSnapshot().Summary.Operation,
		"the upsert+delete batch must commit as an overwrite under copy-on-write")

	// Finally: the router's own copy-on-write auto-create (no
	// identifier-field-ids registered) must be accepted by UC — this is the
	// exact CREATE TABLE the original field report saw rejected under
	// merge-on-read, and the no-registration rationale behind copy-on-write.
	autoTable := uniqueTableName("cow_autocreate")
	t.Cleanup(func() { dropTable(t, autoTable) })
	autoRouter := newRouter(t, *dbxSchema, autoTable, true, cowRowOp(t))
	produce(t, ctx, autoRouter, service.MessageBatch{
		opRow("insert", map[string]any{"id": int64(1), "name": "auto"}),
	})
	autoRows := sqlQuery(t, ctx, fmt.Sprintf("SELECT id, name FROM %s", fqTable(autoTable)))
	require.Len(t, autoRows, 1, "COW auto-created table must be readable through the warehouse")
	assert.Equal(t, "1", autoRows[0]["id"])
	assert.Equal(t, "auto", autoRows[0]["name"])
}

// TestDatabricksE2E_IdentifierFieldsRejected reproduces the original field
// report: under merge-on-read with identifier_fields, the router registers
// the identifier-field-ids on CREATE TABLE, and Unity Catalog rejects that
// ("Table with identifier columns is not allowed. [ErrorCode: 2014]" — the
// wording may drift, so the assertion is deliberately loose).
//
// If UC ever ACCEPTS this creation, the test fails loudly: it would mean UC
// behaviour has changed and the copy-on-write no-registration rationale
// should be revisited.
func TestDatabricksE2E_IdentifierFieldsRejected(t *testing.T) {
	skipIfNotConfigured(t)
	ctx := t.Context()

	tableName := uniqueTableName("mor_create_e2e")
	// In case creation unexpectedly succeeds, don't leave the table behind.
	t.Cleanup(func() { dropTable(t, tableName) })

	router := newRouter(t, *dbxSchema, tableName, true, morRowOp(t))
	// int64 id (via structured message) so the identifier column passes the
	// router's own non-floating-point key validation and the CREATE TABLE
	// actually reaches Unity Catalog carrying identifier-field-ids.
	err := router.Route(ctx, service.MessageBatch{
		opRow("insert", map[string]any{"id": int64(1), "name": "alice"}),
	})
	if err == nil {
		t.Fatal("Unity Catalog ACCEPTED a CREATE TABLE carrying identifier-field-ids — UC behaviour has changed! " +
			"Revisit the copy-on-write no-registration rationale (Router.schemaWithIdentifierFields) and this test.")
	}
	t.Logf("UC rejected CREATE TABLE with identifier-field-ids as expected. Full error: %v", err)
	assert.Contains(t, strings.ToLower(err.Error()), "identifier",
		"expected the rejection to mention identifier columns (loose match — UC wording may have drifted, check the logged error)")
}

// TestDatabricksE2E_MORDeleteFilesDiagnostic is a DIAGNOSTIC, not a gate: it
// settles empirically whether UC rejects a merge-on-read equality-delete
// commit outright, or accepts it and then serves stale (or no) rows. The
// table is pre-created WITHOUT identifier-field-ids (COW-style schema) so
// creation cannot fail, then a merge-on-read router attempts an
// equality-delete commit against it. Every outcome is logged; nothing about
// UC's choice is a hard pass/fail — the test only fails if we learn nothing.
func TestDatabricksE2E_MORDeleteFilesDiagnostic(t *testing.T) {
	skipIfNotConfigured(t)
	ctx := t.Context()

	tableName := uniqueTableName("mor_diag_e2e")
	t.Cleanup(func() { dropTable(t, tableName) })

	client := newCatalogClient(t, ctx)
	_, err := client.CreateTable(ctx, tableName, iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.Int64Type{}},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.StringType{}},
	))
	require.NoError(t, err, "pre-creating the diagnostic table (no identifier-field-ids) must succeed")

	router := newRouter(t, *dbxSchema, tableName, true, morRowOp(t))

	// Seed with plain inserts — append-only commits, expected to succeed even
	// under merge-on-read.
	if err := router.Route(ctx, service.MessageBatch{
		opRow("insert", map[string]any{"id": int64(1), "name": "one"}),
		opRow("insert", map[string]any{"id": int64(2), "name": "two"}),
	}); err != nil {
		t.Logf("LEARNED (unexpected): even append-only inserts failed under merge-on-read: %v", err)
		return
	}
	time.Sleep(2 * time.Second)

	// The probe: a merge-on-read delete writes an Iceberg v2 equality-delete
	// file and commits it. Does UC reject the commit, or accept it?
	deleteErr := router.Route(ctx, service.MessageBatch{
		opRow("delete", map[string]any{"id": int64(2)}),
	})
	if deleteErr != nil {
		t.Logf("LEARNED: UC REJECTED the equality-delete commit outright (error, not silent staleness): %v", deleteErr)
		return
	}
	time.Sleep(2 * time.Second)
	t.Log("LEARNED: UC ACCEPTED the equality-delete commit; inspecting what a reader now sees...")

	if loaded, lerr := client.LoadTable(ctx, tableName); lerr == nil {
		dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
		t.Logf("catalog view after commit: %d data manifests, %d delete manifests", dataManifests, deleteManifests)
	} else {
		t.Logf("could not load table back through the catalog: %v", lerr)
	}

	rows, qerr := runSQL(ctx, fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", fqTable(tableName)))
	switch {
	case qerr != nil:
		t.Logf("LEARNED: warehouse read-back FAILED after the accepted delete commit (UC likely refuses tables with delete files): %v", qerr)
	case len(rows) == 2:
		t.Logf("LEARNED: warehouse serves STALE rows — the equality delete was silently ignored by the reader: %v", rows)
	case len(rows) == 1 && rows[0]["id"] == "1":
		t.Logf("LEARNED: warehouse APPLIED the equality delete — UC merge-on-read reading works here: %v", rows)
	default:
		t.Logf("LEARNED: unexpected read-back state after the accepted delete commit: %v", rows)
	}
}

// TestDatabricksE2E_CommitLatencyBench measures copy-on-write upsert commit
// latency against the real UC at three batch sizes. Gated behind
// -databricks.bench because it burns (a tiny amount of) warehouse and API
// time. Per size: seed the table, then time 3 full-batch upsert commits —
// the worst case, every data file rewritten.
func TestDatabricksE2E_CommitLatencyBench(t *testing.T) {
	skipIfNotConfigured(t)
	if !*dbxBench {
		t.Skip("set -databricks.bench to run the commit latency bench")
	}
	ctx := t.Context()

	batch := func(op string, size, gen int) service.MessageBatch {
		msgs := make(service.MessageBatch, size)
		for i := range msgs {
			msgs[i] = opRow(op, map[string]any{
				"id":    int64(i),
				"name":  fmt.Sprintf("user_%d", i),
				"value": int64(gen),
			})
		}
		return msgs
	}

	for _, size := range []int{100, 1000, 5000} {
		t.Run(fmt.Sprintf("rows_%d", size), func(t *testing.T) {
			tableName := uniqueTableName(fmt.Sprintf("cow_bench_%d", size))
			t.Cleanup(func() { dropTable(t, tableName) })

			router := newRouter(t, *dbxSchema, tableName, true, cowRowOp(t))
			produce(t, ctx, router, batch("insert", size, 0)) // seed (auto-creates the table)

			for commit := 1; commit <= 3; commit++ {
				start := time.Now()
				require.NoError(t, router.Route(ctx, batch("upsert", size, commit)))
				elapsed := time.Since(start)
				t.Logf("size=%d commit=%d: %v (%.0f rows/s)", size, commit, elapsed, float64(size)/elapsed.Seconds())
			}
		})
	}
}
