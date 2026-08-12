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

// assertCOWSnapshot loads the committed table through the REST catalog and
// asserts the copy-on-write invariant: at least one real data manifest, ZERO
// delete manifests (what makes the result readable by engine-backed catalogs),
// and the current snapshot's operation equals wantOp. It is the shared
// end-of-test check reused by the copy-on-write e2e tests in this package.
func assertCOWSnapshot(t *testing.T, ctx context.Context, infra *testInfrastructure, ns, tbl string, wantOp table.Operation) {
	t.Helper()
	client := infra.NewCatalogClient(t, ns)
	loaded, err := client.LoadTable(ctx, tbl)
	require.NoError(t, err)
	dataManifests, deleteManifests := countManifestsByContent(t, ctx, loaded)
	assert.Positive(t, dataManifests, "expected at least one data manifest to inspect")
	assert.Zero(t, deleteManifests, "copy-on-write must leave zero delete manifests")
	require.NotNil(t, loaded.CurrentSnapshot())
	assert.Equal(t, wantOp, loaded.CurrentSnapshot().Summary.Operation,
		"unexpected snapshot operation under copy-on-write")
}

// TestCOWRowOperationKeyTypesIntegration is the copy-on-write analog of
// TestRowOperationKeyTypesIntegration: for every supported non-string merge-key
// type it pre-creates a table keyed on that type, seeds two rows, then runs a
// single mutating batch (upsert k1 in place + delete k2) under
// merge_strategy: copy-on-write and asserts — via DuckDB, an INDEPENDENT reader
// of the committed table — the exact final row set.
//
// This is the highest-value gap closed by these tests. The copy-on-write filter
// literal (cowKeyLiteral) and the rewrite's re-encoding of the surviving key
// (cowMassage -> jsonLeafValue -> Arrow) are the riskiest code: a wrong
// encoding makes the overwrite filter select no rows, so the delete/upsert
// silently becomes a no-op (a silent no-op instead of a mutation) or the rewritten key is
// corrupted. All existing unit round-trips read back through iceberg-go's own
// Arrow scan, so a self-consistent-but-wrong encoding would pass. DuckDB reads
// the parquet + manifests itself, so a wrong encoding shows up here as a wrong
// row count, a stale row surviving, or a key that fails DuckDB's own typed
// equality against the literal we expect.
//
// The int64 case deliberately uses values > 2^53 (not representable exactly as
// float64) to prove the integer-as-string encoding survives the rewrite. The
// per-type WHERE ... = <typed literal> query asks DuckDB to confirm the
// surviving key equals the exact value we upserted, using DuckDB's own type
// system rather than iceberg-go's.
//
// Note: boolean and decimal are GATED as copy-on-write merge keys (the vendored
// iceberg-go overwrite filter cannot apply either — see cowKeyLiteral), so they
// are intentionally not exercised as keys here.
func TestCOWRowOperationKeyTypesIntegration(t *testing.T) {
	integration.CheckSkip(t)
	ctx := context.Background()
	infra := setupTestInfra(t, ctx)
	const ns = "cow_keytypes"
	infra.CreateNamespace(t, ns)

	cases := []struct {
		name    string
		tbl     string
		keyType iceberg.Type
		k1, k2  any
		// lit is the DuckDB typed literal equal to k1 (the surviving/upserted
		// key). The final row must satisfy k = lit under DuckDB's own typing.
		lit string
		// skip, when non-empty, documents a genuine defect this case surfaces and
		// keeps the case visible without failing the suite (see the comment on the
		// timestamp case below).
		skip string
	}{
		{
			name:    "int64-big",
			tbl:     "k_int64",
			keyType: iceberg.Int64Type{},
			// 2^53+1 and 2^53+2: exact as int64, NOT exact as float64.
			k1:  int64(9007199254740993),
			k2:  int64(9007199254740994),
			lit: "9007199254740993",
		},
		{
			name:    "date",
			tbl:     "k_date",
			keyType: iceberg.DateType{},
			k1:      time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			k2:      time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC),
			lit:     "DATE '2024-01-15'",
		},
		{
			name:    "time",
			tbl:     "k_time",
			keyType: iceberg.TimeType{},
			// UTC time-of-day; the copy-on-write encoder canonicalises in UTC so
			// filter and storage agree.
			k1:  time.Date(1970, 1, 1, 12, 30, 45, 0, time.UTC),
			k2:  time.Date(1970, 1, 1, 13, 30, 45, 0, time.UTC),
			lit: "TIME '12:30:45'",
		},
		{
			name:    "timestamp",
			tbl:     "k_ts",
			keyType: iceberg.TimestampType{},
			k1:      time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC),
			k2:      time.Date(2024, 1, 16, 12, 30, 45, 0, time.UTC),
			lit:     "TIMESTAMP '2024-01-15 12:30:45'",
			// Previously skipped: a copy-on-write upsert/delete on a table with a
			// no-timezone `timestamp` column used to fail at commit with
			//   "failed to rewrite file ...: cannot promote timestamptz to timestamp".
			// The append path (icebergx/parquet.go) wrote no-tz `timestamp` columns
			// with the parquet annotation isAdjustedToUTC=true, so iceberg-go read the
			// existing file back as `timestamptz` and the copy-on-write rewrite's
			// strict schema visitor refused to promote it to the table's declared
			// no-tz `timestamp`. Fixed by writing no-tz `timestamp` with
			// isAdjustedToUTC=false, matching iceberg-go's own Arrow writer. NOTE: this
			// only fixes tables whose data files were written after the fix; a table
			// with pre-existing old-encoding (isAdjustedToUTC=true) files still fails
			// copy-on-write and would need its data rewritten.
		},
		{
			name:    "timestamptz",
			tbl:     "k_tstz",
			keyType: iceberg.TimestampTzType{},
			k1:      time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC),
			k2:      time.Date(2024, 1, 16, 12, 30, 45, 0, time.UTC),
			lit:     "TIMESTAMPTZ '2024-01-15 12:30:45+00'",
		},
		{
			name:    "uuid",
			tbl:     "k_uuid",
			keyType: iceberg.UUIDType{},
			k1:      "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			k2:      "1b4e28ba-2fa1-11d2-883f-0016d3cca427",
			lit:     "UUID 'f47ac10b-58cc-4372-a567-0e02b2c3d479'",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}
			// Pre-create the table keyed on the typed column. The schema carries no
			// identifier-field-ids: under copy-on-write, identifier_fields are the
			// connector-side merge key only.
			client := infra.NewCatalogClient(t, ns)
			_, err := client.CreateTable(ctx, tc.tbl, iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "k", Type: tc.keyType, Required: true},
				iceberg.NestedField{ID: 2, Name: "val", Type: iceberg.StringType{}, Required: false},
			))
			require.NoError(t, err)

			operation, err := service.NewInterpolatedString(`${! meta("op") }`)
			require.NoError(t, err)
			router := infra.NewRouter(t, ns, tc.tbl,
				WithRowOperation(icebergimpl.RowOpConfig{
					Operation:        operation,
					IdentifierFields: []string{"k"},
					MergeStrategy:    icebergimpl.MergeStrategyCOW,
				}))

			// Seed k1, k2 (append fast path — no keyed ops in this batch).
			produceMessages(t, ctx, router, service.MessageBatch{
				opStructMsg("insert", map[string]any{"k": tc.k1, "val": "a"}),
				opStructMsg("insert", map[string]any{"k": tc.k2, "val": "b"}),
			})

			// One mutating batch: upsert k1 (new value) and delete k2. Contains both
			// a row to (re)write and keys to remove, so it lands as a single
			// copy-on-write overwrite. A wrong key-literal encoding makes the filter
			// match nothing: k2 would survive (count 2) or the upsert would duplicate
			// k1 (count 2).
			produceMessages(t, ctx, router, service.MessageBatch{
				opStructMsg("upsert", map[string]any{"k": tc.k1, "val": "a2"}),
				opStructMsg("delete", map[string]any{"k": tc.k2}),
			})

			// (a) Exactly one surviving row (k2 deleted, k1 not duplicated),
			// observed by DuckDB. Select the key column per the projection quirk.
			type valRow struct {
				Val string `json:"val"`
			}
			rows := querySQL[valRow](t, ctx, infra,
				fmt.Sprintf(`SELECT k, val FROM iceberg_cat."%s"."%s";`, ns, tc.tbl))
			require.Lenf(t, rows, 1, "expected exactly one surviving row; got %d", len(rows))
			assert.Equal(t, "a2", rows[0].Val, "surviving row must be the upserted value")

			// (b) The surviving key equals the exact value we upserted, per DuckDB's
			// own typed comparison. This is the load-bearing check for the >2^53
			// int64 case: a truncated re-encode would fail k = 9007199254740993.
			match := querySQL[countResult](t, ctx, infra,
				fmt.Sprintf(`SELECT COUNT(*) AS count FROM iceberg_cat."%s"."%s" WHERE k = %s AND val = 'a2';`,
					ns, tc.tbl, tc.lit))
			require.Len(t, match, 1)
			assert.Equal(t, 1, match[0].Count,
				"DuckDB must find the surviving row keyed by the exact upserted value %s", tc.lit)

			// (c) Copy-on-write invariant: zero delete manifests + overwrite op.
			assertCOWSnapshot(t, ctx, infra, ns, tc.tbl, table.OpOverwrite)
		})
	}
}
