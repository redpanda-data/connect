// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func testSchemaCache(t *testing.T) *schemaCache {
	t.Helper()
	return newSchemaCache(nil, "", service.NewLoggerFromSlog(slog.Default()))
}

func parseSchema(t *testing.T, s any) schema.Common {
	t.Helper()
	require.NotNil(t, s)
	c, err := schema.ParseFromAny(s)
	require.NoError(t, err)
	return c
}

func childByName(t *testing.T, c schema.Common, name string) schema.Common {
	t.Helper()
	for i := range c.Children {
		if c.Children[i].Name == name {
			return c.Children[i]
		}
	}
	t.Fatalf("child %q not found in schema %q", name, c.Name)
	return schema.Common{}
}

// seedCache is a shorthand that seeds the cache and returns the schema.
func seedCache(t *testing.T, sc *schemaCache, schemaName, tableName string, meta []replication.ColumnMeta) any {
	t.Helper()
	sc.seedFromColumnMeta(replication.UserTable{Schema: schemaName, Name: tableName}, meta)
	s, _, err := sc.schemaForEvent(context.Background(), replication.UserTable{Schema: schemaName, Name: tableName}, nil)
	require.NoError(t, err)
	return s
}

// ---------------------------------------------------------------------------
// Type mapping
// ---------------------------------------------------------------------------

func TestOracleTypeToCommonType(t *testing.T) {
	tests := []struct {
		typeName string
		want     schema.CommonType
	}{
		{"BINARY_FLOAT", schema.Float32},
		{"binary_float", schema.Float32},
		{"Binary_Float", schema.Float32},

		{"BINARY_DOUBLE", schema.Float64},
		{"binary_double", schema.Float64},

		{"RAW", schema.ByteArray},
		{"raw", schema.ByteArray},
		{"LONG RAW", schema.ByteArray},
		{"long raw", schema.ByteArray},
		{"BLOB", schema.ByteArray},
		{"blob", schema.ByteArray},

		{"DATE", schema.Timestamp},
		{"date", schema.Timestamp},
		{"TIMESTAMP", schema.Timestamp},
		{"timestamp", schema.Timestamp},
		{"TIMESTAMP WITH TIME ZONE", schema.Timestamp},
		{"timestamp with time zone", schema.Timestamp},
		{"TIMESTAMP WITH LOCAL TIME ZONE", schema.Timestamp},
		{"timestamp with local time zone", schema.Timestamp},

		// ALL_TAB_COLUMNS.DATA_TYPE embeds the fractional-seconds precision in
		// the type name for the TIMESTAMP family. These are the names the
		// catalog actually reports — the bare names above only come from the
		// driver. Regression test for the snapshot-vs-streaming schema split
		// where TIMESTAMP(6) fell through to String.
		{"TIMESTAMP(0)", schema.Timestamp},
		{"TIMESTAMP(6)", schema.Timestamp},
		{"TIMESTAMP(9)", schema.Timestamp},
		{"TIMESTAMP(6) WITH TIME ZONE", schema.Timestamp},
		{"TIMESTAMP(9) WITH TIME ZONE", schema.Timestamp},
		{"TIMESTAMP(6) WITH LOCAL TIME ZONE", schema.Timestamp},

		// go-ora driver names (sql.ColumnType.DatabaseTypeName()) for the same
		// temporal types, as seeded from snapshot column metadata.
		{"TimeStampDTY", schema.Timestamp},
		{"TimeStampTZ", schema.Timestamp},
		{"TimeStampTZ_DTY", schema.Timestamp},
		{"TimeStampLTZ_DTY", schema.Timestamp},
		{"TimeStampeLTZ", schema.Timestamp},

		// go-ora driver names for binary types.
		{"VarRaw", schema.ByteArray},
		{"LongRaw", schema.ByteArray},
		{"LongVarRaw", schema.ByteArray},
		{"OCIBlobLocator", schema.ByteArray},

		{"JSON", schema.Any},
		{"json", schema.Any},
		// go-ora v2.9.0 has no stringer entry for the native JSON type (119).
		{"TNSType(119)", schema.Any},
		// ...but other unknown TNS type ids must still default to String.
		{"TNSType(121)", schema.String},

		// Interval catalog names also carry parenthesised qualifiers; they are
		// unmapped and must land on String from both sources.
		{"INTERVAL DAY(2) TO SECOND(6)", schema.String},
		{"INTERVAL YEAR(2) TO MONTH", schema.String},
		{"IntervalDS_DTY", schema.String},
		{"IntervalYM_DTY", schema.String},

		{"VARCHAR2", schema.String},
		{"varchar2", schema.String},
		{"CHAR", schema.String},
		{"NVARCHAR2", schema.String},
		{"NCHAR", schema.String},
		{"CLOB", schema.String},
		{"NCLOB", schema.String},
		{"LONG", schema.String},

		// Unknown types default to String.
		{"MYSTERY_TYPE", schema.String},
		{"", schema.String},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			assert.Equal(t, tt.want, oracleTypeToCommonType(tt.typeName))
		})
	}
}

func TestOracleNumberToCommonType(t *testing.T) {
	tests := []struct {
		name      string
		precision int64
		scale     int64
		hasInfo   bool
		wantType  schema.CommonType
	}{
		{"integer precision 10", 10, 0, true, schema.Int64},
		{"integer precision 18 boundary", 18, 0, true, schema.Int64},
		{"precision 19 exceeds int64", 19, 0, true, schema.Decimal},
		{"precision 38 max oracle", 38, 0, true, schema.Decimal},
		{"fractional scale 2", 10, 2, true, schema.Decimal},
		{"bare NUMBER no info", 0, 0, false, schema.BigDecimal},
		{"NUMBER(0) edge case maps to BigDecimal", 0, 0, true, schema.BigDecimal},
		// Negative scale (NUMBER(p,-s)) is only ever reported by the catalog;
		// the driver's uint8 scale wraps and lands on BigDecimal via the
		// scale-greater-than-precision sentinel, so the catalog must match.
		{"negative scale maps to BigDecimal", 5, -2, true, schema.BigDecimal},
		{"driver-wrapped negative scale maps to BigDecimal", 5, 254, true, schema.BigDecimal},
		{"driver sentinel scale 255 maps to BigDecimal", 38, 255, true, schema.BigDecimal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := replication.NumberToCommon("col", tt.precision, tt.scale, tt.hasInfo)
			assert.Equal(t, tt.wantType, c.Type)
			// Bounded decimals must carry the declared precision/scale in
			// their logical params — the Avro decimal type is
			// (precision, scale), not just "decimal", so a silent clamp or
			// substitution here re-registers an incompatible schema.
			if tt.wantType == schema.Decimal {
				require.NotNil(t, c.Logical)
				require.NotNil(t, c.Logical.Decimal)
				assert.Equal(t, int32(tt.precision), c.Logical.Decimal.Precision)
				assert.Equal(t, int32(tt.scale), c.Logical.Decimal.Scale)
			}
		})
	}
}

func TestIsNumberType(t *testing.T) {
	for _, tt := range []struct {
		typeName string
		want     bool
	}{
		{"NUMBER", true},
		{"number", true},
		{"Number", true},
		{"INTEGER", true},
		{"integer", true},
		{"INT", true},
		{"int", true},
		{"SMALLINT", true},
		{"smallint", true},
		{"FLOAT", true},
		{"float", true},
		{"VARCHAR2", false},
		{"DATE", false},
		{"BLOB", false},
		{"", false},
	} {
		t.Run(tt.typeName, func(t *testing.T) {
			assert.Equal(t, tt.want, isNumberType(tt.typeName))
		})
	}
}

// TestOracleSchemaSourceParity pins the two schema sources to each other. The
// schema cache is populated from ALL_TAB_COLUMNS on Connect()/drift refresh and
// from go-ora driver column metadata on snapshot seeding — and the two report
// the same Oracle column differently (catalog "TIMESTAMP(6)" vs driver
// "TimeStampDTY"; catalog NULL precision for INTEGER vs driver (38, 255); …).
// If both inputs for any column type don't map to an identical schema.Common,
// the schema attached to a message flips depending on which path produced it,
// and Schema Registry rejects every message from the losing path with a
// permanent BACKWARD MISSING_UNION_BRANCH error.
//
// Each row records the column type's real-world representation from both
// sources:
//   - catalog: DATA_TYPE / DATA_PRECISION / DATA_SCALE as ALL_TAB_COLUMNS
//     reports them, NULLs included — the row is fed through catalogNumberInfo
//     exactly as fetchTableSchema does.
//   - driver: sql.ColumnType DatabaseTypeName() / DecimalSize() as go-ora
//     v2.9.0 reports them. NUMBER-family wire metadata: bare NUMBER and FLOAT
//     surface as precision 38 / scale 0xFF (see go-ora ParameterInfo.load);
//     NUMBER(*,s) — including INTEGER/INT/SMALLINT, which are NUMBER(*,0) —
//     surfaces as precision 38 with the declared scale (verified against real
//     Oracle by TestIntegrationOracleDBCDCDataTypeConsistency's restart leg);
//     a declared negative scale wraps through uint8 (e.g. -2 → 254);
//     DecimalSize() returns ok=true for any NUMBER.
func TestOracleSchemaSourceParity(t *testing.T) {
	type catalogSource struct {
		typeName         string
		precision, scale sql.NullInt64
	}
	type driverSource struct {
		typeName         string
		precision, scale int64
		hasInfo          bool
	}
	n := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	tests := []struct {
		name    string
		catalog catalogSource
		drivers []driverSource // some types have multiple driver spellings
		want    schema.CommonType
		// wantDecimal pins the exact [precision, scale] for Decimal rows.
		// Agreement between the two sources alone is NOT enough: Avro decimal
		// compatibility is on (precision, scale), so both sources agreeing on
		// the WRONG parameters would still re-register an incompatible schema
		// against existing subjects.
		wantDecimal []int32
	}{
		{
			name:    "DATE",
			catalog: catalogSource{typeName: "DATE"},
			drivers: []driverSource{{typeName: "DATE"}},
			want:    schema.Timestamp,
		},
		{
			name:    "TIMESTAMP",
			catalog: catalogSource{typeName: "TIMESTAMP(6)"},
			drivers: []driverSource{{typeName: "TimeStampDTY"}, {typeName: "TIMESTAMP"}},
			want:    schema.Timestamp,
		},
		{
			name:    "TIMESTAMP(9)",
			catalog: catalogSource{typeName: "TIMESTAMP(9)"},
			drivers: []driverSource{{typeName: "TimeStampDTY"}},
			want:    schema.Timestamp,
		},
		{
			name:    "TIMESTAMP WITH TIME ZONE",
			catalog: catalogSource{typeName: "TIMESTAMP(6) WITH TIME ZONE"},
			drivers: []driverSource{{typeName: "TimeStampTZ_DTY"}, {typeName: "TimeStampTZ"}},
			want:    schema.Timestamp,
		},
		{
			name:    "TIMESTAMP WITH LOCAL TIME ZONE",
			catalog: catalogSource{typeName: "TIMESTAMP(6) WITH LOCAL TIME ZONE"},
			drivers: []driverSource{{typeName: "TimeStampLTZ_DTY"}, {typeName: "TimeStampeLTZ"}},
			want:    schema.Timestamp,
		},
		{
			name:        "NUMBER(10,2)",
			catalog:     catalogSource{typeName: "NUMBER", precision: n(10), scale: n(2)},
			drivers:     []driverSource{{typeName: "NUMBER", precision: 10, scale: 2, hasInfo: true}},
			want:        schema.Decimal,
			wantDecimal: []int32{10, 2},
		},
		{
			name:    "NUMBER(5)",
			catalog: catalogSource{typeName: "NUMBER", precision: n(5), scale: n(0)},
			drivers: []driverSource{{typeName: "NUMBER", precision: 5, scale: 0, hasInfo: true}},
			want:    schema.Int64,
		},
		{
			// Bare NUMBER: catalog reports NULL precision and scale; the driver
			// substitutes (38, 0xFF).
			name:    "NUMBER",
			catalog: catalogSource{typeName: "NUMBER"},
			drivers: []driverSource{{typeName: "NUMBER", precision: 38, scale: 255, hasInfo: true}},
			want:    schema.BigDecimal,
		},
		{
			// INTEGER is NUMBER(*,0): catalog reports NULL precision with scale
			// 0; the driver reports (38, 0). Both must land on Decimal(38,0).
			name:        "INTEGER",
			catalog:     catalogSource{typeName: "NUMBER", scale: n(0)},
			drivers:     []driverSource{{typeName: "NUMBER", precision: 38, scale: 0, hasInfo: true}},
			want:        schema.Decimal,
			wantDecimal: []int32{38, 0},
		},
		{
			// NUMBER(*,2): catalog reports NULL precision with the declared
			// scale; the driver reports (38, 2).
			name:        "NUMBER(*,2)",
			catalog:     catalogSource{typeName: "NUMBER", scale: n(2)},
			drivers:     []driverSource{{typeName: "NUMBER", precision: 38, scale: 2, hasInfo: true}},
			want:        schema.Decimal,
			wantDecimal: []int32{38, 2},
		},
		{
			// FLOAT: catalog reports DATA_TYPE FLOAT with binary precision and
			// NULL scale; the driver reports NUMBER with the undeclared-scale
			// sentinel.
			name:    "FLOAT",
			catalog: catalogSource{typeName: "FLOAT", precision: n(126)},
			drivers: []driverSource{{typeName: "NUMBER", precision: 38, scale: 255, hasInfo: true}},
			want:    schema.BigDecimal,
		},
		{
			// Negative scale: catalog reports it faithfully; the driver's uint8
			// scale wraps (-2 → 254) and trips the scale > precision sentinel.
			name:    "NUMBER(5,-2)",
			catalog: catalogSource{typeName: "NUMBER", precision: n(5), scale: n(-2)},
			drivers: []driverSource{{typeName: "NUMBER", precision: 5, scale: 254, hasInfo: true}},
			want:    schema.BigDecimal,
		},
		{
			name:    "BINARY_FLOAT",
			catalog: catalogSource{typeName: "BINARY_FLOAT"},
			drivers: []driverSource{{typeName: "IBFloat"}, {typeName: "BFloat"}},
			want:    schema.Float32,
		},
		{
			name:    "BINARY_DOUBLE",
			catalog: catalogSource{typeName: "BINARY_DOUBLE"},
			drivers: []driverSource{{typeName: "IBDouble"}, {typeName: "BDouble"}},
			want:    schema.Float64,
		},
		{
			name:    "RAW",
			catalog: catalogSource{typeName: "RAW"},
			drivers: []driverSource{{typeName: "RAW"}, {typeName: "VarRaw"}},
			want:    schema.ByteArray,
		},
		{
			name:    "LONG RAW",
			catalog: catalogSource{typeName: "LONG RAW"},
			drivers: []driverSource{{typeName: "LongRaw"}, {typeName: "LongVarRaw"}},
			want:    schema.ByteArray,
		},
		{
			// BLOB surfaces as a locator or, with inline LOB fetching, LongRaw.
			name:    "BLOB",
			catalog: catalogSource{typeName: "BLOB"},
			drivers: []driverSource{{typeName: "OCIBlobLocator"}, {typeName: "LongRaw"}},
			want:    schema.ByteArray,
		},
		{
			name:    "VARCHAR2",
			catalog: catalogSource{typeName: "VARCHAR2"},
			drivers: []driverSource{{typeName: "NCHAR"}, {typeName: "VARCHAR"}},
			want:    schema.String,
		},
		{
			name:    "CHAR",
			catalog: catalogSource{typeName: "CHAR"},
			drivers: []driverSource{{typeName: "CHAR"}},
			want:    schema.String,
		},
		{
			// CLOB/NCLOB surface as a locator or, with inline LOB fetching,
			// LongVarChar.
			name:    "CLOB",
			catalog: catalogSource{typeName: "CLOB"},
			drivers: []driverSource{{typeName: "OCIClobLocator"}, {typeName: "LongVarChar"}},
			want:    schema.String,
		},
		{
			name:    "LONG",
			catalog: catalogSource{typeName: "LONG"},
			drivers: []driverSource{{typeName: "LONG"}, {typeName: "LongVarChar"}},
			want:    schema.String,
		},
		{
			name:    "JSON",
			catalog: catalogSource{typeName: "JSON"},
			drivers: []driverSource{{typeName: "TNSType(119)"}},
			want:    schema.Any,
		},
		{
			name:    "XMLTYPE",
			catalog: catalogSource{typeName: "XMLTYPE"},
			drivers: []driverSource{{typeName: "OCIXMLType"}, {typeName: "XMLType"}},
			want:    schema.String,
		},
		{
			name:    "INTERVAL DAY TO SECOND",
			catalog: catalogSource{typeName: "INTERVAL DAY(2) TO SECOND(6)"},
			drivers: []driverSource{{typeName: "IntervalDS_DTY"}},
			want:    schema.String,
		},
		{
			name:    "INTERVAL YEAR TO MONTH",
			catalog: catalogSource{typeName: "INTERVAL YEAR(2) TO MONTH"},
			drivers: []driverSource{{typeName: "IntervalYM_DTY"}},
			want:    schema.String,
		},
		{
			name:    "ROWID",
			catalog: catalogSource{typeName: "ROWID"},
			drivers: []driverSource{{typeName: "ROWID"}},
			want:    schema.String,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, s, hasInfo := catalogNumberInfo(tt.catalog.precision, tt.catalog.scale)
			fromCatalog := columnToCommon("COL", tt.catalog.typeName, p, s, hasInfo)
			assert.Equal(t, tt.want, fromCatalog.Type, "catalog mapping for %s", tt.catalog.typeName)
			if tt.wantDecimal != nil {
				require.NotNil(t, fromCatalog.Logical)
				require.NotNil(t, fromCatalog.Logical.Decimal)
				assert.Equal(t, tt.wantDecimal[0], fromCatalog.Logical.Decimal.Precision, "decimal precision for %s", tt.name)
				assert.Equal(t, tt.wantDecimal[1], fromCatalog.Logical.Decimal.Scale, "decimal scale for %s", tt.name)
			}

			for _, d := range tt.drivers {
				fromDriver := columnToCommon("COL", d.typeName, d.precision, d.scale, d.hasInfo)
				assert.Equalf(t, fromCatalog, fromDriver,
					"schema derived from driver metadata (%q) must equal schema derived from catalog metadata (%q)",
					d.typeName, tt.catalog.typeName)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Schema cache
// ---------------------------------------------------------------------------

func TestSchemaCacheHit(t *testing.T) {
	sc := testSchemaCache(t)
	s := seedCache(t, sc, "S", "T", []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
		{Name: "B", TypeName: "NUMBER", Precision: 10, Scale: 0, HasDecimalSize: true},
		{Name: "C", TypeName: "DATE"},
	})

	ctx := context.Background()
	tbl := replication.UserTable{Schema: "S", Name: "T"}

	// All known subsets are cache hits.
	for _, keys := range [][]string{{"A", "B", "C"}, {"A", "B"}, {"A"}, {}, nil} {
		got, _, err := sc.schemaForEvent(ctx, tbl, keys)
		require.NoError(t, err)
		assert.Equal(t, s, got, "expected cache hit for keys %v", keys)
	}
}

func TestSchemaCacheSubsetKeysNoRefresh(t *testing.T) {
	sc := testSchemaCache(t)
	seedCache(t, sc, "S", "T", []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
		{Name: "B", TypeName: "NUMBER", Precision: 5, Scale: 0, HasDecimalSize: true},
		{Name: "C", TypeName: "DATE"},
	})

	tbl := replication.UserTable{Schema: "S", Name: "T"}

	// [A, B] is a subset of [A, B, C] — should not trigger a re-fetch.
	// Passing nil db proves no DB call is made (would panic on nil).
	got, _, err := sc.schemaForEvent(context.Background(), tbl, []string{"A", "B"})
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestSchemaCacheEmptyKeysNoRefresh(t *testing.T) {
	sc := testSchemaCache(t)
	seedCache(t, sc, "S", "T", []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
	})

	// Empty keys (DELETE event) — always a cache hit.
	got, _, err := sc.schemaForEvent(context.Background(), replication.UserTable{Schema: "S", Name: "T"}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestSchemaCacheSeedFromColumnMeta(t *testing.T) {
	sc := testSchemaCache(t)
	s := seedCache(t, sc, "S", "T", []replication.ColumnMeta{
		{Name: "NAME", TypeName: "VARCHAR2"},
		{Name: "AGE", TypeName: "NUMBER", Precision: 10, Scale: 0, HasDecimalSize: true},
		{Name: "BALANCE", TypeName: "NUMBER", Precision: 18, Scale: 2, HasDecimalSize: true},
	})

	c := parseSchema(t, s)
	assert.Equal(t, "T", c.Name)
	assert.Equal(t, schema.Object, c.Type)
	require.Len(t, c.Children, 3)

	name := childByName(t, c, "NAME")
	assert.Equal(t, schema.String, name.Type)
	assert.True(t, name.Optional)

	age := childByName(t, c, "AGE")
	assert.Equal(t, schema.Int64, age.Type)
	assert.True(t, age.Optional)

	balance := childByName(t, c, "BALANCE")
	assert.Equal(t, schema.Decimal, balance.Type)
	require.NotNil(t, balance.Logical)
	require.NotNil(t, balance.Logical.Decimal)
	assert.Equal(t, int32(18), balance.Logical.Decimal.Precision)
	assert.Equal(t, int32(2), balance.Logical.Decimal.Scale)
	assert.True(t, balance.Optional)
}

func TestSchemaCacheSeedFromColumnMetaOverride(t *testing.T) {
	sc := testSchemaCache(t)
	tbl := replication.UserTable{Schema: "S", Name: "T"}

	// Seed with 2 columns.
	sc.seedFromColumnMeta(tbl, []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
		{Name: "B", TypeName: "NUMBER", Precision: 5, Scale: 0, HasDecimalSize: true},
	})
	s1, _, err := sc.schemaForEvent(context.Background(), tbl, nil)
	require.NoError(t, err)
	c1 := parseSchema(t, s1)
	require.Len(t, c1.Children, 2)

	// Seed again with 3 columns — should override.
	sc.seedFromColumnMeta(tbl, []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
		{Name: "B", TypeName: "NUMBER", Precision: 5, Scale: 0, HasDecimalSize: true},
		{Name: "C", TypeName: "DATE"},
	})
	s2, _, err := sc.schemaForEvent(context.Background(), tbl, nil)
	require.NoError(t, err)
	c2 := parseSchema(t, s2)
	require.Len(t, c2.Children, 3)
}

func TestSeedFromColumnMetaIdentitySkip(t *testing.T) {
	newMeta := func() []replication.ColumnMeta {
		return []replication.ColumnMeta{
			{Name: "A", TypeName: "VARCHAR2"},
			{Name: "B", TypeName: "NUMBER", Precision: 5, Scale: 0, HasDecimalSize: true},
		}
	}

	t.Run("same slice skips rebuild", func(t *testing.T) {
		sc := testSchemaCache(t)
		tbl := replication.UserTable{Schema: "S", Name: "T"}
		meta := newMeta()

		sc.seedFromColumnMeta(tbl, meta)
		tableKey := tbl.Schema + "." + tbl.Name
		first := sc.schemas[tableKey]
		require.NotNil(t, first)

		sc.seedFromColumnMeta(tbl, meta)
		assert.Same(t, first, sc.schemas[tableKey],
			"re-seeding with the same slice must skip the rebuild and leave the cached schema untouched")
	})

	t.Run("different slice with equal content still rebuilds", func(t *testing.T) {
		sc := testSchemaCache(t)
		tbl := replication.UserTable{Schema: "S", Name: "T"}
		meta := newMeta()

		sc.seedFromColumnMeta(tbl, meta)
		tableKey := tbl.Schema + "." + tbl.Name
		first := sc.schemas[tableKey]
		require.NotNil(t, first)

		freshCopy := make([]replication.ColumnMeta, len(meta))
		copy(freshCopy, meta)
		sc.seedFromColumnMeta(tbl, freshCopy)
		assert.NotSame(t, first, sc.schemas[tableKey],
			"re-seeding with a different (even if equal-content) slice must rebuild the cached schema")
	})

	t.Run("sameColumnMeta edge cases", func(t *testing.T) {
		a := newMeta()
		sameSlice := a
		equalContent := make([]replication.ColumnMeta, len(a))
		copy(equalContent, a)
		var emptyA, emptyB []replication.ColumnMeta

		assert.True(t, sameColumnMeta(a, sameSlice), "identical slice header must be reported as same")
		assert.True(t, sameColumnMeta(nil, nil), "two nil slices must be reported as same")
		assert.True(t, sameColumnMeta(emptyA, emptyB), "two nil/empty slices must be reported as same regardless of identity")
		assert.False(t, sameColumnMeta(a, equalContent), "distinct slices with equal content must not be reported as same")
		assert.False(t, sameColumnMeta(a, nil), "non-nil vs nil must not be reported as same")
		assert.False(t, sameColumnMeta(a, a[:1]), "different lengths must not be reported as same")
	})
}

func TestSchemaCacheMultiTable(t *testing.T) {
	sc := testSchemaCache(t)
	s1 := seedCache(t, sc, "S", "T1", []replication.ColumnMeta{
		{Name: "A", TypeName: "VARCHAR2"},
		{Name: "B", TypeName: "NUMBER", Precision: 10, Scale: 0, HasDecimalSize: true},
	})
	s2 := seedCache(t, sc, "S", "T2", []replication.ColumnMeta{
		{Name: "X", TypeName: "DATE"},
		{Name: "Y", TypeName: "BLOB"},
		{Name: "Z", TypeName: "BINARY_FLOAT"},
	})

	c1 := parseSchema(t, s1)
	c2 := parseSchema(t, s2)

	assert.Equal(t, "T1", c1.Name)
	require.Len(t, c1.Children, 2)

	assert.Equal(t, "T2", c2.Name)
	require.Len(t, c2.Children, 3)

	assert.NotEqual(t, c1.Name, c2.Name)
}

func TestSchemaRoundTrip(t *testing.T) {
	sc := testSchemaCache(t)
	s := seedCache(t, sc, "MYSCHEMA", "EVENTS", []replication.ColumnMeta{
		{Name: "ID", TypeName: "NUMBER", Precision: 10, Scale: 0, HasDecimalSize: true},
		{Name: "NAME", TypeName: "VARCHAR2"},
		{Name: "CREATED_AT", TypeName: "TIMESTAMP"},
		{Name: "PAYLOAD", TypeName: "JSON"},
		{Name: "DATA", TypeName: "BLOB"},
		{Name: "SCORE", TypeName: "BINARY_DOUBLE"},
	})

	c := parseSchema(t, s)
	assert.Equal(t, "EVENTS", c.Name)
	require.Len(t, c.Children, 6)

	expected := map[string]schema.CommonType{
		"ID":         schema.Int64,
		"NAME":       schema.String,
		"CREATED_AT": schema.Timestamp,
		"PAYLOAD":    schema.Any,
		"DATA":       schema.ByteArray,
		"SCORE":      schema.Float64,
	}
	for name, wantType := range expected {
		child := childByName(t, c, name)
		assert.Equal(t, wantType, child.Type, "field %s", name)
		assert.True(t, child.Optional, "field %s should be optional", name)
	}
}

// ---------------------------------------------------------------------------
// Streaming value coercion
// ---------------------------------------------------------------------------

func TestCoerceStreamingValues(t *testing.T) {
	log := service.NewLoggerFromSlog(slog.Default())

	tests := []struct {
		name string
		data map[string]any
		info *columnTypeInfo
		want map[string]any
	}{
		{
			name: "int64 coercion",
			data: map[string]any{"age": "42"},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"age": {Type: schema.Int64}}},
			want: map[string]any{"age": int64(42)},
		},
		{
			name: "float64 coercion",
			data: map[string]any{"price": "3.14"},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"price": {Type: schema.Float64}}},
			want: map[string]any{"price": float64(3.14)},
		},
		{
			name: "float32 produces float64",
			data: map[string]any{"ratio": "1.5"},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"ratio": {Type: schema.Float32}}},
			want: map[string]any{"ratio": float64(1.5)},
		},
		{
			name: "json.Number float coerced to float64",
			data: map[string]any{"score": json.Number("1.5")},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"score": {Type: schema.Float64}}},
			want: map[string]any{"score": float64(1.5)},
		},
		{
			name: "json.Number float32 coerced to float64",
			data: map[string]any{"ratio": json.Number("3.14")},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"ratio": {Type: schema.Float32}}},
			want: map[string]any{"ratio": float64(3.14)},
		},
		{
			name: "json.Number int coerced to int64",
			data: map[string]any{"id": json.Number("42")},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"id": {Type: schema.Int64}}},
			want: map[string]any{"id": int64(42)},
		},
		{
			name: "decimal string canonicalised at declared scale",
			data: map[string]any{"amount": "12345.6789"},
			info: &columnTypeInfo{
				colTypes: map[string]schema.Common{
					"amount": {
						Type: schema.Decimal,
						Logical: &schema.LogicalParams{
							Decimal: &schema.DecimalParams{Precision: 10, Scale: 5},
						},
					},
				},
			},
			want: map[string]any{"amount": "12345.67890"},
		},
		{
			name: "big decimal string canonicalised at natural scale",
			data: map[string]any{"amount": "12345.67890"},
			info: &columnTypeInfo{
				colTypes: map[string]schema.Common{
					"amount": {Type: schema.BigDecimal},
				},
			},
			want: map[string]any{"amount": "12345.67890"},
		},
		{
			// Regression: a bare integer literal (int64, as produced by the
			// redo-log converter) for a Decimal column must be canonicalised to
			// a string rather than leaking as a JSON number that downstream Avro
			// string-field encoding rejects.
			name: "decimal int64 canonicalised to string",
			data: map[string]any{"amount": int64(2)},
			info: &columnTypeInfo{
				colTypes: map[string]schema.Common{
					"amount": {
						Type: schema.Decimal,
						Logical: &schema.LogicalParams{
							Decimal: &schema.DecimalParams{Precision: 10, Scale: 2},
						},
					},
				},
			},
			want: map[string]any{"amount": "2.00"},
		},
		{
			name: "big decimal int64 canonicalised to string",
			data: map[string]any{"amount": int64(678)},
			info: &columnTypeInfo{
				colTypes: map[string]schema.Common{
					"amount": {Type: schema.BigDecimal},
				},
			},
			want: map[string]any{"amount": "678"},
		},
		{
			name: "varchar2 string not coerced",
			data: map[string]any{"name": "hello"},
			info: &columnTypeInfo{
				colTypes: map[string]schema.Common{"name": {Type: schema.String}},
			},
			want: map[string]any{"name": "hello"},
		},
		{
			name: "already typed int64 left alone",
			data: map[string]any{"id": int64(42)},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"id": {Type: schema.Int64}}},
			want: map[string]any{"id": int64(42)},
		},
		{
			name: "nil value stays nil",
			data: map[string]any{"col": nil},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"col": {Type: schema.Int64}}},
			want: map[string]any{"col": nil},
		},
		{
			name: "unknown column unchanged",
			data: map[string]any{"mystery": "value"},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{}},
			want: map[string]any{"mystery": "value"},
		},
		{
			name: "nil info is no-op",
			data: map[string]any{"age": "99"},
			info: nil,
			want: map[string]any{"age": "99"},
		},
		{
			name: "invalid int64 string preserved",
			data: map[string]any{"count": "not-a-number"},
			info: &columnTypeInfo{colTypes: map[string]schema.Common{"count": {Type: schema.Int64}}},
			want: map[string]any{"count": "not-a-number"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coerceStreamingValues(tt.data, tt.info, log)
			assert.Equal(t, tt.want, tt.data)
		})
	}
}

func TestCoerceStreamingValuesColumnTypeInfoFromCache(t *testing.T) {
	// Verify that seedFromColumnMeta produces correct columnTypeInfo
	// that can be used for coercion.
	sc := testSchemaCache(t)
	log := service.NewLoggerFromSlog(slog.Default())

	tbl := replication.UserTable{Schema: "S", Name: "T"}
	sc.seedFromColumnMeta(tbl, []replication.ColumnMeta{
		{Name: "ID", TypeName: "NUMBER", Precision: 10, Scale: 0, HasDecimalSize: true},
		{Name: "AMOUNT", TypeName: "NUMBER", Precision: 20, Scale: 5, HasDecimalSize: true},
		{Name: "NAME", TypeName: "VARCHAR2"},
		{Name: "SCORE", TypeName: "BINARY_FLOAT"},
	})

	_, typeInfo, err := sc.schemaForEvent(t.Context(), tbl, nil)
	require.NoError(t, err)
	require.NotNil(t, typeInfo)

	// ID: NUMBER(10,0) → Int64
	assert.Equal(t, schema.Int64, typeInfo.colTypes["ID"].Type)
	// AMOUNT: NUMBER(20,5) → Decimal(20,5)
	assert.Equal(t, schema.Decimal, typeInfo.colTypes["AMOUNT"].Type)
	require.NotNil(t, typeInfo.colTypes["AMOUNT"].Logical)
	require.NotNil(t, typeInfo.colTypes["AMOUNT"].Logical.Decimal)
	assert.Equal(t, int32(20), typeInfo.colTypes["AMOUNT"].Logical.Decimal.Precision)
	assert.Equal(t, int32(5), typeInfo.colTypes["AMOUNT"].Logical.Decimal.Scale)
	// NAME: VARCHAR2 → String
	assert.Equal(t, schema.String, typeInfo.colTypes["NAME"].Type)
	// SCORE: BINARY_FLOAT → Float32
	assert.Equal(t, schema.Float32, typeInfo.colTypes["SCORE"].Type)

	// Verify coercion works with this typeInfo
	data := map[string]any{
		"ID":     "42",
		"AMOUNT": "12345.67890",
		"NAME":   "hello",
		"SCORE":  "1.5",
	}
	coerceStreamingValues(data, typeInfo, log)

	assert.Equal(t, int64(42), data["ID"])
	assert.Equal(t, "12345.67890", data["AMOUNT"])
	assert.Equal(t, "hello", data["NAME"])
	assert.Equal(t, float64(1.5), data["SCORE"])
}

// TestSnapshotScannerSchemaParity pins the three case-sensitive enumerations
// of driver type-name spellings to each other: the schema mapping
// (columnToCommon), the snapshot scan-destination switch
// (replication.SnapshotScanDest), and the lob_enabled filter
// (replication.IsLOBTypeName). These encode the same fact — what kind of
// column a driver spelling denotes — in three hand-maintained lists, and they
// have drifted twice before (LongRaw missing from the schema mapping,
// LongVarRaw missing from the LOB filter). This table is the single place a
// new spelling must be added; any list it doesn't reach fails here.
func TestSnapshotScannerSchemaParity(t *testing.T) {
	type row struct {
		spelling         string
		precision, scale int64
		hasDecimal       bool
		wantSchema       schema.CommonType
		wantScanDest     string // %T of the scan destination
		wantLOB          bool
	}
	rows := []row{
		// Binary family. RAW/VarRaw are plain columns; the rest are LOBs.
		{spelling: "RAW", wantSchema: schema.ByteArray, wantScanDest: "*sql.Null[[]uint8]"},
		{spelling: "VarRaw", wantSchema: schema.ByteArray, wantScanDest: "*sql.Null[[]uint8]"},
		{spelling: "LongRaw", wantSchema: schema.ByteArray, wantScanDest: "*sql.Null[[]uint8]", wantLOB: true},
		{spelling: "LongVarRaw", wantSchema: schema.ByteArray, wantScanDest: "*sql.Null[[]uint8]", wantLOB: true},
		{spelling: "OCIBlobLocator", wantSchema: schema.ByteArray, wantScanDest: "*sql.Null[[]uint8]", wantLOB: true},

		// Temporal family.
		{spelling: "DATE", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TIMESTAMP", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TimeStampDTY", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TimeStampTZ", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TimeStampTZ_DTY", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TimeStampLTZ_DTY", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},
		{spelling: "TimeStampeLTZ", wantSchema: schema.Timestamp, wantScanDest: "*sql.NullTime"},

		// NUMBER family: the scan destination depends on the classified type.
		{spelling: "NUMBER", precision: 5, scale: 0, hasDecimal: true, wantSchema: schema.Int64, wantScanDest: "*sql.Null[int64]"},
		{spelling: "NUMBER", precision: 10, scale: 2, hasDecimal: true, wantSchema: schema.Decimal, wantScanDest: "*sql.NullString"},
		{spelling: "NUMBER", precision: 38, scale: 255, hasDecimal: true, wantSchema: schema.BigDecimal, wantScanDest: "*sql.NullString"},

		// Binary floats.
		{spelling: "IBFloat", wantSchema: schema.Float32, wantScanDest: "*sql.Null[float64]"},
		{spelling: "BFloat", wantSchema: schema.Float32, wantScanDest: "*sql.Null[float64]"},
		{spelling: "IBDouble", wantSchema: schema.Float64, wantScanDest: "*sql.Null[float64]"},
		{spelling: "BDouble", wantSchema: schema.Float64, wantScanDest: "*sql.Null[float64]"},

		// Character LOB family.
		{spelling: "CLOB", wantSchema: schema.String, wantScanDest: "*sql.NullString", wantLOB: true},
		{spelling: "NCLOB", wantSchema: schema.String, wantScanDest: "*sql.NullString", wantLOB: true},
		{spelling: "LONG", wantSchema: schema.String, wantScanDest: "*sql.NullString", wantLOB: true},
		{spelling: "LongVarChar", wantSchema: schema.String, wantScanDest: "*sql.NullString", wantLOB: true},
		{spelling: "OCIClobLocator", wantSchema: schema.String, wantScanDest: "*sql.NullString", wantLOB: true},

		// JSON (TNS type 119 has no stringer entry in go-ora v2.9.0).
		{spelling: "JSON", wantSchema: schema.Any, wantScanDest: "*sql.NullString"},
		{spelling: "TNSType(119)", wantSchema: schema.Any, wantScanDest: "*sql.NullString"},

		// Plain character types and unknowns take the default string path.
		{spelling: "NCHAR", wantSchema: schema.String, wantScanDest: "*sql.Null[string]"},
		{spelling: "VARCHAR", wantSchema: schema.String, wantScanDest: "*sql.Null[string]"},
		{spelling: "CHAR", wantSchema: schema.String, wantScanDest: "*sql.Null[string]"},
		{spelling: "ROWID", wantSchema: schema.String, wantScanDest: "*sql.Null[string]"},
		{spelling: "TNSType(121)", wantSchema: schema.String, wantScanDest: "*sql.Null[string]"},
	}

	for _, r := range rows {
		t.Run(fmt.Sprintf("%s(%d,%d)", r.spelling, r.precision, r.scale), func(t *testing.T) {
			common := columnToCommon("COL", r.spelling, r.precision, r.scale, r.hasDecimal)
			assert.Equal(t, r.wantSchema, common.Type, "schema class for %q", r.spelling)

			dest, mapper := replication.SnapshotScanDest(r.spelling, "COL", r.precision, r.scale, r.hasDecimal)
			assert.Equal(t, r.wantScanDest, fmt.Sprintf("%T", dest), "scan destination for %q", r.spelling)
			assert.NotNil(t, mapper, "mapper for %q", r.spelling)

			assert.Equal(t, r.wantLOB, replication.IsLOBTypeName(r.spelling), "lob_enabled filtering for %q", r.spelling)
		})
	}
}
