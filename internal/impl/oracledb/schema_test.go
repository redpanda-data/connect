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
	"encoding/json"
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

		{"JSON", schema.Any},
		{"json", schema.Any},

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
		want      schema.CommonType
	}{
		{"integer precision 10", 10, 0, true, schema.Int64},
		{"integer precision 18 boundary", 18, 0, true, schema.Int64},
		{"precision 19 exceeds int64", 19, 0, true, schema.String},
		{"precision 38 max oracle", 38, 0, true, schema.String},
		{"fractional scale 2", 10, 2, true, schema.String},
		{"bare NUMBER no info", 0, 0, false, schema.String},
		{"NUMBER(0) edge case", 0, 0, true, schema.String},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, oracleNumberToCommonType(tt.precision, tt.scale, tt.hasInfo))
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
	assert.Equal(t, schema.String, balance.Type)
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
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"age": schema.Int64}},
			want: map[string]any{"age": int64(42)},
		},
		{
			name: "float64 coercion",
			data: map[string]any{"price": "3.14"},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"price": schema.Float64}},
			want: map[string]any{"price": float64(3.14)},
		},
		{
			name: "float32 produces float64",
			data: map[string]any{"ratio": "1.5"},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"ratio": schema.Float32}},
			want: map[string]any{"ratio": float64(1.5)},
		},
		{
			name: "json.Number float coerced to float64",
			data: map[string]any{"score": json.Number("1.5")},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"score": schema.Float64}},
			want: map[string]any{"score": float64(1.5)},
		},
		{
			name: "json.Number float32 coerced to float64",
			data: map[string]any{"ratio": json.Number("3.14")},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"ratio": schema.Float32}},
			want: map[string]any{"ratio": float64(3.14)},
		},
		{
			name: "json.Number int coerced to int64",
			data: map[string]any{"id": json.Number("42")},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"id": schema.Int64}},
			want: map[string]any{"id": int64(42)},
		},
		{
			name: "numeric string NUMBER column to json.Number",
			data: map[string]any{"amount": "12345.67890"},
			info: &columnTypeInfo{
				colTypes:    map[string]schema.CommonType{"amount": schema.String},
				numericCols: map[string]struct{}{"amount": {}},
			},
			want: map[string]any{"amount": json.Number("12345.67890")},
		},
		{
			name: "varchar2 string not coerced",
			data: map[string]any{"name": "hello"},
			info: &columnTypeInfo{
				colTypes:    map[string]schema.CommonType{"name": schema.String},
				numericCols: map[string]struct{}{},
			},
			want: map[string]any{"name": "hello"},
		},
		{
			name: "already typed int64 left alone",
			data: map[string]any{"id": int64(42)},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"id": schema.Int64}},
			want: map[string]any{"id": int64(42)},
		},
		{
			name: "nil value stays nil",
			data: map[string]any{"col": nil},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"col": schema.Int64}},
			want: map[string]any{"col": nil},
		},
		{
			name: "unknown column unchanged",
			data: map[string]any{"mystery": "value"},
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{}},
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
			info: &columnTypeInfo{colTypes: map[string]schema.CommonType{"count": schema.Int64}},
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
	assert.Equal(t, schema.Int64, typeInfo.colTypes["ID"])
	// AMOUNT: NUMBER(20,5) → String + numericCols
	assert.Equal(t, schema.String, typeInfo.colTypes["AMOUNT"])
	_, isNumeric := typeInfo.numericCols["AMOUNT"]
	assert.True(t, isNumeric, "AMOUNT should be in numericCols")
	// NAME: VARCHAR2 → String but NOT in numericCols
	assert.Equal(t, schema.String, typeInfo.colTypes["NAME"])
	_, nameNumeric := typeInfo.numericCols["NAME"]
	assert.False(t, nameNumeric, "NAME should not be in numericCols")
	// SCORE: BINARY_FLOAT → Float32
	assert.Equal(t, schema.Float32, typeInfo.colTypes["SCORE"])

	// Verify coercion works with this typeInfo
	data := map[string]any{
		"ID":     "42",
		"AMOUNT": "12345.67890",
		"NAME":   "hello",
		"SCORE":  "1.5",
	}
	coerceStreamingValues(data, typeInfo, log)

	assert.Equal(t, int64(42), data["ID"])
	assert.Equal(t, json.Number("12345.67890"), data["AMOUNT"])
	assert.Equal(t, "hello", data["NAME"])
	assert.Equal(t, float64(1.5), data["SCORE"])
}
