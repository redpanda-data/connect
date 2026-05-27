// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"context"
	"database/sql"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestHanaTypeToCommonType(t *testing.T) {
	tests := []struct {
		hanaType string
		want     schema.CommonType
	}{
		{"TINYINT", schema.Int64},
		{"SMALLINT", schema.Int64},
		{"INT", schema.Int64},
		{"INTEGER", schema.Int64},
		{"BIGINT", schema.Int64},
		{"FLOAT", schema.Float64},
		{"DOUBLE", schema.Float64},
		{"REAL", schema.Float32},
		{"BOOLEAN", schema.Boolean},
		{"DATE", schema.Timestamp},
		{"TIME", schema.Timestamp},
		{"TIMESTAMP", schema.Timestamp},
		{"SECONDDATE", schema.Timestamp},
		{"VARBINARY", schema.ByteArray},
		{"BLOB", schema.ByteArray},
		{"VARCHAR", schema.String},
		{"NVARCHAR", schema.String},
		{"CHAR", schema.String},
		{"NCHAR", schema.String},
		{"ALPHANUM", schema.String},
		{"CLOB", schema.String},
		{"NCLOB", schema.String},
		{"UNKNOWN_XYZ", schema.String},
		{"", schema.String},
	}

	for _, tc := range tests {
		t.Run(tc.hanaType, func(t *testing.T) {
			assert.Equal(t, tc.want, hanaTypeToCommonType(tc.hanaType))
		})
	}
}

func TestIsDecimalType(t *testing.T) {
	tests := []struct {
		hanaType string
		want     bool
	}{
		{"DECIMAL", true},
		{"NUMERIC", true},
		{"INTEGER", false},
		{"BIGINT", false},
		{"DOUBLE", false},
		{"VARCHAR", false},
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.hanaType, func(t *testing.T) {
			assert.Equal(t, tc.want, isDecimalType(tc.hanaType))
		})
	}
}

func TestHanaDecimalToCommon(t *testing.T) {
	validInt := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	invalid := sql.NullInt64{Valid: false}

	tests := []struct {
		name      string
		precision sql.NullInt64
		scale     sql.NullInt64
		wantType  schema.CommonType
	}{
		{
			name:      "scale=0 precision=10 -> Int64",
			precision: validInt(10),
			scale:     validInt(0),
			wantType:  schema.Int64,
		},
		{
			name:      "scale=0 precision=18 -> Int64",
			precision: validInt(18),
			scale:     validInt(0),
			wantType:  schema.Int64,
		},
		{
			name:      "scale=0 precision=19 -> Decimal (not Int64, within Avro range)",
			precision: validInt(19),
			scale:     validInt(0),
			wantType:  schema.Decimal,
		},
		{
			name:      "scale=4 precision=20 -> Decimal",
			precision: validInt(20),
			scale:     validInt(4),
			wantType:  schema.Decimal,
		},
		{
			name:      "scale=2 precision=38 -> Decimal (Avro max)",
			precision: validInt(38),
			scale:     validInt(2),
			wantType:  schema.Decimal,
		},
		{
			name:      "scale not valid -> BigDecimal",
			precision: validInt(20),
			scale:     invalid,
			wantType:  schema.BigDecimal,
		},
		{
			name:      "precision not valid -> BigDecimal",
			precision: invalid,
			scale:     validInt(4),
			wantType:  schema.BigDecimal,
		},
		{
			name:      "both not valid -> BigDecimal",
			precision: invalid,
			scale:     invalid,
			wantType:  schema.BigDecimal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := hanaDecimalToCommon("col", tc.precision, tc.scale)
			assert.Equal(t, tc.wantType, got.Type)
			assert.Equal(t, "col", got.Name)
		})
	}
}

func TestSchemaCacheEmptySchemaName(t *testing.T) {
	// nil db: any query would panic; early return on empty schemaName must prevent that.
	cache := newSchemaCache(nil, service.MockResources().Logger())
	got, err := cache.schemaForEvent(context.Background(), "", "TABLE", []string{"ID"})
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestSchemaCacheCachesResults(t *testing.T) {
	// Seed the cache directly; a nil DB would panic on a real query.
	cache := newSchemaCache(nil, service.MockResources().Logger())

	seeded := &cachedSchema{
		schemaVal: map[string]any{"type": "record", "name": "MY_TABLE"},
		keys:      map[string]struct{}{"ID": {}, "NAME": {}},
	}
	cache.entries["MY_SCHEMA.MY_TABLE"] = seeded

	// Both known columns — no DB query issued.
	got, err := cache.schemaForEvent(context.Background(), "MY_SCHEMA", "MY_TABLE", []string{"ID", "NAME"})
	require.NoError(t, err)
	assert.Equal(t, seeded.schemaVal, got)

	// Second call — still cached.
	got2, err := cache.schemaForEvent(context.Background(), "MY_SCHEMA", "MY_TABLE", []string{"ID"})
	require.NoError(t, err)
	assert.Equal(t, seeded.schemaVal, got2)
}
