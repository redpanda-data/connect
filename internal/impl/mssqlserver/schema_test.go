// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package mssqlserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func TestMssqlTypeNameToCommonType(t *testing.T) {
	tests := []struct {
		typeName string
		expected schema.CommonType
	}{
		// Integer types
		{"TINYINT", schema.Int64},
		{"SMALLINT", schema.Int64},
		{"INT", schema.Int64},
		{"BIGINT", schema.Int64},
		// Lowercase / mixed case is normalised
		{"tinyint", schema.Int64},
		{"int", schema.Int64},
		// Floating-point types
		{"FLOAT", schema.Float64},
		{"REAL", schema.Float32},
		// Decimal / money types: preserve precision as string
		{"DECIMAL", schema.String},
		{"NUMERIC", schema.String},
		{"MONEY", schema.String},
		{"SMALLMONEY", schema.String},
		// Boolean
		{"BIT", schema.Boolean},
		// Timestamp types
		{"DATETIME", schema.Timestamp},
		{"DATETIME2", schema.Timestamp},
		{"SMALLDATETIME", schema.Timestamp},
		{"DATETIMEOFFSET", schema.Timestamp},
		// Date-only and time-only → String (consistent with PostgreSQL)
		{"DATE", schema.String},
		{"TIME", schema.String},
		// Binary types
		{"BINARY", schema.ByteArray},
		{"VARBINARY", schema.ByteArray},
		{"VARBINARY(MAX)", schema.ByteArray},
		{"IMAGE", schema.ByteArray},
		// TIMESTAMP/ROWVERSION is a binary counter, not datetime
		{"TIMESTAMP", schema.ByteArray},
		{"ROWVERSION", schema.ByteArray},
		// String types (default catch-all)
		{"CHAR", schema.String},
		{"VARCHAR", schema.String},
		{"NCHAR", schema.String},
		{"NVARCHAR", schema.String},
		{"NVARCHAR(MAX)", schema.String},
		{"XML", schema.String},
		{"UNIQUEIDENTIFIER", schema.String},
		// Unknown type → String
		{"UNKNOWN_TYPE", schema.String},
		{"", schema.String},
	}

	for _, tt := range tests {
		t.Run(tt.typeName, func(t *testing.T) {
			got := mssqlTypeNameToCommonType(tt.typeName)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// TestMssqlTypeNameToCommonTypeAllMSSQLTypes verifies every MSSQL type
// present in the all_data_types integration test table is mapped correctly.
func TestMssqlTypeNameToCommonTypeAllMSSQLTypes(t *testing.T) {
	typeExpectations := map[string]schema.CommonType{
		"TINYINT":        schema.Int64,
		"SMALLINT":       schema.Int64,
		"INT":            schema.Int64,
		"BIGINT":         schema.Int64,
		"DECIMAL":        schema.String,
		"NUMERIC":        schema.String,
		"FLOAT":          schema.Float64,
		"REAL":           schema.Float32,
		"DATE":           schema.String,
		"DATETIME":       schema.Timestamp,
		"DATETIME2":      schema.Timestamp,
		"SMALLDATETIME":  schema.Timestamp,
		"TIME":           schema.String,
		"DATETIMEOFFSET": schema.Timestamp,
		"CHAR":           schema.String,
		"VARCHAR":        schema.String,
		"NCHAR":          schema.String,
		"NVARCHAR":       schema.String,
		"BINARY":         schema.ByteArray,
		"VARBINARY":      schema.ByteArray,
		"VARBINARY(MAX)": schema.ByteArray,
		"BIT":            schema.Boolean,
		"XML":            schema.String,
		"MONEY":          schema.String,
		"SMALLMONEY":     schema.String,
		"TIMESTAMP":      schema.ByteArray,
		"ROWVERSION":     schema.ByteArray,
	}

	for typeName, expectedType := range typeExpectations {
		t.Run(typeName, func(t *testing.T) {
			got := mssqlTypeNameToCommonType(typeName)
			assert.Equal(t, expectedType, got, "unexpected mapping for MSSQL type %q", typeName)
		})
	}
}

// TestSchemaCache verifies the in-memory schema cache on batchPublisher.
func TestSchemaCache(t *testing.T) {
	b := &batchPublisher{tableSchemas: make(map[string]any)}

	// No column types → cache miss, returns nil
	assert.Nil(t, b.getOrComputeTableSchema("users", nil, nil))

	// Pre-seed the cache directly (simulates a prior call with real column types)
	sentinel := map[string]any{"name": "users", "type": "OBJECT"}
	b.tableSchemas["users"] = sentinel

	// Should return the cached value without re-computing
	got := b.getOrComputeTableSchema("users", nil, nil)
	require.NotNil(t, got)
	assert.Equal(t, sentinel, got)

	// An unknown table with no types still returns nil
	assert.Nil(t, b.getOrComputeTableSchema("other", nil, nil))
}
