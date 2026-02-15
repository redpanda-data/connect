// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInferIcebergType(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		wantType string // Use type string for comparison
		wantNil  bool
		wantErr  bool
	}{
		{
			name:    "nil value",
			value:   nil,
			wantNil: true,
		},
		{
			name:     "string",
			value:    "hello",
			wantType: "string",
		},
		{
			name:     "bool",
			value:    true,
			wantType: "boolean",
		},
		{
			name:     "int",
			value:    42,
			wantType: "double",
		},
		{
			name:     "int64",
			value:    int64(42),
			wantType: "double",
		},
		{
			name:     "float64",
			value:    3.14,
			wantType: "double",
		},
		{
			name:     "time.Time",
			value:    time.Now(),
			wantType: "timestamptz",
		},
		{
			name:     "[]byte",
			value:    []byte("binary data"),
			wantType: "binary",
		},
		{
			name:     "[]any with strings",
			value:    []any{"a", "b", "c"},
			wantType: "list",
		},
		{
			name:     "map[string]any",
			value:    map[string]any{"name": "test", "count": 42},
			wantType: "struct",
		},
		{
			name:     "nested struct",
			value:    map[string]any{"user": map[string]any{"name": "alice", "age": 30}},
			wantType: "struct",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InferIcebergType(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if tt.wantNil {
				assert.Nil(t, got)
				return
			}

			require.NotNil(t, got)
			assert.Equal(t, tt.wantType, got.Type())
		})
	}
}

func TestBuildSchemaFromRecord(t *testing.T) {
	t.Run("simple record", func(t *testing.T) {
		record := map[string]any{
			"id":   42,
			"name": "test",
			"flag": true,
		}

		schema, err := BuildSchemaFromRecord(record)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// Should have 3 fields
		assert.Len(t, schema.Fields(), 3)

		// All fields should be optional
		for _, field := range schema.Fields() {
			assert.False(t, field.Required, "field %s should be optional", field.Name)
		}
	})

	t.Run("nested record", func(t *testing.T) {
		record := map[string]any{
			"user": map[string]any{
				"name":  "alice",
				"email": "alice@example.com",
			},
			"items": []any{
				map[string]any{"sku": "ABC", "qty": 2},
			},
		}

		schema, err := BuildSchemaFromRecord(record)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// Should have 2 top-level fields
		assert.Len(t, schema.Fields(), 2)
	})

	t.Run("record with nil values", func(t *testing.T) {
		record := map[string]any{
			"name":    "test",
			"unknown": nil, // Should be skipped
		}

		schema, err := BuildSchemaFromRecord(record)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// Should only have 1 field (nil field is skipped)
		assert.Len(t, schema.Fields(), 1)
		assert.Equal(t, "name", schema.Fields()[0].Name)
	})

	t.Run("empty record", func(t *testing.T) {
		record := map[string]any{}

		schema, err := BuildSchemaFromRecord(record)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// Should have 0 fields
		assert.Empty(t, schema.Fields())
	})

	t.Run("record with timestamp", func(t *testing.T) {
		now := time.Now()
		record := map[string]any{
			"event":     "test",
			"timestamp": now,
		}

		schema, err := BuildSchemaFromRecord(record)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// Find the timestamp field
		var tsField *iceberg.NestedField
		for _, f := range schema.Fields() {
			if f.Name == "timestamp" {
				tsField = &f
				break
			}
		}
		require.NotNil(t, tsField)
		assert.Equal(t, "timestamptz", tsField.Type.Type())
	})
}

func TestInferIcebergTypeForAddColumn(t *testing.T) {
	t.Run("nil defaults to string", func(t *testing.T) {
		typ, err := InferIcebergTypeForAddColumn(nil)
		require.NoError(t, err)
		assert.Equal(t, "string", typ.Type())
	})

	t.Run("non-nil uses InferIcebergType", func(t *testing.T) {
		typ, err := InferIcebergTypeForAddColumn(42)
		require.NoError(t, err)
		assert.Equal(t, "double", typ.Type())
	})
}
