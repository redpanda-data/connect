// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"encoding/json"
	"testing"
	"time"

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
		// Integer types → int or long
		{
			name:     "int",
			value:    42,
			wantType: "long",
		},
		{
			name:     "int8",
			value:    int8(42),
			wantType: "int",
		},
		{
			name:     "int16",
			value:    int16(42),
			wantType: "int",
		},
		{
			name:     "int32",
			value:    int32(42),
			wantType: "int",
		},
		{
			name:     "int64",
			value:    int64(42),
			wantType: "long",
		},
		{
			name:     "uint8",
			value:    uint8(42),
			wantType: "int",
		},
		{
			name:     "uint16",
			value:    uint16(42),
			wantType: "int",
		},
		{
			name:     "uint32",
			value:    uint32(42),
			wantType: "long",
		},
		{
			name:     "uint64",
			value:    uint64(42),
			wantType: "long",
		},
		// Float types → float or double
		{
			name:     "float32",
			value:    float32(3.14),
			wantType: "float",
		},
		{
			name:     "float64",
			value:    3.14,
			wantType: "double",
		},
		// json.Number → always double to avoid silent truncation during schema evolution
		{
			name:     "json.Number integer",
			value:    json.Number("42"),
			wantType: "double",
		},
		{
			name:     "json.Number float",
			value:    json.Number("3.14"),
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

func TestInferIcebergTypeForAddColumn(t *testing.T) {
	t.Run("nil defaults to string", func(t *testing.T) {
		typ, err := InferIcebergTypeForAddColumn(nil)
		require.NoError(t, err)
		assert.Equal(t, "string", typ.Type())
	})

	t.Run("non-nil int uses long", func(t *testing.T) {
		typ, err := InferIcebergTypeForAddColumn(42)
		require.NoError(t, err)
		assert.Equal(t, "long", typ.Type())
	})
}
