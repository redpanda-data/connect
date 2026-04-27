// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

func TestParseIcebergTypeString(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"boolean", "boolean", false},
		{"int", "int", false},
		{"long", "long", false},
		{"float", "float", false},
		{"double", "double", false},
		{"string", "string", false},
		{"binary", "binary", false},
		{"date", "date", false},
		{"time", "time", false},
		{"timestamp", "timestamp", false},
		{"timestamptz", "timestamptz", false},
		{"uuid", "uuid", false},
		// Case insensitivity
		{"Boolean", "boolean", false},
		{"STRING", "string", false},
		{"Long", "long", false},
		// Whitespace
		{" string ", "string", false},
		{"  int  ", "int", false},
		// Decimal
		{"decimal(10, 2)", "decimal(10, 2)", false},
		{"decimal(38,0)", "decimal(38, 0)", false},
		{"Decimal(5, 3)", "decimal(5, 3)", false},
		// Fixed
		{"fixed[16]", "fixed[16]", false},
		{"fixed[4]", "fixed[4]", false},
		{"Fixed[32]", "fixed[32]", false},
		// Errors
		{"unknown", "", true},
		{"", "", true},
		{"decimal()", "", true},
		{"fixed[]", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseIcebergTypeString(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Type())
		})
	}
}

func TestCommonTypeToIcebergType(t *testing.T) {
	tests := []struct {
		name    string
		common  schema.Common
		want    string
		wantErr bool
	}{
		{"Boolean", schema.Common{Type: schema.Boolean}, "boolean", false},
		{"Int32", schema.Common{Type: schema.Int32}, "int", false},
		{"Int64", schema.Common{Type: schema.Int64}, "long", false},
		{"Float32", schema.Common{Type: schema.Float32}, "float", false},
		{"Float64", schema.Common{Type: schema.Float64}, "double", false},
		{"String", schema.Common{Type: schema.String}, "string", false},
		{"ByteArray", schema.Common{Type: schema.ByteArray}, "binary", false},
		{"Timestamp", schema.Common{Type: schema.Timestamp}, "timestamptz", false},
		{"Any fallback", schema.Common{Type: schema.Any}, "string", false},
		{"Null fallback", schema.Common{Type: schema.Null}, "string", false},
		{"Object", schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "name", Type: schema.String},
				{Name: "age", Type: schema.Int32},
			},
		}, "struct", false},
		{"Array", schema.Common{
			Type:     schema.Array,
			Children: []schema.Common{{Type: schema.String}},
		}, "list", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commonTypeToIcebergType(&tt.common)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Type())
		})
	}
}

func TestFindCommonField(t *testing.T) {
	root := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "name", Type: schema.String},
			{Name: "parent", Type: schema.Object, Children: []schema.Common{
				{Name: "child", Type: schema.Int64},
			}},
			{Name: "items", Type: schema.Array, Children: []schema.Common{
				{Type: schema.Object, Children: []schema.Common{
					{Name: "sku", Type: schema.String},
				}},
			}},
		},
	}

	tests := []struct {
		name  string
		path  icebergx.Path
		want  bool
		wType schema.CommonType
	}{
		{
			name:  "flat field",
			path:  icebergx.Path{{Kind: icebergx.PathField, Name: "name"}},
			want:  true,
			wType: schema.String,
		},
		{
			name: "nested field",
			path: icebergx.Path{
				{Kind: icebergx.PathField, Name: "parent"},
				{Kind: icebergx.PathField, Name: "child"},
			},
			want:  true,
			wType: schema.Int64,
		},
		{
			name: "array element field",
			path: icebergx.Path{
				{Kind: icebergx.PathField, Name: "items"},
				{Kind: icebergx.PathListElement},
				{Kind: icebergx.PathField, Name: "sku"},
			},
			want:  true,
			wType: schema.String,
		},
		{
			name:  "missing field",
			path:  icebergx.Path{{Kind: icebergx.PathField, Name: "nonexistent"}},
			want:  false,
			wType: 0,
		},
		{
			name:  "empty path",
			path:  icebergx.Path{},
			want:  false,
			wType: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := findCommonField(root, tt.path)
			assert.Equal(t, tt.want, found)
			if found {
				assert.Equal(t, tt.wType, got.Type)
			}
		})
	}
}

func TestIsPrimitiveType(t *testing.T) {
	assert.True(t, isPrimitiveType(iceberg.StringType{}))
	assert.True(t, isPrimitiveType(iceberg.BooleanType{}))
	assert.True(t, isPrimitiveType(iceberg.Int32Type{}))
	assert.True(t, isPrimitiveType(iceberg.Float64Type{}))
	assert.False(t, isPrimitiveType(&iceberg.StructType{}))
	assert.False(t, isPrimitiveType(&iceberg.ListType{}))
	assert.False(t, isPrimitiveType(&iceberg.MapType{}))
}

func mustParseBloblang(t *testing.T, mapping string) *bloblang.Executor {
	t.Helper()
	exec, err := bloblang.Parse(mapping)
	require.NoError(t, err)
	return exec
}

func TestTypeResolverResolveTypeForAddColumn(t *testing.T) {
	t.Run("default inference", func(t *testing.T) {
		r := newTypeResolver("", nil, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"name": "hello"})

		field := NewUnknownFieldError(nil, "name", "hello")
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "string", got.Type())

		field = NewUnknownFieldError(nil, "count", 42)
		got, err = r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "long", got.Type())
	})

	t.Run("schema_metadata override", func(t *testing.T) {
		r := newTypeResolver("test_schema", nil, nil)

		commonSchema := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "count", Type: schema.Int64},
			},
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})
		msg.MetaSetMut("test_schema", commonSchema.ToAny())

		field := NewUnknownFieldError(nil, "count", 42)
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "long", got.Type(), "should use Int64 from schema metadata, not Float64 from inference")
	})

	t.Run("bloblang mapping override", func(t *testing.T) {
		exec := mustParseBloblang(t, `root = "long"`)
		r := newTypeResolver("", exec, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})

		field := NewUnknownFieldError(nil, "count", 42)
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "long", got.Type())
	})

	t.Run("schema_metadata then mapping", func(t *testing.T) {
		exec := mustParseBloblang(t, `root = "long"`)
		r := newTypeResolver("test_schema", exec, nil)

		commonSchema := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "count", Type: schema.Int32},
			},
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})
		msg.MetaSetMut("test_schema", commonSchema.ToAny())

		field := NewUnknownFieldError(nil, "count", 42)
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		// Mapping sees inferred_type="int" (from schema_metadata) and overrides to "long"
		assert.Equal(t, "long", got.Type())
	})

	t.Run("struct type skips mapping", func(t *testing.T) {
		exec := mustParseBloblang(t, `root = "string"`)
		r := newTypeResolver("test_schema", exec, nil)

		commonSchema := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "nested", Type: schema.Object, Children: []schema.Common{
					{Name: "x", Type: schema.String},
				}},
			},
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"nested": map[string]any{"x": "hello"}})
		msg.MetaSetMut("test_schema", commonSchema.ToAny())

		field := NewUnknownFieldError(nil, "nested", map[string]any{"x": "hello"})
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "struct", got.Type(), "struct type should skip the bloblang mapping")
	})

	t.Run("mapping receives inferred_type", func(t *testing.T) {
		exec := mustParseBloblang(t, `root = if this.inferred_type == "long" { "decimal(10, 2)" } else { this.inferred_type }`)
		r := newTypeResolver("", exec, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42, "name": "test"})

		// Numeric → inferred as "long" → mapping converts to "decimal(10, 2)"
		field := NewUnknownFieldError(nil, "count", 42)
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "decimal(10, 2)", got.Type())

		// String → inferred as "string" → mapping passes through
		field = NewUnknownFieldError(nil, "name", "test")
		got, err = r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err)
		assert.Equal(t, "string", got.Type())
	})

	t.Run("schema_metadata configured but missing on message", func(t *testing.T) {
		r := newTypeResolver("test_schema", nil, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})

		field := NewUnknownFieldError(nil, "count", 42)
		got, err := r.resolveTypeForAddColumn(field, msg, "ns", "tbl")
		require.NoError(t, err, "should not error when schema_metadata is missing from message")
		assert.Equal(t, "long", got.Type(), "should fall back to inference")
	})
}

func TestTypeResolverResolveTypeForCreateTable(t *testing.T) {
	t.Run("default inference", func(t *testing.T) {
		r := newTypeResolver("", nil, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"name": "hello"})

		got, err := r.resolveTypeForCreateTable("name", "hello", msg, "ns", "tbl", newTypeInferrer())
		require.NoError(t, err)
		assert.Equal(t, "string", got.Type())
	})

	t.Run("nil value returns nil", func(t *testing.T) {
		r := newTypeResolver("", nil, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{})

		got, err := r.resolveTypeForCreateTable("name", nil, msg, "ns", "tbl", newTypeInferrer())
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("schema_metadata override", func(t *testing.T) {
		r := newTypeResolver("test_schema", nil, nil)

		commonSchema := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "count", Type: schema.Int64},
			},
		}
		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})
		msg.MetaSetMut("test_schema", commonSchema.ToAny())

		got, err := r.resolveTypeForCreateTable("count", 42, msg, "ns", "tbl", newTypeInferrer())
		require.NoError(t, err)
		assert.Equal(t, "long", got.Type())
	})

	t.Run("bloblang mapping override", func(t *testing.T) {
		exec := mustParseBloblang(t, `root = "long"`)
		r := newTypeResolver("", exec, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})

		got, err := r.resolveTypeForCreateTable("count", 42, msg, "ns", "tbl", newTypeInferrer())
		require.NoError(t, err)
		assert.Equal(t, "long", got.Type())
	})

	t.Run("schema_metadata configured but missing on message", func(t *testing.T) {
		r := newTypeResolver("test_schema", nil, nil)

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{"count": 42})

		got, err := r.resolveTypeForCreateTable("count", 42, msg, "ns", "tbl", newTypeInferrer())
		require.NoError(t, err, "should not error when schema_metadata is missing from message")
		assert.Equal(t, "long", got.Type(), "should fall back to inference")
	})

	t.Run("shared allocator produces unique field IDs across nested structs", func(t *testing.T) {
		r := newTypeResolver("", nil, nil)
		ti := newTypeInferrer()

		record := map[string]any{
			"id": int64(1),
			"source": map[string]any{
				"account_id": "ACC-123",
				"bank_code":  "SWIFT-XYZ",
			},
			"destination": map[string]any{
				"account_id": "ACC-456",
				"bank_code":  "SWIFT-ABC",
			},
		}

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(record)

		// Build fields the same way buildSchemaWithResolver does.
		var allIDs []int
		for name, value := range record {
			fieldType, err := r.resolveTypeForCreateTable(name, value, msg, "ns", "tbl", ti)
			require.NoError(t, err)
			if fieldType == nil {
				continue
			}
			topID := ti.allocateFieldID()
			allIDs = append(allIDs, topID)

			// Collect nested field IDs from struct types.
			if st, ok := fieldType.(*iceberg.StructType); ok {
				for _, f := range st.FieldList {
					allIDs = append(allIDs, f.ID)
				}
			}
		}

		// Every ID must be unique — this is the regression test for the collision bug.
		seen := make(map[int]bool, len(allIDs))
		for _, id := range allIDs {
			assert.False(t, seen[id], "duplicate field ID %d — nested struct IDs collide with top-level", id)
			seen[id] = true
		}
		assert.GreaterOrEqual(t, len(allIDs), 7, "expected at least 7 fields (1 primitive + 2 structs with 2 fields each)")
	})
}
