// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestFindCaseOnlyDuplicate covers the create-time guard that prevents an
// inbound record with two keys differing only in case from being committed
// as two separate iceberg columns under case-insensitive matching, which
// would either be rejected by the catalog or corrupt the schema.
func TestFindCaseOnlyDuplicate(t *testing.T) {
	t.Run("no duplicates returns false", func(t *testing.T) {
		record := map[string]any{"id": 1, "name": "alice", "email": "x@y.z"}
		_, _, ok := findCaseOnlyDuplicate(record)
		assert.False(t, ok)
	})

	t.Run("uppercase and lowercase duplicate", func(t *testing.T) {
		record := map[string]any{"id": 1, "ID": 2}
		a, b, ok := findCaseOnlyDuplicate(record)
		assert.True(t, ok)
		// Order is non-deterministic over map iteration; assert the pair.
		assert.ElementsMatch(t, []string{"id", "ID"}, []string{a, b})
	})

	t.Run("mixed-case duplicate", func(t *testing.T) {
		record := map[string]any{"User_Id": 1, "user_id": 2}
		a, b, ok := findCaseOnlyDuplicate(record)
		assert.True(t, ok)
		pair := []string{strings.ToLower(a), strings.ToLower(b)}
		assert.Equal(t, []string{"user_id", "user_id"}, pair)
	})

	t.Run("empty record returns false", func(t *testing.T) {
		_, _, ok := findCaseOnlyDuplicate(map[string]any{})
		assert.False(t, ok)
	})
}

// TestBuildSchemaWithResolverPreservesColumnOrder verifies that columns in the
// resulting Iceberg schema appear in the order defined by the schema registry
// metadata, not in Go map iteration order.
func TestBuildSchemaWithResolverPreservesColumnOrder(t *testing.T) {
	router := &Router{
		caseSensitive: true,
		resolver:      newTypeResolver("schema_key", nil, true, nil),
	}

	// Build a schema.Common with fields in a specific order that differs from
	// alphabetical to make the test deterministic and meaningful.
	schemaMeta := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "zebra", Type: schema.String},
			{Name: "alpha", Type: schema.String},
			{Name: "mango", Type: schema.String},
		},
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_key", schemaMeta.ToAny())

	record := map[string]any{
		"zebra": "z-value",
		"alpha": "a-value",
		"mango": "m-value",
	}

	icebergSchema, err := router.buildSchemaWithResolver(record, msg, tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	fields := icebergSchema.Fields()
	require.Len(t, fields, 3)
	assert.Equal(t, "zebra", fields[0].Name)
	assert.Equal(t, "alpha", fields[1].Name)
	assert.Equal(t, "mango", fields[2].Name)
}

// TestBuildSchemaWithResolverAppendsRecordOnlyFields verifies that record fields
// not declared in the schema metadata are still included in the iceberg schema.
// Without this, an enriched record with extra keys would silently lose columns
// the moment metadata-driven ordering kicks in.
func TestBuildSchemaWithResolverAppendsRecordOnlyFields(t *testing.T) {
	router := &Router{
		caseSensitive: true,
		resolver:      newTypeResolver("schema_key", nil, true, nil),
	}

	schemaMeta := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "zebra", Type: schema.String},
			{Name: "alpha", Type: schema.String},
		},
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_key", schemaMeta.ToAny())

	record := map[string]any{
		"zebra":  "z",
		"alpha":  "a",
		"extra2": "e2",
		"extra1": "e1",
	}

	icebergSchema, err := router.buildSchemaWithResolver(record, msg, tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	fields := icebergSchema.Fields()
	require.Len(t, fields, 4)
	// Metadata order first.
	assert.Equal(t, "zebra", fields[0].Name)
	assert.Equal(t, "alpha", fields[1].Name)
	// Record-only keys appended in sorted order for determinism.
	assert.Equal(t, "extra1", fields[2].Name)
	assert.Equal(t, "extra2", fields[3].Name)
}

// TestBuildSchemaWithResolverCaseInsensitiveOrdering verifies that when
// caseSensitive is false, schema metadata field names match record keys with
// case folding, and the iceberg column name preserves the record's original
// casing. Without case-folded matching, lowercase canonical metadata against
// mixed-case record keys would drop every column.
func TestBuildSchemaWithResolverCaseInsensitiveOrdering(t *testing.T) {
	router := &Router{
		caseSensitive: false,
		resolver:      newTypeResolver("schema_key", nil, false, nil),
	}

	schemaMeta := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "zebra", Type: schema.String},
			{Name: "alpha", Type: schema.String},
		},
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_key", schemaMeta.ToAny())

	record := map[string]any{
		"Zebra": "z",
		"ALPHA": "a",
	}

	icebergSchema, err := router.buildSchemaWithResolver(record, msg, tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	fields := icebergSchema.Fields()
	require.Len(t, fields, 2)
	assert.Equal(t, "Zebra", fields[0].Name)
	assert.Equal(t, "ALPHA", fields[1].Name)
}

// TestBuildSchemaWithResolverCaseInsensitiveWithRecordOnlyFields combines
// case-insensitive metadata-driven ordering with record-only leftover fields
// to exercise the full merge path: lowercase canonical metadata, mixed-case
// record keys, plus extras absent from metadata that should appear sorted at
// the end with their original casing preserved.
func TestBuildSchemaWithResolverCaseInsensitiveWithRecordOnlyFields(t *testing.T) {
	router := &Router{
		caseSensitive: false,
		resolver:      newTypeResolver("schema_key", nil, false, nil),
	}

	schemaMeta := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "alpha", Type: schema.String},
			{Name: "beta", Type: schema.String},
		},
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_key", schemaMeta.ToAny())

	record := map[string]any{
		"Alpha": "a", // case-folded match for metadata "alpha"
		"BETA":  "b", // case-folded match for metadata "beta"
		"gamma": "g", // record-only
		"Delta": "d", // record-only
	}

	icebergSchema, err := router.buildSchemaWithResolver(record, msg, tableKey{namespace: "ns", table: "t"})
	require.NoError(t, err)

	fields := icebergSchema.Fields()
	require.Len(t, fields, 4)
	// Metadata-ordered first, with the record's original casing preserved.
	assert.Equal(t, "Alpha", fields[0].Name)
	assert.Equal(t, "BETA", fields[1].Name)
	// Record-only keys appended in bytewise sorted order ("Delta" precedes
	// "gamma" because uppercase 'D' < lowercase 'g').
	assert.Equal(t, "Delta", fields[2].Name)
	assert.Equal(t, "gamma", fields[3].Name)
}

// TestBuildSchemaWithResolverRejectsCaseOnlyDuplicateMetadata verifies that
// metadata declaring two top-level children differing only in case is rejected
// at create time when case-insensitive matching is in effect, mirroring the
// same guard applied to nested struct metadata. Without this, the override
// path would silently dedupe and admit one child arbitrarily.
func TestBuildSchemaWithResolverRejectsCaseOnlyDuplicateMetadata(t *testing.T) {
	router := &Router{
		caseSensitive: false,
		resolver:      newTypeResolver("schema_key", nil, false, nil),
	}

	schemaMeta := schema.Common{
		Type: schema.Object,
		Children: []schema.Common{
			{Name: "alpha", Type: schema.String},
			{Name: "ALPHA", Type: schema.String},
		},
	}

	msg := service.NewMessage(nil)
	msg.MetaSetMut("schema_key", schemaMeta.ToAny())

	record := map[string]any{
		"alpha": "a",
	}

	_, err := router.buildSchemaWithResolver(record, msg, tableKey{namespace: "ns", table: "t"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "differ only in case")
}
