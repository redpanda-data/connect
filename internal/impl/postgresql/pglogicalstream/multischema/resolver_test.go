// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package multischema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlobToLike(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"public", "public"},
		{"tenant_*", "tenant!_%"},
		{"*", "%"},
		{"tenant_a", "tenant!_a"},
		{"100%", "100!%"},
		{"a!b", "a!!b"},
		{"multi_*_end", "multi!_%!_end"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, globToLike(tt.input))
		})
	}
}

func TestSchemaPatternToLike(t *testing.T) {
	tests := []struct {
		pattern       string
		expected      string
		caseSensitive bool
		errContains   string
	}{
		// Unquoted glob patterns — folded to lower-case, '*' → '%', '_' escaped,
		// matched case-insensitively regardless of how the matched schema was created.
		{pattern: "public", expected: "public"},
		{pattern: "tenant_*", expected: "tenant!_%"},
		{pattern: "*", expected: "%"},
		{pattern: "schema_1", expected: "schema!_1"},
		// Upper-case is folded: TENANT_* matches the same rows as tenant_*.
		{pattern: "TENANT_*", expected: "tenant!_%"},
		// Quoted exact identifier — case preserved, no wildcard expansion,
		// matched case-sensitively.
		{pattern: `"MySchema"`, expected: "MySchema", caseSensitive: true},
		{pattern: `"schema_1"`, expected: "schema!_1", caseSensitive: true},
		{pattern: `"has%bang!"`, expected: "has!%bang!!", caseSensitive: true},
		// Unterminated quoted identifier → error.
		{pattern: `"bad`, errContains: "invalid quoted schema identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got, caseSensitive, err := schemaPatternToLike(tt.pattern)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
			assert.Equal(t, tt.caseSensitive, caseSensitive)
		})
	}
}

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"MySchema", "MySchema"},
		{"schema_1", "schema!_1"},
		{"100%", "100!%"},
		{"bang!bang", "bang!!bang"},
		{"has_a%b!c", "has!_a!%b!!c"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, escapeLike(tt.input))
		})
	}
}

func TestSchemaMatchesExcludePattern(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		pattern     string
		expected    bool
		errContains string
	}{
		// Unquoted patterns - case-insensitive, '*' as wildcard.
		{name: "unquoted exact match", schema: `"tenant_a"`, pattern: "tenant_a", expected: true},
		{name: "unquoted exact no match", schema: `"tenant_a"`, pattern: "tenant_b", expected: false},
		{name: "unquoted glob match", schema: `"tenant_test_x"`, pattern: "tenant_test_*", expected: true},
		{name: "unquoted glob no match", schema: `"tenant_prod_x"`, pattern: "tenant_test_*", expected: false},
		{name: "bare wildcard matches everything", schema: `"anything"`, pattern: "*", expected: true},
		// Case-folding: unquoted patterns and unquoted-origin schema names both
		// fold to lower-case, mirroring PostgreSQL's identifier folding.
		{name: "case-insensitive exact match", schema: `"tenant_a"`, pattern: "TENANT_A", expected: true},
		{name: "case-insensitive glob match", schema: `"Tenant_Test_X"`, pattern: "tenant_test_*", expected: true},
		// Quoted patterns - exact, case-sensitive, no wildcard expansion.
		{name: "quoted exact case-sensitive match", schema: `"MySchema"`, pattern: `"MySchema"`, expected: true},
		{name: "quoted exact case mismatch does not match", schema: `"MySchema"`, pattern: `"myschema"`, expected: false},
		{name: "quoted pattern does not expand wildcard", schema: `"tenant_a"`, pattern: `"tenant_*"`, expected: false},
		// Errors - only from a malformed pattern, never from the candidate
		// schema name, since that's always freshly quoted by resolveSchemas.
		{name: "unterminated quoted pattern errors", schema: `"tenant_a"`, pattern: `"unterminated`, errContains: "invalid quoted schema identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := schemaMatchesExcludePattern(tt.schema, tt.pattern)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestDiffSchemaSets(t *testing.T) {
	tests := []struct {
		name            string
		previous        []string
		current         []string
		expectedAdded   []string
		expectedRemoved []string
	}{
		// nil previous is the first resolution, not drift - Resolve's caller
		// is responsible for ignoring this result in that case (see the
		// doc comment on diffSchemaSets), but the function itself still
		// reports every current schema as "added" since it has nothing to
		// compare against.
		{
			name:          "nil previous, empty current",
			previous:      nil,
			current:       nil,
			expectedAdded: nil, expectedRemoved: nil,
		},
		{
			name:          "nil previous, non-empty current",
			previous:      nil,
			current:       []string{`"a"`, `"b"`},
			expectedAdded: []string{`"a"`, `"b"`}, expectedRemoved: nil,
		},
		{
			name:          "no change",
			previous:      []string{`"a"`, `"b"`},
			current:       []string{`"a"`, `"b"`},
			expectedAdded: nil, expectedRemoved: nil,
		},
		{
			name:          "added only",
			previous:      []string{`"a"`},
			current:       []string{`"a"`, `"b"`},
			expectedAdded: []string{`"b"`}, expectedRemoved: nil,
		},
		{
			name:          "removed only",
			previous:      []string{`"a"`, `"b"`},
			current:       []string{`"a"`},
			expectedAdded: nil, expectedRemoved: []string{`"b"`},
		},
		{
			name:          "added and removed",
			previous:      []string{`"a"`, `"b"`},
			current:       []string{`"b"`, `"c"`},
			expectedAdded: []string{`"c"`}, expectedRemoved: []string{`"a"`},
		},
		{
			name:          "everything replaced",
			previous:      []string{`"a"`, `"b"`},
			current:       []string{`"c"`, `"d"`},
			expectedAdded: []string{`"c"`, `"d"`}, expectedRemoved: []string{`"a"`, `"b"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added, removed := diffSchemaSets(tt.previous, tt.current)
			assert.Equal(t, tt.expectedAdded, added, "added")
			assert.Equal(t, tt.expectedRemoved, removed, "removed")
		})
	}
}
