// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

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
		pattern     string
		expected    string
		errContains string
	}{
		// Unquoted glob patterns — folded to lower-case, '*' → '%', '_' escaped.
		{"public", "public", ""},
		{"tenant_*", "tenant!_%", ""},
		{"*", "%", ""},
		{"schema_1", "schema!_1", ""},
		// Upper-case is folded: TENANT_* matches the same rows as tenant_*.
		{"TENANT_*", "tenant!_%", ""},
		// Quoted exact identifier — case preserved, no wildcard expansion.
		{`"MySchema"`, "MySchema", ""},
		{`"schema_1"`, "schema!_1", ""},
		{`"has%bang!"`, "has!%bang!!", ""},
		// Unterminated quoted identifier → error.
		{`"bad`, "", "invalid quoted schema identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			got, err := schemaPatternToLike(tt.pattern)
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
