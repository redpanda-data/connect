// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSOQLEscape(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 28, 15, 30, 45, 0, time.UTC)

	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, "null"},
		{"string", "Acme", "'Acme'"},
		{"string with single quote", "O'Brien", `'O\'Brien'`},
		{"string with backslash", `back\slash`, `'back\\slash'`},
		{"string with both", `it's a \\test`, `'it\'s a \\\\test'`},
		{"empty string", "", "''"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", 42, "42"},
		{"int64 negative", int64(-7), "-7"},
		{"float64", 3.14, "3.14"},
		{"time UTC", ts, "2026-04-28T15:30:45Z"},
		{"time non-UTC normalises", ts.In(time.FixedZone("CET", 7200)), "2026-04-28T15:30:45Z"},
		{"bytes", []byte("Acme"), "'Acme'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := soqlEscape(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSOQLEscapeUnsupported(t *testing.T) {
	t.Parallel()
	_, err := soqlEscape(map[string]any{"x": 1})
	require.Error(t, err)
}

func TestSOQLQuoteString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want string
	}{
		{"", "''"},
		{"plain", "'plain'"},
		{"O'Brien", `'O\'Brien'`},
		{`back\slash`, `'back\\slash'`},
		{`it's \\ done`, `'it\'s \\\\ done'`},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, soqlQuoteString(tc.in))
		})
	}
}

func TestSubstituteSOQLPlaceholders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		where string
		args  []any
		want  string
		err   bool
	}{
		{
			name:  "no placeholders, no args",
			where: "Status = 'Active'",
			want:  "Status = 'Active'",
		},
		{
			name:  "single string placeholder",
			where: "Name = ?",
			args:  []any{"Acme"},
			want:  "Name = 'Acme'",
		},
		{
			name:  "two placeholders mixed",
			where: "Status = ? AND CreatedDate > ?",
			args:  []any{"Active", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
			want:  "Status = 'Active' AND CreatedDate > 2026-01-01T00:00:00Z",
		},
		{
			name:  "question mark inside literal stays put",
			where: "Description = '?' AND Id = ?",
			args:  []any{"abc"},
			want:  "Description = '?' AND Id = 'abc'",
		},
		{
			name:  "IN list",
			where: "OwnerId IN (?, ?)",
			args:  []any{"a", "b"},
			want:  "OwnerId IN ('a', 'b')",
		},
		{
			name:  "escaped quote inside literal",
			where: `Name = 'O\'Brien' AND Id = ?`,
			args:  []any{"abc"},
			want:  `Name = 'O\'Brien' AND Id = 'abc'`,
		},
		{
			name:  "fewer args than placeholders",
			where: "A = ? AND B = ?",
			args:  []any{"x"},
			err:   true,
		},
		{
			name:  "more args than placeholders",
			where: "A = ?",
			args:  []any{"x", "y"},
			err:   true,
		},
		{
			name:  "no placeholders but args provided",
			where: "A = 1",
			args:  []any{"x"},
			err:   true,
		},
		{
			name:  "empty where, empty args",
			where: "",
			want:  "",
		},
		{
			name:  "unsupported arg type bubbles up",
			where: "A = ?",
			args:  []any{map[string]any{"k": "v"}},
			err:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := substituteSOQLPlaceholders(tc.where, tc.args)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestBuildSOQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		object  string
		columns []string
		where   string
		prefix  string
		suffix  string
		args    []any
		want    string
		err     bool
	}{
		{
			name:    "minimal SELECT",
			object:  "Account",
			columns: []string{"Id", "Name"},
			want:    "SELECT Id, Name FROM Account",
		},
		{
			name:    "WHERE with no placeholders",
			object:  "Account",
			columns: []string{"Id"},
			where:   "Status = 'Active'",
			want:    "SELECT Id FROM Account WHERE Status = 'Active'",
		},
		{
			name:    "WHERE with placeholder substitution",
			object:  "Account",
			columns: []string{"Id"},
			where:   "Id = ?",
			args:    []any{"001"},
			want:    "SELECT Id FROM Account WHERE Id = '001'",
		},
		{
			name:    "suffix only",
			object:  "Account",
			columns: []string{"Id"},
			suffix:  "ORDER BY Id LIMIT 10",
			want:    "SELECT Id FROM Account ORDER BY Id LIMIT 10",
		},
		{
			name:    "prefix and suffix",
			object:  "Account",
			columns: []string{"Id"},
			prefix:  "  /*+ HINT */  ",
			suffix:  "  ORDER BY Id  ",
			want:    "/*+ HINT */ SELECT Id FROM Account ORDER BY Id",
		},
		{
			name:    "multiple columns join with comma-space",
			object:  "Contact",
			columns: []string{"Id", "Account.Name", "Owner.Email"},
			want:    "SELECT Id, Account.Name, Owner.Email FROM Contact",
		},
		{
			name:    "WHERE substitution error propagates",
			object:  "Account",
			columns: []string{"Id"},
			where:   "Id = ?",
			args:    nil,
			err:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildSOQL(tc.object, tc.columns, tc.where, tc.prefix, tc.suffix, tc.args)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
