// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication_test

import (
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSnapshotFilterKeys(t *testing.T) {
	t.Run("uppercases keys, leaves queries untouched", func(t *testing.T) {
		normalized, err := replication.NormalizeSnapshotFilterKeys(map[string]string{
			"testdb.foo": "select * from testdb.foo",
			"TestDB.Bar": "SELECT * FROM TESTDB.BAR",
		})
		require.NoError(t, err)
		require.Equal(t, map[string]string{
			"TESTDB.FOO": "select * from testdb.foo",
			"TESTDB.BAR": "SELECT * FROM TESTDB.BAR",
		}, normalized)
	})

	t.Run("empty map", func(t *testing.T) {
		normalized, err := replication.NormalizeSnapshotFilterKeys(map[string]string{})
		require.NoError(t, err)
		require.Empty(t, normalized)
	})

	t.Run("case-insensitive duplicate keys rejected", func(t *testing.T) {
		_, err := replication.NormalizeSnapshotFilterKeys(map[string]string{
			"testdb.foo": "SELECT * FROM TESTDB.FOO",
			"TESTDB.FOO": "SELECT * FROM TESTDB.FOO WHERE ID > 1",
		})
		require.ErrorContains(t, err, "specified more than once")
	})
}

func TestValidateSnapshotFilters(t *testing.T) {
	tests := []struct {
		name       string
		filters    map[string]string
		errContain string
	}{
		{
			name:    "star select",
			filters: map[string]string{"TESTDB.USERS": "SELECT * FROM TESTDB.USERS"},
		},
		{
			name:    "star select with where",
			filters: map[string]string{"TESTDB.PRODUCTS": "SELECT * FROM TESTDB.PRODUCTS WHERE ID > 1000"},
		},
		{
			name:    "column list with where",
			filters: map[string]string{"TESTDB.FOO": "SELECT ID, NAME FROM TESTDB.FOO WHERE ID > 500"},
		},
		{
			name:    "double quoted identifiers",
			filters: map[string]string{"TESTDB.FOO": `SELECT * FROM "TESTDB"."FOO"`},
		},
		{
			name:    "bare alias",
			filters: map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO f WHERE f.ID > 500"},
		},
		{
			name:    "AS alias",
			filters: map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO AS f WHERE f.ID > 500"},
		},
		{
			name:    "unqualified table",
			filters: map[string]string{"FOO": "SELECT * FROM FOO"},
		},
		{
			name:    "lowercase keywords and identifiers",
			filters: map[string]string{"TESTDB.FOO": "select * from testdb.foo"},
		},
		{
			name:    "subquery in select list",
			filters: map[string]string{"TESTDB.FOO": "SELECT (SELECT MAX(ID) FROM OTHER) FROM TESTDB.FOO"},
		},
		{
			name:    "subquery in where clause",
			filters: map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO WHERE ID IN (SELECT ID FROM OTHER)"},
		},
		{
			name:    "trailing semicolon tolerated",
			filters: map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO;"},
		},
		{
			name: "multiple valid filters",
			filters: map[string]string{
				"TESTDB.FOO":  "SELECT * FROM TESTDB.FOO",
				"TESTDB.FOO2": "SELECT ID, NAME FROM TESTDB.FOO2 WHERE ID > 500",
			},
		},
		{
			name:    "empty filters",
			filters: map[string]string{},
		},
		{
			name:       "not a select statement",
			filters:    map[string]string{"TESTDB.FOO": "DELETE FROM TESTDB.FOO"},
			errContain: "must be a SELECT statement",
		},
		{
			name:       "malformed sql missing from",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FRM TESTDB.FOO"},
			errContain: "is not valid SQL",
		},
		{
			name:       "comma joined tables",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO, TESTDB.BAR"},
			errContain: "must query exactly one table",
		},
		{
			name:       "explicit join",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO JOIN TESTDB.BAR ON 1=1"},
			errContain: "must reference a simple table name",
		},
		{
			name:       "subquery as from table",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM (SELECT * FROM TESTDB.FOO) t"},
			errContain: "must reference a simple table name",
		},
		{
			name:       "union combining a second select",
			filters:    map[string]string{"TESTDB.FOO": "SELECT ID FROM TESTDB.FOO UNION SELECT PASSWORD FROM SECRETS"},
			errContain: "UNION",
		},
		{
			name:       "intersect combining a second select",
			filters:    map[string]string{"TESTDB.FOO": "SELECT ID FROM TESTDB.FOO INTERSECT SELECT PASSWORD FROM SECRETS"},
			errContain: "INTERSECT",
		},
		{
			name:       "minus combining a second select",
			filters:    map[string]string{"TESTDB.FOO": "SELECT ID FROM TESTDB.FOO MINUS SELECT PASSWORD FROM SECRETS"},
			errContain: "MINUS",
		},
		{
			name:       "statement chaining via semicolon",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO; DROP TABLE TESTDB.FOO"},
			errContain: "multiple statements",
		},
		{
			name:       "unbalanced parentheses",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.FOO WHERE (ID > 1"},
			errContain: "unbalanced parentheses",
		},
		{
			name:       "table does not match filter key",
			filters:    map[string]string{"TESTDB.FOO": "SELECT * FROM TESTDB.BAR"},
			errContain: "does not match the table referenced in the query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := replication.ValidateSnapshotFilters(tt.filters)
			if tt.errContain == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.errContain)
		})
	}
}
