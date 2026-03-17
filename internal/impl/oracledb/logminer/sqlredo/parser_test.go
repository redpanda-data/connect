// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

func TestParseTest(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantNewValues map[string]any
		wantOldValues map[string]any
		wantErr       bool
	}{
		{
			name: "INSERT with quoted identifiers",
			sql:  `insert into "MYAPP"."CUSTOMERS" ("ID","NAME","EMAIL") values ('1','John Doe','john@example.com')`,
			wantNewValues: map[string]any{
				"ID":    "1",
				"NAME":  "John Doe",
				"EMAIL": "john@example.com",
			},
		},
		{
			name: "UPDATE with double quotes",
			sql:  `update "MYAPP"."CUSTOMERS" set "NAME" = 'Jane Doe', "EMAIL" = 'jane@example.com' where "ID" = '1' and "NAME" = 'John Doe'`,
			wantNewValues: map[string]any{
				"NAME":  "Jane Doe",
				"EMAIL": "jane@example.com",
			},
			wantOldValues: map[string]any{
				"ID":   "1",
				"NAME": "John Doe",
			},
		},
		{
			name: "DELETE with double quotes",
			sql:  `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "NAME" = 'John Doe'`,
			wantOldValues: map[string]any{
				"ID":   "1",
				"NAME": "John Doe",
			},
		},
		{
			name: "INSERT with escaped single quotes",
			sql:  `insert into "MYAPP"."MESSAGES" ("ID","TEXT") values ('1','It''s a test')`,
			wantNewValues: map[string]any{
				"ID":   "1",
				"TEXT": "It's a test",
			},
		},
		{
			name: "INSERT with double quotes inside string",
			sql:  `insert into "MYAPP"."MESSAGES" ("ID","TEXT") values ('1','He said "Hello"')`,
			wantNewValues: map[string]any{
				"ID":   "1",
				"TEXT": `He said "Hello"`,
			},
		},
		{
			name: "INSERT with Oracle functions",
			sql:  `insert into "MYAPP"."ORDERS" ("ID","ORDER_DATE") values ('100',TO_DATE('2020-01-15','YYYY-MM-DD'))`,
			wantNewValues: map[string]any{
				"ID":         "100",
				"ORDER_DATE": "TO_DATE('2020-01-15', 'YYYY-MM-DD')",
			},
		},
		{
			// Regression: a single quote inside a double-quoted Oracle identifier (e.g.
			// "O'Brien") must not toggle inSingleQuote. Without the fix the parser treats
			// all characters after the quote as inside a string literal, corrupting the
			// column names and values that follow.
			name: "INSERT with single quote inside double-quoted table name",
			sql:  `insert into "MYAPP"."O'Brien" ("ID","NAME") values ('1','Alice')`,
			wantNewValues: map[string]any{
				"ID":   "1",
				"NAME": "Alice",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlredo.ParseSQLCommand(tt.sql)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			newValues, oldValues, err := sqlredo.ExtractValuesFromAST(stmt)
			require.NoError(t, err)

			assert.Equal(t, tt.wantNewValues, newValues)
			assert.Equal(t, tt.wantOldValues, oldValues)
		})
	}
}
