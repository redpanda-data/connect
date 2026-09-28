// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo_test

import (
	"encoding/json"
	"testing"
	"time"

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
				"ORDER_DATE": "TO_DATE('2020-01-15','YYYY-MM-DD')",
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
		{
			name: "INSERT with NULL value includes column as nil",
			sql:  `insert into "MYAPP"."SAMPLES" ("ID","SPEC_CH_ID","SPEC_CKC_ID") values ('1',NULL,NULL)`,
			wantNewValues: map[string]any{
				"ID":          "1",
				"SPEC_CH_ID":  nil,
				"SPEC_CKC_ID": nil,
			},
		},
		{
			name: "UPDATE without WHERE clause",
			sql:  `update "MYAPP"."TEST" set "COL1" = '1', "COL2" = NULL, "COL3" = 'Hello'`,
			wantNewValues: map[string]any{
				"COL1": "1",
				"COL2": nil,
				"COL3": "Hello",
			},
			wantOldValues: map[string]any{},
		},
		{
			name:          "DELETE without WHERE clause",
			sql:           `delete from "MYAPP"."TEST"`,
			wantOldValues: map[string]any{},
		},
		// Oracle LogMiner can emit table aliases in SQL_REDO
		{
			name: "UPDATE with table alias in SET clause",
			sql:  `update "MYAPP"."TEST" a set a."COL1" = '1', a."COL2" = NULL, a."COL3" = 'Hello'`,
			wantNewValues: map[string]any{
				"COL1": "1",
				"COL2": nil,
				"COL3": "Hello",
			},
			wantOldValues: map[string]any{},
		},
		{
			name: "DELETE with table alias in WHERE clause",
			sql:  `delete from "MYAPP"."TEST" a where a."COL1" = '1' and a."COL2" = '2'`,
			wantOldValues: map[string]any{
				"COL1": "1",
				"COL2": "2",
			},
		},
		// Oracle LogMiner emits ROWID-based WHERE clauses for tables without a primary key
		// or without supplemental logging enabled.
		{
			name: "DELETE with ROWID in WHERE clause",
			sql:  `delete from "CMMSAPP"."FWEQPEQUIPMENT" where ROWID = 'AAABCDefgHIJKLM'`,
			wantOldValues: map[string]any{
				"ROWID": "AAABCDefgHIJKLM",
			},
		},
		{
			name: "UPDATE with ROWID in WHERE clause",
			sql:  `update "CMMSAPP"."FWEQPEQUIPMENT" set "STATUS" = 'ACTIVE' where ROWID = 'AAABCDefgHIJKLM'`,
			wantNewValues: map[string]any{
				"STATUS": "ACTIVE",
			},
			wantOldValues: map[string]any{
				"ROWID": "AAABCDefgHIJKLM",
			},
		},
		// IS NULL / IS NOT NULL predicates in WHERE must be excluded from the result map
		{
			name: "IS NULL and IS NOT NULL in WHERE clause excluded from result",
			sql:  `delete from "MYAPP"."TEST" where "C1" = '1' and "C2" IS NULL and "C3" IS NOT NULL`,
			wantOldValues: map[string]any{
				"C1": "1",
			},
		},
		// Oracle emits bare "Unsupported Type" for columns it cannot represent
		{
			name: "INSERT with Unsupported Type bare value is nil",
			sql:  `insert into "MYAPP"."TEST"("ID","NAME","UT","C1") values ('1','Acme',Unsupported Type,NULL)`,
			wantNewValues: map[string]any{
				"ID":   "1",
				"NAME": "Acme",
				"UT":   nil,
				"C1":   nil,
			},
		},
		// literal || inside a quoted string is not the concatenation operator
		{
			name: "literal double-pipe inside string is not concatenation",
			sql:  `insert into "UNKNOWN"."TABLE" ("COL1","COL2") values ('I||am','test||case')`,
			wantNewValues: map[string]any{
				"COL1": "I||am",
				"COL2": "test||case",
			},
		},
		// identifiers (table and column names) may contain spaces or special characters
		{
			name: "table and column names with spaces and special characters",
			sql:  `insert into "UNKNOWN"."OBJ# 74858"("COL 1","COL 2") values ('1','Hello')`,
			wantNewValues: map[string]any{
				"COL 1": "1",
				"COL 2": "Hello",
			},
		},
		// Complex INSERT combining strings, Oracle functions, Unsupported Type, and NULL
		{
			name: "INSERT mixing strings, Oracle functions, Unsupported Type, and NULL",
			sql:  `insert into "MYAPP"."TEST"("ID","NAME","TS","UT","DATE","C1","C2") values ('1','Acme',TO_TIMESTAMP('2020-02-01 00:00:00.'),Unsupported Type,TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),NULL,NULL)`,
			wantNewValues: map[string]any{
				"ID":   "1",
				"NAME": "Acme",
				"TS":   "TO_TIMESTAMP('2020-02-01 00:00:00.')",
				"UT":   nil,
				"DATE": "TO_DATE('2020-02-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')",
				"C1":   nil,
				"C2":   nil,
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

			newValues, oldValues, err := sqlredo.ExtractValuesFromAST(stmt, nil)
			require.NoError(t, err)

			assert.Equal(t, tt.wantNewValues, newValues)
			assert.Equal(t, tt.wantOldValues, oldValues)
		})
	}
}

func TestExtractValuesWithConverter(t *testing.T) {
	converter := sqlredo.NewOracleValueConverter(time.UTC)

	tests := []struct {
		name          string
		sql           string
		wantNewValues map[string]any
		wantOldValues map[string]any
	}{
		{
			name: "INSERT with bare integer literal",
			sql:  `insert into "MYAPP"."ORDERS" ("ID","AMOUNT") values (100,45.67)`,
			wantNewValues: map[string]any{
				"ID":     int64(100),
				"AMOUNT": json.Number("45.67"),
			},
		},
		{
			name: "INSERT with quoted numeric string preserved as string",
			sql:  `insert into "MYAPP"."PRODUCTS" ("SKU","NAME") values ('12345','Widget')`,
			wantNewValues: map[string]any{
				"SKU":  "12345",
				"NAME": "Widget",
			},
		},
		{
			name: "INSERT mixing bare and quoted numerics",
			sql:  `insert into "MYAPP"."ITEMS" ("ID","CODE") values (42,'42')`,
			wantNewValues: map[string]any{
				"ID":   int64(42),
				"CODE": "42",
			},
		},
		{
			name: "UPDATE with bare numeric in SET clause",
			sql:  `update "MYAPP"."ORDERS" set "AMOUNT" = 99.99 where "ID" = '1'`,
			wantNewValues: map[string]any{
				"AMOUNT": json.Number("99.99"),
			},
			wantOldValues: map[string]any{
				"ID": "1",
			},
		},
		{
			name: "INSERT with scientific notation",
			sql:  `insert into "MYAPP"."DATA" ("VAL") values (1.79E+100)`,
			wantNewValues: map[string]any{
				"VAL": json.Number("1.79E+100"),
			},
		},
		{
			name: "INSERT with Oracle function still converts",
			sql:  `insert into "MYAPP"."EVENTS" ("ID","TS") values (1,TO_DATE('2020-01-15','YYYY-MM-DD'))`,
			wantNewValues: map[string]any{
				"ID": int64(1),
				"TS": time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "INSERT with NULL numeric columns includes them as nil",
			sql:  `insert into "SPACE"."T_EXT_SAMPLES" ("SAMPLE_ID","SPEC_CH_ID","SPEC_CKC_ID") values (2100318824,NULL,NULL)`,
			wantNewValues: map[string]any{
				"SAMPLE_ID":   int64(2100318824),
				"SPEC_CH_ID":  nil,
				"SPEC_CKC_ID": nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := sqlredo.ParseSQLCommand(tt.sql)
			require.NoError(t, err)

			newValues, oldValues, err := sqlredo.ExtractValuesFromAST(stmt, &converter)
			require.NoError(t, err)

			assert.Equal(t, tt.wantNewValues, newValues)
			if tt.wantOldValues != nil {
				assert.Equal(t, tt.wantOldValues, oldValues)
			}
		})
	}
}
