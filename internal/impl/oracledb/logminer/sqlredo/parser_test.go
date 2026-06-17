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

func TestScanSQLCommand(t *testing.T) {
	converter := sqlredo.NewOracleValueConverter(time.UTC)

	tests := []struct {
		name    string
		sql     string
		wantNew map[string]any
		wantOld map[string]any
		wantErr bool
	}{
		{
			name:    "INSERT strings",
			sql:     `insert into "MYAPP"."CUSTOMERS" ("ID","NAME","EMAIL") values ('1','John Doe','john@example.com')`,
			wantNew: map[string]any{"ID": "1", "NAME": "John Doe", "EMAIL": "john@example.com"},
		},
		{
			name:    "INSERT with NULL values",
			sql:     `insert into "MYAPP"."SAMPLES" ("ID","SPEC_CH_ID","SPEC_CKC_ID") values ('1',NULL,NULL)`,
			wantNew: map[string]any{"ID": "1", "SPEC_CH_ID": nil, "SPEC_CKC_ID": nil},
		},
		{
			name:    "INSERT escaped single quote in string",
			sql:     `insert into "MYAPP"."MESSAGES" ("ID","TEXT") values ('1','It''s a test')`,
			wantNew: map[string]any{"ID": "1", "TEXT": "It's a test"},
		},
		{
			name:    "INSERT double quote inside string",
			sql:     `insert into "MYAPP"."MESSAGES" ("ID","TEXT") values ('1','He said "Hello"')`,
			wantNew: map[string]any{"ID": "1", "TEXT": `He said "Hello"`},
		},
		{
			name:    "INSERT Oracle function bare value",
			sql:     `insert into "MYAPP"."ORDERS" ("ID","ORDER_DATE") values ('100',TO_DATE('2020-01-15','YYYY-MM-DD'))`,
			wantNew: map[string]any{"ID": "100", "ORDER_DATE": time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:    "INSERT numeric bare value",
			sql:     `insert into "MYAPP"."ORDERS" ("ID","AMOUNT") values (100,45.67)`,
			wantNew: map[string]any{"ID": int64(100), "AMOUNT": json.Number("45.67")},
		},
		{
			name:    "INSERT single quote inside double-quoted table name",
			sql:     `insert into "MYAPP"."O'Brien" ("ID","NAME") values ('1','Alice')`,
			wantNew: map[string]any{"ID": "1", "NAME": "Alice"},
		},
		{
			name:    "UPDATE SET and WHERE",
			sql:     `update "MYAPP"."CUSTOMERS" set "NAME" = 'Jane Doe', "EMAIL" = 'jane@example.com' where "ID" = '1' and "NAME" = 'John Doe'`,
			wantNew: map[string]any{"NAME": "Jane Doe", "EMAIL": "jane@example.com"},
			wantOld: map[string]any{"ID": "1", "NAME": "John Doe"},
		},
		{
			name:    "UPDATE NULL in SET",
			sql:     `update "MYAPP"."CUSTOMERS" set "EMAIL" = NULL where "ID" = '1'`,
			wantNew: map[string]any{"EMAIL": nil},
			wantOld: map[string]any{"ID": "1"},
		},
		{
			name:    "DELETE basic",
			sql:     `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "NAME" = 'John Doe'`,
			wantOld: map[string]any{"ID": "1", "NAME": "John Doe"},
		},
		{
			name:    "DELETE IS NULL excluded from map",
			sql:     `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "COMM" IS NULL`,
			wantOld: map[string]any{"ID": "1"},
		},
		{
			name:    "DELETE IS NOT NULL excluded from map",
			sql:     `delete from "MYAPP"."CUSTOMERS" where "ID" = '1' and "COMM" IS NOT NULL`,
			wantOld: map[string]any{"ID": "1"},
		},
		{
			name:    "unrecognised statement returns error",
			sql:     `SELECT * FROM "T"`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNew, gotOld, err := sqlredo.ScanSQLCommand(tt.sql, &converter)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantNew, gotNew)
			if tt.wantOld != nil {
				assert.Equal(t, tt.wantOld, gotOld)
			}
		})
	}
}
