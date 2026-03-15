// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSelectLobLocator(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantErr    bool
		wantSchema string
		wantTable  string
		wantColumn string
		wantPKs    map[string]any
	}{
		{
			name:       "CLOB column (loc_c)",
			sql:        "DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6216); \n loc_b BLOB; \n buf_b RAW(6216); \n loc_nc NCLOB; \n buf_nc NVARCHAR2(6216); \nBEGIN\n select \"CONTENT\" into loc_c from \"MYSCHEMA\".\"MYTABLE\" where \"ID\" = '42' and ROWID = 'AAAXxxx' for update;\nEND;",
			wantSchema: "MYSCHEMA",
			wantTable:  "MYTABLE",
			wantColumn: "CONTENT",
			wantPKs:    map[string]any{"ID": "42"},
		},
		{
			name:       "NCLOB column (loc_nc)",
			sql:        "DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6216); \n loc_b BLOB; \n buf_b RAW(6216); \n loc_nc NCLOB; \n buf_nc NVARCHAR2(6216); \nBEGIN\n select \"DESCRIPTION\" into loc_nc from \"TESTDB\".\"PRODUCTS\" where \"ID\" = '1' and ROWID = 'AAAXxxx' for update;\nEND;",
			wantSchema: "TESTDB",
			wantTable:  "PRODUCTS",
			wantColumn: "DESCRIPTION",
			wantPKs:    map[string]any{"ID": "1"},
		},
		{
			name:       "single variable declaration (lob_1)",
			sql:        `DECLARE lob_1 CLOB; lob_1_f BOOLEAN; BEGIN select "CONTENT" into lob_1 from "MYSCHEMA"."MYTABLE" where "ID" = '42' and ROWID = 'AAAXxxx' for update; lob_1_f := dbms_lob.isopen(lob_1) = 0; if lob_1_f then dbms_lob.open(lob_1, dbms_lob.lob_readwrite); end if; END;`,
			wantSchema: "MYSCHEMA",
			wantTable:  "MYTABLE",
			wantColumn: "CONTENT",
			wantPKs:    map[string]any{"ID": "42"},
		},
		{
			name:       "multi-column PK",
			sql:        `DECLARE lob_1 CLOB; lob_1_f BOOLEAN; BEGIN select "DATA" into lob_1 from "S"."T" where "PK1" = 'A' and "PK2" = '99' and ROWID = 'xxx' for update; END;`,
			wantSchema: "S",
			wantTable:  "T",
			wantColumn: "DATA",
			wantPKs:    map[string]any{"PK1": "A", "PK2": "99"},
		},
		{
			name:       "escaped single quote in PK value",
			sql:        `DECLARE lob_1 CLOB; lob_1_f BOOLEAN; BEGIN select "NOTE" into lob_1 from "S"."T" where "KEY" = 'it''s' and ROWID = 'xxx' for update; END;`,
			wantSchema: "S",
			wantTable:  "T",
			wantColumn: "NOTE",
			wantPKs:    map[string]any{"KEY": "it's"},
		},
		{
			name:    "empty SQL",
			sql:     "",
			wantErr: true,
		},
		{
			name:    "missing select into",
			sql:     `DECLARE lob_1 CLOB; lob_1_f BOOLEAN; BEGIN no select here from "S"."T" where "ID" = '1' and ROWID = 'x' for update; END;`,
			wantErr: true,
		},
		{
			name:    "missing from clause",
			sql:     `DECLARE lob_1 CLOB; lob_1_f BOOLEAN; BEGIN select "COL" into lob_1 no table here where "ID" = '1' and ROWID = 'x' for update; END;`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSelectLobLocator(tc.sql)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSchema, got.Schema)
			assert.Equal(t, tc.wantTable, got.Table)
			assert.Equal(t, tc.wantColumn, got.Column)
			assert.Equal(t, tc.wantPKs, got.PKValues)
		})
	}
}

func TestParseLobWrite(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		isBinary   bool
		wantErr    bool
		wantData   []byte
		wantOffset int64
		wantLength int64
	}{
		{
			name:       "CLOB buffer assignment",
			sql:        " buf_c := 'Hello World';\n  dbms_lob.write(loc_c, 11, 1, buf_c);",
			isBinary:   false,
			wantData:   []byte("Hello World"),
			wantOffset: 1,
			wantLength: 11,
		},
		{
			name:       "BLOB HEXTORAW buffer assignment",
			sql:        " buf_b := HEXTORAW('48656C6C6F');\n  dbms_lob.write(loc_b, 5, 1, buf_b);",
			isBinary:   true,
			wantData:   []byte("Hello"),
			wantOffset: 1,
			wantLength: 5,
		},
		{
			name:       "non-1 offset",
			sql:        " buf_c := 'ing';\n  dbms_lob.write(loc_c, 3, 6, buf_c);",
			isBinary:   false,
			wantData:   []byte("ing"),
			wantOffset: 6,
			wantLength: 3,
		},
		{
			name:       "escaped quote in CLOB",
			sql:        " buf_c := 'it''s!';\n  dbms_lob.write(loc_c, 6, 1, buf_c);",
			isBinary:   false,
			wantData:   []byte("it's!"),
			wantOffset: 1,
			wantLength: 6,
		},
		{
			name:     "invalid SQL",
			sql:      "not a lob write",
			isBinary: false,
			wantErr:  true,
		},
		{
			name:     "BLOB data without HEXTORAW",
			sql:      " buf_b := 'hello';\n  dbms_lob.write(loc_b, 5, 1, buf_b);",
			isBinary: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLobWrite(tc.sql, tc.isBinary)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantData, got.Data)
			assert.Equal(t, tc.wantOffset, got.Offset)
			assert.Equal(t, tc.wantLength, got.Length)
		})
	}
}
