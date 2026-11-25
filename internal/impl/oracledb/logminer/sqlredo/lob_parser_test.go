// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
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
			sql:        " buf_c := 'it''s!';\n  dbms_lob.write(loc_c, 5, 1, buf_c);",
			isBinary:   false,
			wantData:   []byte("it's!"),
			wantOffset: 1,
			wantLength: 5,
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
		{
			// Oracle sometimes emits HEXTORAW() for CLOB/NCLOB data (e.g. zero-padded segments).
			name:       "CLOB HEXTORAW fallback",
			sql:        " buf_c := HEXTORAW('48656C6C6F');\n  dbms_lob.write(loc_c, 5, 1, buf_c);",
			isBinary:   false,
			wantData:   []byte("Hello"),
			wantOffset: 1,
			wantLength: 5,
		},
		{
			// Zero-padded CLOB segment — the case that triggered the original warning.
			name:       "CLOB HEXTORAW fallback with zero bytes",
			sql:        " buf_b := HEXTORAW('000000000000');\n  dbms_lob.write(loc_c, 6, 1, buf_b);",
			isBinary:   false,
			wantData:   []byte{0, 0, 0, 0, 0, 0},
			wantOffset: 1,
			wantLength: 6,
		},
		{
			name:       "uppercase DBMS_LOB.WRITE and mixed-case hextoraw",
			sql:        " buf_b := hextoraw('48656c6C6F');\n  DBMS_LOB.WRITE(loc_b, 5, 1, buf_b);",
			isBinary:   true,
			wantData:   []byte("Hello"),
			wantOffset: 1,
			wantLength: 5,
		},
		{
			name:       "extra whitespace",
			sql:        "\n\t buf_c  :=   'abc' ;\n  dbms_lob.write  (  loc_c ,  3  ,\t7 ,  buf_c );",
			wantData:   []byte("abc"),
			wantOffset: 7,
			wantLength: 3,
		},
		{
			name:       "escaped quotes at start and end",
			sql:        " buf_c := '''a''';\n  dbms_lob.write(loc_c, 3, 1, buf_c);",
			wantData:   []byte("'a'"),
			wantOffset: 1,
			wantLength: 3,
		},
		{
			name:       "only escaped quote",
			sql:        " buf_c := '''';\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
			wantData:   []byte("'"),
			wantOffset: 1,
			wantLength: 1,
		},
		{
			name:       "data containing assignment and write call text",
			sql:        " buf_c := 'x := ''y''; dbms_lob.write(loc_c, 99, 99, buf_c); z';\n  dbms_lob.write(loc_c, 54, 3, buf_c);",
			wantData:   []byte("x := 'y'; dbms_lob.write(loc_c, 99, 99, buf_c); z"),
			wantOffset: 3,
			wantLength: 54,
		},
		{
			name:       "empty literal",
			sql:        " buf_c := '';\n  dbms_lob.write(loc_c, 0, 1, buf_c);",
			wantData:   []byte{},
			wantOffset: 1,
			wantLength: 0,
		},
		{
			name:       "empty HEXTORAW",
			sql:        " buf_b := HEXTORAW('');\n  dbms_lob.write(loc_b, 0, 1, buf_b);",
			isBinary:   true,
			wantData:   []byte{},
			wantOffset: 1,
			wantLength: 0,
		},
		{
			name:       "11g unescaped lone quote",
			sql:        " buf_c := 'it's fine';\n  dbms_lob.write(loc_c, 9, 1, buf_c);",
			wantData:   []byte("it's fine"),
			wantOffset: 1,
			wantLength: 9,
		},
		{
			name:       "11g quote at start and end",
			sql:        " buf_c := ''a'';\n  dbms_lob.write(loc_c, 3, 1, buf_c);",
			wantData:   []byte("'a'"),
			wantOffset: 1,
			wantLength: 3,
		},
		{
			name:       "11g data containing quote semicolon and text",
			sql:        " buf_c := 'a'; b := 'c';\n  dbms_lob.write(loc_c, 11, 1, buf_c);",
			wantData:   []byte("a'; b := 'c"),
			wantOffset: 1,
			wantLength: 11,
		},
		{
			name:       "11g fake write call inside data",
			sql:        " buf_c := 'x';\n  dbms_lob.write(loc_c, 9, 9, buf_c); y';\n  dbms_lob.write(loc_c, 40, 1, buf_c);",
			wantData:   []byte("x';\n  dbms_lob.write(loc_c, 9, 9, buf_c); y"),
			wantOffset: 1,
			wantLength: 40,
		},
		{
			name:       "11g data with legitimate adjacent quotes is not unescaped",
			sql:        " buf_c := 'say ''hi'' now';\n  dbms_lob.write(loc_c, 14, 1, buf_c);",
			wantData:   []byte("say ''hi'' now"),
			wantOffset: 1,
			wantLength: 14,
		},
		{
			name:       "21c escaped data with declared length of unescaped text",
			sql:        " buf_c := 'say ''hi'' now';\n  dbms_lob.write(loc_c, 12, 1, buf_c);",
			wantData:   []byte("say 'hi' now"),
			wantOffset: 1,
			wantLength: 12,
		},
		{
			name:       "length matches neither style falls back to unescaped",
			sql:        " buf_c := 'say ''hi'' now';\n  dbms_lob.write(loc_c, 99, 1, buf_c);",
			wantData:   []byte("say 'hi' now"),
			wantOffset: 1,
			wantLength: 99,
		},
		{
			name:       "NCLOB multibyte 21c escaped",
			sql:        " buf_c := 'héllo ''wörld'' 日本';\n  dbms_lob.write(loc_c, 15, 1, buf_c);",
			wantData:   []byte("héllo 'wörld' 日本"),
			wantOffset: 1,
			wantLength: 15,
		},
		{
			name:       "NCLOB multibyte 11g unescaped quotes",
			sql:        " buf_c := 'é'日'本'';\n  dbms_lob.write(loc_c, 6, 1, buf_c);",
			wantData:   []byte("é'日'本'"),
			wantOffset: 1,
			wantLength: 6,
		},
		{
			name:       "non-BMP char counted as UTF-16 units, 11g adjacent quotes",
			sql:        " buf_c := '😀''x';\n  dbms_lob.write(loc_c, 5, 1, buf_c);",
			wantData:   []byte("😀''x"),
			wantOffset: 1,
			wantLength: 5,
		},
		{
			name:       "non-BMP char counted as UTF-16 units, 21c escaped",
			sql:        " buf_c := '😀''x';\n  dbms_lob.write(loc_c, 4, 1, buf_c);",
			wantData:   []byte("😀'x"),
			wantOffset: 1,
			wantLength: 4,
		},
		{name: "malformed: missing write call", sql: " buf_c := 'abc';", wantErr: true},
		{name: "malformed: missing assignment", sql: " dbms_lob.write(loc_c, 3, 1, buf_c);", wantErr: true},
		{name: "malformed: unterminated literal", sql: " buf_c := 'abc;\n dbms_lob.write(loc_c, 3, 1, buf_c);", wantErr: true},
		{name: "malformed: unterminated HEXTORAW", sql: " buf_b := HEXTORAW('4865;\n dbms_lob.write(loc_b, 2, 1, buf_b);", isBinary: true, wantErr: true},
		{name: "malformed: odd-length hex", sql: " buf_b := HEXTORAW('486');\n dbms_lob.write(loc_b, 2, 1, buf_b);", isBinary: true, wantErr: true},
		{name: "malformed: non-hex chars", sql: " buf_b := HEXTORAW('zz');\n dbms_lob.write(loc_b, 1, 1, buf_b);", isBinary: true, wantErr: true},
		{name: "malformed: non-numeric length", sql: " buf_c := 'a';\n dbms_lob.write(loc_c, x, 1, buf_c);", wantErr: true},
		{name: "malformed: missing offset", sql: " buf_c := 'a';\n dbms_lob.write(loc_c, 1);", wantErr: true},
		{name: "malformed: overflowing length", sql: " buf_c := 'a';\n dbms_lob.write(loc_c, 99999999999999999999, 1, buf_c);", wantErr: true},
		{
			name:     "CLOB with no string literal and no HEXTORAW",
			sql:      " buf_c := SOMEFUNCTION('x');\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
			isBinary: false,
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

func TestParseLobTrim(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		want    int64
		wantErr bool
	}{
		{name: "basic", sql: "\n dbms_lob.trim(loc_c, 100);", want: 100},
		{name: "mixed case and whitespace", sql: "DBMS_LOB.Trim  (  loc_c ,\t42 \n) ;", want: 42},
		{name: "zero", sql: "dbms_lob.trim(loc_b, 0);", want: 0},
		{name: "no call", sql: "nothing", wantErr: true},
		{name: "non-numeric", sql: "dbms_lob.trim(loc_b, abc);", wantErr: true},
		{name: "missing paren", sql: "dbms_lob.trim(loc_b, 5;", wantErr: true},
		{name: "empty first arg", sql: "dbms_lob.trim(, 5);", wantErr: true},
		{name: "overflow", sql: "dbms_lob.trim(loc, 99999999999999999999);", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseLobTrim(tc.sql)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func BenchmarkParseLobWrite(b *testing.B) {
	const (
		blobBytes = 2048
		clobChars = 4000
	)
	rng := rand.New(rand.NewSource(1))
	raw := make([]byte, blobBytes)
	_, _ = rng.Read(raw)
	blobSQL := fmt.Sprintf("\n buf_b := HEXTORAW('%s');\n  dbms_lob.write(loc_b, %d, 65057, buf_b);",
		strings.ToUpper(hex.EncodeToString(raw)), blobBytes)

	var sb strings.Builder
	for sb.Len() < clobChars {
		sb.WriteString("The quick brown fox isn''t lazy; it jumps over dogs. ")
	}
	clobSQL := fmt.Sprintf("\n buf_c := '%s';\n  dbms_lob.write(loc_c, %d, 1, buf_c);", sb.String(), sb.Len())

	b.Run("blob", func(b *testing.B) {
		b.SetBytes(int64(len(blobSQL)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := ParseLobWrite(blobSQL, true); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("clob", func(b *testing.B) {
		b.SetBytes(int64(len(clobSQL)))
		b.ReportAllocs()
		for b.Loop() {
			if _, err := ParseLobWrite(clobSQL, false); err != nil {
				b.Fatal(err)
			}
		}
	})
}
