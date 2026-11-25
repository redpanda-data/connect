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
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Regex reference implementation of ParseLobWrite, retained to assert that the
// hand-written scanner is behaviourally identical.
var (
	refReLobWriteParams = regexp.MustCompile(`(?i)dbms_lob\.write\s*\([^,]+,\s*(\d+)\s*,\s*(\d+)\s*,`)
	refReLobAssignment  = regexp.MustCompile(`(?i):=\s*(HEXTORAW\('[0-9A-Fa-f]*'\)|'(?:[^']|'')*')`)
	refReLobHextoraw    = regexp.MustCompile(`(?i)HEXTORAW\('([0-9A-Fa-f]*)'\)`)
	refReLobStrLiteral  = regexp.MustCompile(`^'((?:[^']|'')*)'$`)
)

func parseLobWriteRegex(sql string, isBinary bool) (*LobWriteInfo, error) {
	var (
		length int64
		offset int64
		data   []byte
		err    error
	)

	paramsMatch := refReLobWriteParams.FindStringSubmatch(sql)
	if paramsMatch == nil {
		return nil, errors.New("could not parse dbms_lob.write() call in LOB_WRITE SQL")
	}
	if length, err = strconv.ParseInt(paramsMatch[1], 10, 64); err != nil {
		return nil, fmt.Errorf("parsing LOB write length: %w", err)
	}
	if offset, err = strconv.ParseInt(paramsMatch[2], 10, 64); err != nil {
		return nil, fmt.Errorf("parsing LOB write offset: %w", err)
	}

	var expr string
	if m := refReLobAssignment.FindStringSubmatch(sql); m != nil {
		expr = m[1]
	} else {
		return nil, errors.New("could not find LOB data in LOB_WRITE SQL")
	}

	if isBinary {
		matchHex := refReLobHextoraw.FindStringSubmatch(expr)
		if matchHex == nil {
			return nil, errors.New("could not find HEXTORAW() in LOB_WRITE BLOB data expression")
		}
		if data, err = hex.DecodeString(matchHex[1]); err != nil {
			return nil, fmt.Errorf("hex-decoding BLOB data: %w", err)
		}
	} else {
		if matchStr := refReLobStrLiteral.FindStringSubmatch(expr); matchStr != nil {
			data = []byte(strings.ReplaceAll(matchStr[1], "''", "'"))
		} else {
			matchHex := refReLobHextoraw.FindStringSubmatch(expr)
			if matchHex == nil {
				return nil, errors.New("could not find string literal in LOB_WRITE CLOB data expression")
			}
			if data, err = hex.DecodeString(matchHex[1]); err != nil {
				return nil, fmt.Errorf("hex-decoding CLOB data: %w", err)
			}
		}
	}
	return &LobWriteInfo{Data: data, Offset: offset, Length: length}, nil
}

const benchFragmentSize = 2000

func lobBenchHexSQL() string {
	return " buf_b := HEXTORAW('" + strings.Repeat("A1b2C3d4", benchFragmentSize/4) +
		"');\n  dbms_lob.write(loc_b, 2000, 1, buf_b);"
}

func lobBenchStringSQL() string {
	return " buf_c := '" + strings.Repeat("Hello it''s a CLOB. ", benchFragmentSize/20) +
		"';\n  dbms_lob.write(loc_c, 2000, 1, buf_c);"
}

func lobWriteSeeds() []string {
	return []string{
		" buf_c := 'Hello World';\n  dbms_lob.write(loc_c, 11, 1, buf_c);",
		" buf_b := HEXTORAW('48656C6C6F');\n  dbms_lob.write(loc_b, 5, 1, buf_b);",
		" buf_c := 'ing';\n  dbms_lob.write(loc_c, 3, 6, buf_c);",
		" buf_c := 'it''s!';\n  dbms_lob.write(loc_c, 6, 1, buf_c);",
		"not a lob write",
		" buf_b := 'hello';\n  dbms_lob.write(loc_b, 5, 1, buf_b);",
		" buf_b := HEXTORAW('000000000000');\n  dbms_lob.write(loc_c, 6, 1, buf_b);",
		" buf_c := SOMEFUNCTION('x');\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
		" buf_c := '';\n  dbms_lob.write(loc_c, 0, 1, buf_c);",
		" buf_b := HEXTORAW('');\n  dbms_lob.write(loc_b, 0, 1, buf_b);",
		" buf_b := HEXTORAW('ABC');\n  dbms_lob.write(loc_b, 1, 1, buf_b);",
		" buf_c := 'unterminated;\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
		" buf_c := 'ab'';\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
		" buf_c := 'a'''\n  dbms_lob.write(loc_c, 1, 1, buf_c);",
		" buf_b := 'HEXTORAW('')';\n  dbms_lob.write(loc_b, 1, 1, buf_b);",
		" buf_b := 'x HEXTORAW(''AB'') y';\n  dbms_lob.write(loc_b, 1, 1, buf_b);",
		" a := 1; buf_c := 'x';\n  DBMS_LOB.WRITE (loc_c ,\t1\n,\r2 , buf_c);",
		" buf_c := 'x';\n  dbmſ_lob.write(loc_c, 1, 1, buf_c);",
		" buf_c := 'x';\n  dbms_lob.write(loc_c, 99999999999999999999, 1, buf_c);",
		" buf_c := 'x';\n  dbms_lob.write(loc_c, 1, 99999999999999999999, buf_c);",
		" buf_c := 'x';\n  dbms_lob.write(, 1, 1, buf_c);",
		" buf_c := 'x';\n  dbms_lob.write(loc_c, 1, buf_c);\n dbms_lob.write(l, 2, 3, b);",
		" x := ; buf_c := hextoraw('4142');\n  dbms_lob.write(loc_c, 2, 1, buf_c);",
		lobBenchHexSQL(),
		lobBenchStringSQL(),
		"buf := 'a'; dbms_lob.write(loc, 1, 1, buf); y := 'b'",
	}
}

func assertLobWriteMatchesRegex(t testing.TB, sql string, isBinary bool) {
	t.Helper()
	wantInfo, wantErr := parseLobWriteRegex(sql, isBinary)
	gotInfo, gotErr := ParseLobWrite(sql, isBinary)
	if wantErr != nil || gotErr != nil {
		if wantErr == nil || gotErr == nil || wantErr.Error() != gotErr.Error() {
			t.Fatalf("error mismatch for %q (binary=%v): regex=%v scanner=%v", sql, isBinary, wantErr, gotErr)
		}
	}
	assert.Equal(t, wantInfo, gotInfo, "result mismatch for %q (binary=%v)", sql, isBinary)
}

func TestParseLobWriteMatchesRegex(t *testing.T) {
	for _, sql := range lobWriteSeeds() {
		assertLobWriteMatchesRegex(t, sql, true)
		assertLobWriteMatchesRegex(t, sql, false)
	}
}

// FuzzParseLobWriteMatchesRegex asserts that the hand-written scanner in
// ParseLobWrite agrees with the original regex implementation on results and
// error messages.
func FuzzParseLobWriteMatchesRegex(f *testing.F) {
	for _, sql := range lobWriteSeeds() {
		f.Add(sql, true)
		f.Add(sql, false)
	}
	f.Fuzz(func(t *testing.T, sql string, isBinary bool) {
		assertLobWriteMatchesRegex(t, sql, isBinary)
	})
}

func BenchmarkParseLobWrite(b *testing.B) {
	cases := []struct {
		name     string
		sql      string
		isBinary bool
	}{
		{"BLOB_2KB_hextoraw", lobBenchHexSQL(), true},
		{"CLOB_2KB_literal", lobBenchStringSQL(), false},
	}
	impls := []struct {
		name string
		fn   func(string, bool) (*LobWriteInfo, error)
	}{
		{"regex", parseLobWriteRegex},
		{"scanner", ParseLobWrite},
	}
	for _, c := range cases {
		for _, impl := range impls {
			b.Run(c.name+"/"+impl.name, func(b *testing.B) {
				b.SetBytes(int64(len(c.sql)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := impl.fn(c.sql, c.isBinary); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
