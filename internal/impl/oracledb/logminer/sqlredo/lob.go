// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"fmt"
	"regexp"
	"strings"
)

// LobLocator holds the parsed result of a SELECT_LOB_LOCATOR SQL_REDO statement.
type LobLocator struct {
	// Column is the name of the LOB column being written.
	Column string
	// Binary is true when the LOB is a BLOB; false for CLOB/NCLOB.
	Binary bool
}

// LobWrite holds the parsed result of a LOB_WRITE SQL_REDO statement.
type LobWrite struct {
	// Data is the raw string for CLOB, or the HEXTORAW hex string for BLOB.
	Data string
	// Offset is the 0-based character/byte offset into the LOB.
	Offset int
	// Binary is true when HEXTORAW() was used, indicating a BLOB write.
	Binary bool
}

// lobWritePattern matches a LOB_WRITE SQL_REDO statement, capturing the data
// payload, the byte length, and the 1-based write offset.
// This mirrors the pattern used by Debezium's LobWriteParser.java.
//
// Capture groups:
//
//	1 - data payload (quoted string, optionally wrapped in HEXTORAW(...))
//	2 - byte/char length argument to dbms_lob.write
//	3 - 1-based write offset argument to dbms_lob.write
var lobWritePattern = regexp.MustCompile(
	`(?si).* := ((?:HEXTORAW\()?'.*'(?:\))?);[\s]*dbms_lob\.write\([^,]+,[\s]*(\d+)[\s]*,[\s]*(\d+)[\s]*,[^,]+\);.*`,
)

// ParseSelectLobLocator parses a SELECT_LOB_LOCATOR SQL_REDO statement and
// returns the column being locked for update and whether it is binary (BLOB).
//
// The SQL_REDO has the form:
//
//	DECLARE
//	  loc_c CLOB; buf_c VARCHAR2(6174);
//	  loc_b BLOB; buf_b RAW(6174);
//	BEGIN
//	  select "COLUMN" into loc_c from "SCHEMA"."TABLE" where ... for update;
func ParseSelectLobLocator(sql string) (LobLocator, error) {
	// Detect binary: the DECLARE block (before BEGIN) contains "loc_b" or "buf_b"
	upper := strings.ToUpper(sql)
	beginIdx := strings.Index(upper, "BEGIN")
	if beginIdx < 0 {
		return LobLocator{}, fmt.Errorf("SELECT_LOB_LOCATOR SQL missing BEGIN keyword")
	}
	declareBlock := upper[:beginIdx]
	binary := strings.Contains(declareBlock, "LOC_B") || strings.Contains(declareBlock, "BUF_B")

	// Extract column name: text between 'select "' and '" into'
	selectIdx := strings.Index(upper, `SELECT "`)
	if selectIdx < 0 {
		return LobLocator{}, fmt.Errorf("SELECT_LOB_LOCATOR SQL missing SELECT clause")
	}
	// Work on original-case string for the column name
	afterSelect := sql[selectIdx+len(`SELECT `):]
	if len(afterSelect) == 0 || afterSelect[0] != '"' {
		return LobLocator{}, fmt.Errorf("SELECT_LOB_LOCATOR SQL column name not quoted")
	}
	closeQuote := strings.Index(afterSelect[1:], `"`)
	if closeQuote < 0 {
		return LobLocator{}, fmt.Errorf("SELECT_LOB_LOCATOR SQL column name closing quote not found")
	}
	column := afterSelect[1 : closeQuote+1]

	return LobLocator{Column: column, Binary: binary}, nil
}

// ParseLobWrite parses a LOB_WRITE SQL_REDO statement and returns the data
// chunk, its 0-based offset, and whether it is binary.
func ParseLobWrite(sql string) (LobWrite, error) {
	m := lobWritePattern.FindStringSubmatch(strings.TrimSpace(sql))
	if m == nil {
		return LobWrite{}, fmt.Errorf("unable to parse LOB_WRITE SQL: %s", sql)
	}

	data := m[1]
	binary := strings.HasPrefix(strings.ToUpper(data), "HEXTORAW(")

	if binary {
		// Strip HEXTORAW(' ... ')
		data = data[len("HEXTORAW('") : len(data)-2]
	} else {
		// Strip surrounding single quotes
		data = data[1 : len(data)-1]
	}

	// Unescape doubled single quotes: '' -> '
	data = strings.ReplaceAll(data, "''", "'")

	// Parse offset (group 3); Oracle uses 1-based offsets, convert to 0-based.
	var offset int
	if _, err := fmt.Sscan(m[3], &offset); err != nil {
		return LobWrite{}, fmt.Errorf("parsing LOB_WRITE offset: %w", err)
	}
	offset--

	return LobWrite{Data: data, Offset: offset, Binary: binary}, nil
}
