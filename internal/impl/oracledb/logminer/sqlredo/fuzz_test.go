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
	"time"
)

// FuzzScanSQLCommand asserts that no SQL_REDO input — however malformed — can
// panic the parser or value converter. The parser only ever returns errors; a
// panic would crash the whole CDC connector.
func FuzzScanSQLCommand(f *testing.F) {
	seeds := []string{
		`insert into "S"."T"("C1","C2") values ('a','b')`,
		`update "S"."T" set "C1" = '1' where "C2" = 'old'`,
		`delete from "S"."T" where "C1" = '1' and ROWID = 'AAAF'`,
		`insert into "S"."T"("C") values (UNISTR('caf\00e9') || UNISTR('\d83d\de00'))`,
		`insert into "S"."T"("C") values (HEXTORAW('123'))`,
		`insert into "S"."T"("C") values (TO_DATE('2020-01-15','YYYY-MM-DD'))`,
		`update "S"."T" a set a."C" = NULL where a."K" IS NOT NULL`,
		`delete from "S"."T" where ROWID = ''`,
		``,
		`garbage`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	conv := NewOracleValueConverter(time.UTC)
	f.Fuzz(func(_ *testing.T, sql string) {
		// Must never panic, regardless of return value.
		if stmt, err := ParseSQLCommand(sql); err == nil {
			_, _, _ = ExtractValuesFromAST(stmt, &conv)
		}
		_ = conv.ConvertValue(sql)
	})
}
