// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"log/slog"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// BenchmarkSeedFromColumnMeta covers the hottest schema-cache path: the
// publisher re-seeds the cache from driver column metadata for EVERY snapshot
// row published (batcher.go Publish → seedFromColumnMeta), so per-column
// type-name mapping cost here is paid once per snapshot message, not once per
// table. Column set mirrors the integration test's ALL_TYPES table using
// go-ora driver type-name spellings, as buildColumnMeta reports them.
func BenchmarkSeedFromColumnMeta(b *testing.B) {
	sc := newSchemaCache(nil, "", service.NewLoggerFromSlog(slog.Default()))
	tbl := replication.UserTable{Schema: "TESTDB", Name: "ALL_TYPES"}
	meta := []replication.ColumnMeta{
		{Name: "ID", TypeName: "NUMBER", Precision: 38, Scale: 255, HasDecimalSize: true},
		{Name: "NUM_PLAIN", TypeName: "NUMBER", Precision: 38, Scale: 255, HasDecimalSize: true},
		{Name: "NUM_38", TypeName: "NUMBER", Precision: 38, Scale: 0, HasDecimalSize: true},
		{Name: "NUM_38_2", TypeName: "NUMBER", Precision: 38, Scale: 2, HasDecimalSize: true},
		{Name: "NUM_10_2", TypeName: "NUMBER", Precision: 10, Scale: 2, HasDecimalSize: true},
		{Name: "NUM_5_0", TypeName: "NUMBER", Precision: 5, Scale: 0, HasDecimalSize: true},
		{Name: "NUM_INT", TypeName: "NUMBER", Precision: 38, Scale: 0, HasDecimalSize: true},
		{Name: "FLT", TypeName: "NUMBER", Precision: 38, Scale: 255, HasDecimalSize: true},
		{Name: "BIN_FLOAT", TypeName: "IBFloat"},
		{Name: "BIN_DOUBLE", TypeName: "IBDouble"},
		{Name: "VC", TypeName: "NCHAR"},
		{Name: "CH", TypeName: "CHAR"},
		{Name: "NVC", TypeName: "NCHAR"},
		{Name: "DT", TypeName: "DATE"},
		{Name: "TS", TypeName: "TimeStampDTY"},
		{Name: "TS_TZ", TypeName: "TimeStampTZ_DTY"},
		{Name: "RW", TypeName: "RAW"},
		{Name: "DOC", TypeName: "LongVarChar"},
	}

	b.ReportAllocs()
	for b.Loop() {
		sc.seedFromColumnMeta(tbl, meta)
	}
}
