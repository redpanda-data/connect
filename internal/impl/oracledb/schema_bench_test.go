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

// oracleAllTypesColumnMeta returns a fresh []ColumnMeta describing the
// integration test's ALL_TYPES table, using go-ora driver type-name
// spellings as buildColumnMeta reports them. It returns a new slice on every
// call, the same as each snapshot page builds a new []ColumnMeta even when the
// columns are identical (see replication/snapshot.go's buildColumnMeta).
func oracleAllTypesColumnMeta() []replication.ColumnMeta {
	return []replication.ColumnMeta{
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
}

func BenchmarkSeedFromColumnMeta(b *testing.B) {
	// Alternates between two different column sets so every call rebuilds.
	b.Run("rebuild", func(b *testing.B) {
		sc := newSchemaCache(nil, "", service.NewLoggerFromSlog(slog.Default()))
		tbl := replication.UserTable{Schema: "TESTDB", Name: "ALL_TYPES"}
		metas := [2][]replication.ColumnMeta{oracleAllTypesColumnMeta(), oracleAllTypesColumnMeta()[1:]}

		b.ReportAllocs()
		var i int
		for b.Loop() {
			sc.seedFromColumnMeta(tbl, metas[i%2])
			i++
		}
	})

	// A new, equal-content slice per call, as parallel snapshot chunks produce:
	// this should cost a content comparison, not a schema rebuild.
	b.Run("equal_content", func(b *testing.B) {
		sc := newSchemaCache(nil, "", service.NewLoggerFromSlog(slog.Default()))
		tbl := replication.UserTable{Schema: "TESTDB", Name: "ALL_TYPES"}
		metas := [2][]replication.ColumnMeta{oracleAllTypesColumnMeta(), oracleAllTypesColumnMeta()}

		b.ReportAllocs()
		var i int
		for b.Loop() {
			sc.seedFromColumnMeta(tbl, metas[i%2])
			i++
		}
	})

	// After the first call this should cost little more than a mutex lock
	// and a slice identity check, not a schema rebuild.
	b.Run("same_slice", func(b *testing.B) {
		sc := newSchemaCache(nil, "", service.NewLoggerFromSlog(slog.Default()))
		tbl := replication.UserTable{Schema: "TESTDB", Name: "ALL_TYPES"}
		meta := oracleAllTypesColumnMeta()

		b.ReportAllocs()
		for b.Loop() {
			sc.seedFromColumnMeta(tbl, meta)
		}
	})
}
