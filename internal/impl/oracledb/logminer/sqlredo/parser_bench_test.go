// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
)

// Representative SQL_REDO strings as Oracle LogMiner produces them.
// Columns and value counts mirror a typical 8-column USERS table with
// string, numeric, and timestamp fields.
var (
	benchInsertSQL = `insert into "TESTDB"."USERS"("ID","USERNAME","EMAIL","FIRST_NAME","LAST_NAME","CREATED_AT","STATUS","SCORE") values ('42','jdoe','jdoe@example.com','John','Doe',TO_TIMESTAMP('2024-01-15 10:30:00.000000','YYYY-MM-DD HH24:MI:SS.FF'),'active',99.5)`
	benchUpdateSQL = `update "TESTDB"."USERS" set "EMAIL" = 'new@example.com',"STATUS" = 'inactive',"SCORE" = 75.0 where "ID" = '42' and "USERNAME" = 'jdoe'`
	benchDeleteSQL = `delete from "TESTDB"."USERS" where "ID" = '42' and "USERNAME" = 'jdoe' and "EMAIL" = 'jdoe@example.com'`

	benchNarrowInsertSQL = `insert into "TESTDB"."CART"("ID","USER_ID","PRODUCT_ID") values ('1','42','100')`
	benchWideInsertSQL   = `insert into "TESTDB"."PRODUCTS"("ID","NAME","DESCRIPTION","PRICE","CATEGORY","SUBCATEGORY","BRAND","SKU","STOCK","WEIGHT","WIDTH","HEIGHT","DEPTH","CREATED_AT","UPDATED_AT","ACTIVE") values ('1','Widget Pro','A professional widget','29.99','Hardware','Tools','Acme','WGT-001','500',1.5,10.0,5.0,3.0,TO_TIMESTAMP('2024-01-01 00:00:00.000000','YYYY-MM-DD HH24:MI:SS.FF'),TO_TIMESTAMP('2024-06-01 12:00:00.000000','YYYY-MM-DD HH24:MI:SS.FF'),'1')`
)

func newRedoEvent(op sqlredo.Operation, sqlStr string) *sqlredo.RedoEvent {
	return &sqlredo.RedoEvent{
		SCN:           1000,
		Operation:     op,
		SQLRedo:       sql.NullString{String: sqlStr, Valid: true},
		SchemaName:    sql.NullString{String: "TESTDB", Valid: true},
		TableName:     sql.NullString{String: "USERS", Valid: true},
		TransactionID: "1.2.3",
		Timestamp:     time.Now(),
	}
}

func BenchmarkRedoEventToDMLEvent_Insert(b *testing.B) {
	p := sqlredo.NewParser()
	ev := newRedoEvent(sqlredo.OpInsert, benchInsertSQL)
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.RedoEventToDMLEvent(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedoEventToDMLEvent_Update(b *testing.B) {
	p := sqlredo.NewParser()
	ev := newRedoEvent(sqlredo.OpUpdate, benchUpdateSQL)
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.RedoEventToDMLEvent(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedoEventToDMLEvent_Delete(b *testing.B) {
	p := sqlredo.NewParser()
	ev := newRedoEvent(sqlredo.OpDelete, benchDeleteSQL)
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.RedoEventToDMLEvent(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedoEventToDMLEvent_NarrowInsert(b *testing.B) {
	p := sqlredo.NewParser()
	ev := newRedoEvent(sqlredo.OpInsert, benchNarrowInsertSQL)
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.RedoEventToDMLEvent(ev); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRedoEventToDMLEvent_WideInsert(b *testing.B) {
	p := sqlredo.NewParser()
	ev := newRedoEvent(sqlredo.OpInsert, benchWideInsertSQL)
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.RedoEventToDMLEvent(ev); err != nil {
			b.Fatal(err)
		}
	}
}
