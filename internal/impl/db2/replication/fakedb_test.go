// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// Fake SQL driver for unit tests in the replication package.
// Registered once as "fake-db2-repl". Each test creates an independent fake
// DB via openFakeDB, which stores its query/exec handlers keyed by a unique DSN.

var (
	replFakeDrvOnce sync.Once
	replFakeConns   sync.Map
	replFakeConnSeq atomic.Int64
)

// replFakeHandlers configures the behaviour of a fake DB connection.
type replFakeHandlers struct {
	// query is called for every SELECT. Return (nil, nil, nil) for no rows.
	query func(query string, args []driver.Value) (columns []string, rows [][]driver.Value, err error)
	// colTypes optionally maps column name → DB type name for a given query.
	// When nil, ColumnTypeDatabaseTypeName returns "" for all columns.
	colTypes func(query string) map[string]string
	// exec is called for every INSERT/UPDATE/DELETE/CREATE/MERGE. May be nil.
	exec func(query string, args []driver.Value) error
}

// openFakeDB creates a *sql.DB backed by the given handlers.
// The DB is closed and its DSN entry removed when t completes.
func openFakeDB(t *testing.T, h *replFakeHandlers) *sql.DB {
	t.Helper()
	replFakeDrvOnce.Do(func() { sql.Register("fake-db2-repl", &replFakeDriver{}) })
	dsn := fmt.Sprintf("repl-fake-%d", replFakeConnSeq.Add(1))
	replFakeConns.Store(dsn, h)
	db, err := sql.Open("fake-db2-repl", dsn)
	if err != nil {
		t.Fatalf("openFakeDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = db.Close()
		replFakeConns.Delete(dsn)
	})
	return db
}

// ---- driver.Driver ----

type replFakeDriver struct{}

func (*replFakeDriver) Open(dsn string) (driver.Conn, error) {
	v, ok := replFakeConns.Load(dsn)
	if !ok {
		return nil, fmt.Errorf("fake-db2-repl: no handlers for DSN %q", dsn)
	}
	return &replFakeConn{h: v.(*replFakeHandlers)}, nil
}

// ---- driver.Conn + driver.ConnBeginTx ----

type replFakeConn struct{ h *replFakeHandlers }

func (c *replFakeConn) Prepare(query string) (driver.Stmt, error) {
	return &replFakeStmt{c: c, q: query}, nil
}
func (*replFakeConn) Close() error              { return nil }
func (*replFakeConn) Begin() (driver.Tx, error) { return &replFakeTx{}, nil }
func (*replFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &replFakeTx{}, nil
}

// ---- driver.Tx ----

type replFakeTx struct{}

func (*replFakeTx) Commit() error   { return nil }
func (*replFakeTx) Rollback() error { return nil }

// ---- driver.Stmt ----

type replFakeStmt struct {
	c *replFakeConn
	q string
}

func (*replFakeStmt) Close() error  { return nil }
func (*replFakeStmt) NumInput() int { return -1 }

func (s *replFakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.c.h.exec != nil {
		return &replFakeResult{}, s.c.h.exec(s.q, args)
	}
	return &replFakeResult{}, nil
}

func (s *replFakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.c.h.query == nil {
		return &replFakeRows{}, nil
	}
	cols, data, err := s.c.h.query(s.q, args)
	if err != nil {
		return nil, err
	}
	var ct map[string]string
	if s.c.h.colTypes != nil {
		ct = s.c.h.colTypes(s.q)
	}
	return &replFakeRows{cols: cols, data: data, colTypes: ct}, nil
}

// ---- driver.Rows + RowsColumnTypeDatabaseTypeName ----

type replFakeRows struct {
	cols     []string
	colTypes map[string]string // column name → DB type name (e.g. "VARCHAR", "INTEGER")
	data     [][]driver.Value
	pos      int
}

func (r *replFakeRows) Columns() []string { return r.cols }
func (*replFakeRows) Close() error        { return nil }

func (r *replFakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	for i, v := range r.data[r.pos] {
		if i < len(dest) {
			dest[i] = v
		}
	}
	r.pos++
	return nil
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName.
func (r *replFakeRows) ColumnTypeDatabaseTypeName(index int) string {
	if r.colTypes == nil || index >= len(r.cols) {
		return ""
	}
	return r.colTypes[r.cols[index]]
}

// ---- driver.Result ----

type replFakeResult struct{}

func (*replFakeResult) LastInsertId() (int64, error) { return 0, nil }
func (*replFakeResult) RowsAffected() (int64, error) { return 1, nil }
