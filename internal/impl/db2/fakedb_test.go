// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

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

// Fake SQL driver for unit tests in the db2 package.
// Registered once as "fake-db2-input" to avoid conflicts with the real "db2-cli" driver.

var (
	inputFakeDrvOnce sync.Once
	inputFakeConns   sync.Map
	inputFakeConnSeq atomic.Int64
)

type inputFakeHandlers struct {
	query func(query string, args []driver.Value) (columns []string, rows [][]driver.Value, err error)
	exec  func(query string, args []driver.Value) error
}

func openInputFakeDB(t *testing.T, h *inputFakeHandlers) *sql.DB {
	t.Helper()
	inputFakeDrvOnce.Do(func() { sql.Register("fake-db2-input", &inputFakeDriver{}) })
	dsn := fmt.Sprintf("input-fake-%d", inputFakeConnSeq.Add(1))
	inputFakeConns.Store(dsn, h)
	db, err := sql.Open("fake-db2-input", dsn)
	if err != nil {
		t.Fatalf("openInputFakeDB: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() {
		_ = db.Close()
		inputFakeConns.Delete(dsn)
	})
	return db
}

type inputFakeDriver struct{}

func (*inputFakeDriver) Open(dsn string) (driver.Conn, error) {
	v, ok := inputFakeConns.Load(dsn)
	if !ok {
		return nil, fmt.Errorf("fake-db2-input: no handlers for DSN %q", dsn)
	}
	return &inputFakeConn{h: v.(*inputFakeHandlers)}, nil
}

type inputFakeConn struct{ h *inputFakeHandlers }

func (c *inputFakeConn) Prepare(query string) (driver.Stmt, error) {
	return &inputFakeStmt{c: c, q: query}, nil
}
func (*inputFakeConn) Close() error              { return nil }
func (*inputFakeConn) Begin() (driver.Tx, error) { return &inputFakeTx{}, nil }
func (*inputFakeConn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &inputFakeTx{}, nil
}

type inputFakeTx struct{}

func (*inputFakeTx) Commit() error   { return nil }
func (*inputFakeTx) Rollback() error { return nil }

type inputFakeStmt struct {
	c *inputFakeConn
	q string
}

func (*inputFakeStmt) Close() error  { return nil }
func (*inputFakeStmt) NumInput() int { return -1 }

func (s *inputFakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.c.h.exec != nil {
		return &inputFakeResult{}, s.c.h.exec(s.q, args)
	}
	return &inputFakeResult{}, nil
}

func (s *inputFakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.c.h.query == nil {
		return &inputFakeRows{}, nil
	}
	cols, data, err := s.c.h.query(s.q, args)
	if err != nil {
		return nil, err
	}
	return &inputFakeRows{cols: cols, data: data}, nil
}

type inputFakeRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *inputFakeRows) Columns() []string { return r.cols }
func (*inputFakeRows) Close() error        { return nil }

func (r *inputFakeRows) Next(dest []driver.Value) error {
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

type inputFakeResult struct{}

func (*inputFakeResult) LastInsertId() (int64, error) { return 0, nil }
func (*inputFakeResult) RowsAffected() (int64, error) { return 1, nil }
