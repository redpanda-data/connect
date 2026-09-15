// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckSingleThreadForLogCountStrategy(t *testing.T) {
	t.Run("single thread is allowed", func(t *testing.T) {
		o := &oracleDBCDCInput{db: newThreadGuardFakeDB(t, 1, nil)}
		require.NoError(t, o.checkSingleThreadForLogCountStrategy(t.Context()))
	})

	t.Run("multi-thread database refuses to start", func(t *testing.T) {
		o := &oracleDBCDCInput{db: newThreadGuardFakeDB(t, 3, nil)}
		err := o.checkSingleThreadForLogCountStrategy(t.Context())
		require.Error(t, err)
		require.ErrorContains(t, err, "log_count")
		require.ErrorContains(t, err, "3 open redo threads")
		require.ErrorContains(t, err, "scn_window")
	})

	t.Run("query failure is wrapped, not swallowed", func(t *testing.T) {
		o := &oracleDBCDCInput{db: newThreadGuardFakeDB(t, 0, errors.New("ORA-00942: table or view does not exist"))}
		err := o.checkSingleThreadForLogCountStrategy(t.Context())
		require.Error(t, err)
		require.ErrorContains(t, err, "checking open redo thread count")
		require.ErrorContains(t, err, "ORA-00942")
	})
}

var threadGuardFakeDriverSeq atomic.Int64

func newThreadGuardFakeDB(t *testing.T, openThreads int, queryErr error) *sql.DB {
	t.Helper()

	name := fmt.Sprintf("fakeoracle_threadguard_%d", threadGuardFakeDriverSeq.Add(1))
	sql.Register(name, &threadGuardFakeDriver{openThreads: openThreads, queryErr: queryErr})

	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type threadGuardFakeDriver struct {
	openThreads int
	queryErr    error
}

func (d *threadGuardFakeDriver) Open(string) (driver.Conn, error) {
	return &threadGuardFakeConn{openThreads: d.openThreads, queryErr: d.queryErr}, nil
}

type threadGuardFakeConn struct {
	openThreads int
	queryErr    error
}

func (*threadGuardFakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("threadGuardFakeConn: Prepare not supported, expected QueryContext usage")
}
func (*threadGuardFakeConn) Close() error { return nil }
func (*threadGuardFakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("threadGuardFakeConn: transactions not supported")
}
func (c *threadGuardFakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &threadGuardFakeCountRow{count: c.openThreads}, nil
}

type threadGuardFakeCountRow struct {
	count int
	done  bool
}

func (*threadGuardFakeCountRow) Columns() []string { return []string{"COUNT(*)"} }
func (*threadGuardFakeCountRow) Close() error      { return nil }
func (r *threadGuardFakeCountRow) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	dest[0] = int64(r.count)
	r.done = true
	return nil
}
