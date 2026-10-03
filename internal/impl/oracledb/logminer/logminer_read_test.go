// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

type rowsDriver struct {
	total    int
	csfEvery int // when > 0, rows (1,2), (3,4), ... are 2-fragment statements
	failAt   int // Next returns errFetch after this many rows when > 0
	closed   *atomic.Bool
}

var errFetch = errors.New("fetch failed")

func (d *rowsDriver) Open(string) (driver.Conn, error) { return &rowsConn{d: d}, nil }

type rowsConn struct{ d *rowsDriver }

func (c *rowsConn) Prepare(string) (driver.Stmt, error) { return &rowsStmt{d: c.d}, nil }
func (*rowsConn) Close() error                          { return nil }
func (*rowsConn) Begin() (driver.Tx, error)             { return nil, errors.New("unsupported") }

type rowsStmt struct{ d *rowsDriver }

func (*rowsStmt) NumInput() int                               { return -1 }
func (*rowsStmt) Close() error                                { return nil }
func (*rowsStmt) Exec([]driver.Value) (driver.Result, error)  { return nil, errors.New("unsupported") }
func (s *rowsStmt) Query([]driver.Value) (driver.Rows, error) { return &genRows{d: s.d}, nil }

type genRows struct {
	d *rowsDriver
	n int
}

func (*genRows) Columns() []string {
	return []string{"SCN", "SQL_REDO", "OPERATION_CODE", "TABLE_NAME", "SEG_OWNER", "TIMESTAMP", "XID", "COMMIT_SCN", "CSF", "USERNAME"}
}

func (r *genRows) Close() error { r.d.closed.Store(true); return nil }

// Row n has SCN n+1. With csfEvery, odd rows start a statement (CSF=1) that the
// following even row completes.
func (r *genRows) Next(dest []driver.Value) error {
	if r.d.failAt > 0 && r.n >= r.d.failAt {
		return errFetch
	}
	if r.n >= r.d.total {
		return io.EOF
	}
	n := r.n
	r.n++
	csf, sqlText, scn := int64(0), fmt.Sprintf("r%d", n), int64(n+1)
	if r.d.csfEvery > 0 && n > 0 {
		if n%2 == 1 {
			csf, sqlText = 1, fmt.Sprintf("r%d-a", n)
		} else {
			sqlText = fmt.Sprintf("r%d-b", n)
		}
	}
	copy(dest, []driver.Value{scn, sqlText, int64(1), "T", "S", time.Now(), []byte("xidxidxi"), nil, csf, "u"})
	return nil
}

func newRowsMiner(t *testing.T, d *rowsDriver) (*LogMiner, *sql.Conn) {
	t.Helper()
	d.closed = new(atomic.Bool)
	name := fmt.Sprintf("rowsdriver_%d", fakeDriverSeq.Add(1))
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	lm := NewMiner(nil, []replication.UserTable{{Schema: "S", Name: "T"}}, &publisherStub{}, NewDefaultConfig(), nil, service.MockResources().Metrics(), service.NewLoggerFromSlog(slog.Default()))
	return lm, conn
}

func TestQueryLogMinerContentsOrderingAcrossBatches(t *testing.T) {
	// Not a multiple of the batch size; CSF pairs straddle batch boundaries.
	total := logMinerReadBatchSize*(logMinerReadAheadBatches+3) + 1
	lm, conn := newRowsMiner(t, &rowsDriver{total: total, csfEvery: 1})

	var got []string
	last, err := lm.queryLogMinerContents(t.Context(), conn, 0, 1<<40, 1, func(_ context.Context, ev *sqlredo.RedoEvent) error {
		got = append(got, ev.SQLRedo.String)
		return nil
	})
	require.NoError(t, err)
	// Row 0 is plain; pairs (1,2)...(total-2,total-1) join, including (499,500)
	// which straddles a batch boundary.
	require.Len(t, got, 1+total/2)
	assert.Equal(t, "r0", got[0])
	assert.Equal(t, "r1-ar2-b", got[1])
	assert.Equal(t, fmt.Sprintf("r%d-ar%d-b", total-2, total-1), got[len(got)-1])
	assert.Equal(t, uint64(total-1), last)
}

func TestQueryLogMinerContentsProcessErrorStopsReader(t *testing.T) {
	d := &rowsDriver{total: 100000}
	lm, conn := newRowsMiner(t, d)

	boom := errors.New("boom")
	calls := 0
	last, err := lm.queryLogMinerContents(t.Context(), conn, 0, 1<<40, 1, func(context.Context, *sqlredo.RedoEvent) error {
		calls++
		if calls == 10 {
			return boom
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
	assert.Equal(t, uint64(9), last)
	assert.True(t, d.closed.Load(), "rows must be closed before returning")
}

func TestQueryLogMinerContentsFetchErrorUnchanged(t *testing.T) {
	d := &rowsDriver{total: 100000, failAt: logMinerReadBatchSize + 7}
	lm, conn := newRowsMiner(t, d)

	var n int
	last, err := lm.queryLogMinerContents(t.Context(), conn, 0, 1<<40, 1, func(context.Context, *sqlredo.RedoEvent) error {
		n++
		return nil
	})
	require.ErrorIs(t, err, errFetch)
	assert.Equal(t, logMinerReadBatchSize+7, n, "rows before the failure must all be processed")
	assert.Equal(t, uint64(n), last)
	assert.True(t, d.closed.Load())
}

func TestQueryLogMinerContentsContextCancel(t *testing.T) {
	d := &rowsDriver{total: 1 << 30}
	lm, conn := newRowsMiner(t, d)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := lm.queryLogMinerContents(ctx, conn, 0, 1<<40, 1, func(context.Context, *sqlredo.RedoEvent) error {
		cancel()
		return ctx.Err()
	})
	require.Error(t, err)
	assert.True(t, d.closed.Load())
}
