// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/replication/incrementalsnapshot"
)

// fakeQueryDriver is a minimal database/sql driver that ignores whatever SQL
// text it's given and always returns the canned rows/columns it was
// constructed with. It exists so tests can exercise code that queries
// *sql.DB (i.e. Stream.incrementalDB) without a real Postgres connection.
type fakeQueryDriver struct {
	columns []string
	rows    [][]driver.Value
	queries *int
}

func (d *fakeQueryDriver) Open(string) (driver.Conn, error) {
	return &fakeQueryConn{driver: d}, nil
}

type fakeQueryConn struct{ driver *fakeQueryDriver }

func (c *fakeQueryConn) Prepare(string) (driver.Stmt, error) {
	return &fakeQueryStmt{conn: c}, nil
}
func (*fakeQueryConn) Close() error              { return nil }
func (*fakeQueryConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not implemented") }

type fakeQueryStmt struct{ conn *fakeQueryConn }

func (*fakeQueryStmt) Close() error  { return nil }
func (*fakeQueryStmt) NumInput() int { return -1 }
func (*fakeQueryStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *fakeQueryStmt) Query([]driver.Value) (driver.Rows, error) {
	if s.conn.driver.queries != nil {
		*s.conn.driver.queries++
	}
	return &fakeQueryRows{columns: s.conn.driver.columns, rows: s.conn.driver.rows}, nil
}

type fakeQueryRows struct {
	columns []string
	rows    [][]driver.Value
	idx     int
}

func (r *fakeQueryRows) Columns() []string { return r.columns }
func (*fakeQueryRows) Close() error        { return nil }
func (r *fakeQueryRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

var fakeQueryDriverSeq atomic.Int64

// newFakeQueryDB registers a fresh fakeQueryDriver under a unique name (since
// sql.Register panics on reuse) and opens a *sql.DB backed by it. If queries
// is non-nil it's incremented once per Query call, letting tests assert on
// query counts (e.g. to prove caching avoids a repeat round trip).
func newFakeQueryDB(t *testing.T, columns []string, rows [][]driver.Value, queries *int) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("fake_pglog_test_%d", fakeQueryDriverSeq.Add(1))
	sql.Register(name, &fakeQueryDriver{columns: columns, rows: rows, queries: queries})
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestResolveIncrementalPKColumnsUsesIncrementalDB(t *testing.T) {
	// pgConn is deliberately left nil: if resolveIncrementalPKColumns (or
	// anything it calls) touched s.pgConn instead of s.incrementalDB, this
	// would panic with a nil pointer dereference rather than returning a
	// result -- this is precisely the deadlock/crash bug being guarded
	// against, since s.pgConn is occupied by the replication protocol once
	// streaming has started.
	db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"tenant_id"}, {"id"}}, nil)
	s := &Stream{incSnapshotConn: db}

	cols, err := s.resolveIncrementalPKColumns(context.Background(), TableFQN{Schema: `"public"`, Table: `"orders"`})
	require.NoError(t, err)
	assert.Equal(t, []string{`"tenant_id"`, `"id"`}, cols)
}

func TestResolveIncrementalPKColumnsNoPrimaryKey(t *testing.T) {
	db := newFakeQueryDB(t, []string{"attname"}, nil, nil)
	s := &Stream{incSnapshotConn: db}

	_, err := s.resolveIncrementalPKColumns(context.Background(), TableFQN{Schema: `"public"`, Table: `"orders"`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no primary key found")
}

func TestIncrementalPKColumnsCachesAndUnquotes(t *testing.T) {
	queries := 0
	// pgConn is left nil for the same reason as above: incrementalPKColumns
	// backs both the coordinator's PK resolution and live DML dedup lookups,
	// either of which may run concurrently with replication streaming.
	db := newFakeQueryDB(t, []string{"attname"}, [][]driver.Value{{"id"}}, &queries)
	s := &Stream{
		incSnapshotConn:    db,
		incSnapshotPKCache: make(map[string][]string),
	}

	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}

	cols, err := s.incrementalPKColumns(context.Background(), table)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, cols, "cached columns must be unquoted")
	assert.Equal(t, 1, queries)

	// Second call for the same table must be served from the cache, not
	// issue a second query.
	cols, err = s.incrementalPKColumns(context.Background(), table)
	require.NoError(t, err)
	assert.Equal(t, []string{"id"}, cols)
	assert.Equal(t, 1, queries, "second lookup for the same table must be cached")
}

func TestResolveIncrementalMaxKeyEmptyTableIsNotAnError(t *testing.T) {
	// Zero rows means the table currently has nothing to backfill; this must
	// be reported as (nil, nil), not an error that aborts the whole stream.
	db := newFakeQueryDB(t, []string{"id"}, nil, nil)
	s := &Stream{incSnapshotConn: db}

	table := incrementalsnapshot.TableID{Schema: "public", Table: "orders"}
	pk, err := s.resolveIncrementalMaxKey(context.Background(), table, []string{"id"}, "SELECT id FROM orders")
	require.NoError(t, err)
	assert.Nil(t, pk)
}

func TestCanonicalizePKValue(t *testing.T) {
	id := uuid.New()

	t.Run("binary uuid normalizes to canonical string", func(t *testing.T) {
		assert.Equal(t, id.String(), canonicalizePKValue([16]byte(id)))
	})

	t.Run("byte slice normalizes to string", func(t *testing.T) {
		assert.Equal(t, "hello", canonicalizePKValue([]byte("hello")))
	})

	t.Run("other types pass through unchanged", func(t *testing.T) {
		assert.Equal(t, int32(5), canonicalizePKValue(int32(5)))
		assert.Nil(t, canonicalizePKValue(nil))
	})

	t.Run("both decode paths produce the same canonical value for the same uuid", func(t *testing.T) {
		// Mirrors the two real decode paths for a uuid PK column: the live
		// streaming path (decodeTextColumnData) already normalizes to
		// uuid.UUID.String(), while the incrementalDB backfill path
		// (prepareScannersAndGetters) may hand back a plain string too, but
		// canonicalizePKValue must treat a raw [16]byte the same as its
		// string form regardless of which path produced it.
		fromStreamingPath := canonicalizePKValue(id.String())
		fromBackfillPath := canonicalizePKValue([16]byte(id))
		assert.Equal(t, fromStreamingPath, fromBackfillPath)
	})
}

// TestCanonicalizePKValueDedupsAcrossDecodePaths proves the fix end-to-end
// against the dedup window: without canonicalizing PK values before they
// reach incrementalsnapshot.PrimaryKey, a UUID primary key decoded as a raw
// [16]byte on one path and a canonical string on the other would never
// dedup, since the window's key is built by directly formatting each
// PrimaryKey element.
func TestCanonicalizePKValueDedupsAcrossDecodePaths(t *testing.T) {
	table := incrementalsnapshot.TableID{Schema: "public", Table: "widgets"}
	id := uuid.New()

	window := incrementalsnapshot.NewWindowBuffer()

	// Simulates a row buffered by the incrementalDB backfill path.
	backfillPK := incrementalsnapshot.PrimaryKey{canonicalizePKValue([16]byte(id))}
	window.Add(incrementalsnapshot.Row{Table: table, PK: backfillPK, Data: map[string]any{"id": id.String()}})
	require.Equal(t, 1, window.Len())

	// Simulates the same row arriving via the live streaming decode path.
	streamedPK := incrementalsnapshot.PrimaryKey{canonicalizePKValue(id.String())}
	removed := window.Remove(table, streamedPK)
	assert.True(t, removed, "the same uuid decoded via either path must dedup to the same window key")
	assert.Equal(t, 0, window.Len())
}
