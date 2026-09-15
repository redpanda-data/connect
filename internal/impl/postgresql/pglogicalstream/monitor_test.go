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
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// estimateStub answers the row-estimate query with a fixed count, or fails,
// so a monitor can be exercised without a database.
type estimateStub struct {
	mu      sync.Mutex
	count   float64
	err     error
	queries int
}

func (s *estimateStub) Open(string) (driver.Conn, error) { return &estimateConn{stub: s}, nil }

func (s *estimateStub) result() (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	return s.count, s.err
}

func (s *estimateStub) queryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queries
}

type estimateConn struct{ stub *estimateStub }

func (*estimateConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (*estimateConn) Close() error                        { return nil }
func (*estimateConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }

func (c *estimateConn) Query(string, []driver.Value) (driver.Rows, error) {
	count, err := c.stub.result()
	if err != nil {
		return nil, err
	}
	return &estimateRows{count: count}, nil
}

type estimateRows struct {
	count float64
	done  bool
}

func (*estimateRows) Columns() []string { return []string{"reltuples"} }
func (*estimateRows) Close() error      { return nil }
func (r *estimateRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.count
	return nil
}

// newTestMonitor builds a monitor with no configured tables, which is what an
// incremental snapshot driven purely by signals looks like.
func newTestMonitor(t *testing.T, stub *estimateStub) *Monitor {
	t.Helper()
	db := sql.OpenDB(stubConnector{stub: stub})
	t.Cleanup(func() { _ = db.Close() })

	return &Monitor{
		snapshotProgress: map[TableFQN]*atomic.Int64{},
		tableStat:        map[TableFQN]float64{},
		estimateFailed:   map[TableFQN]struct{}{},
		dbConn:           db,
		logger:           nil,
	}
}

type stubConnector struct{ stub *estimateStub }

func (c stubConnector) Connect(context.Context) (driver.Conn, error) {
	return &estimateConn{stub: c.stub}, nil
}
func (c stubConnector) Driver() driver.Driver { return c.stub }

func TestMonitorSnapshotTableTracking(t *testing.T) {
	table := TableFQN{Schema: `"public"`, Table: `"orders"`}

	t.Run("a table absent at construction is tracked once it starts", func(t *testing.T) {
		// The signal-driven case: NewMonitor saw no tables, so without
		// registration every update below is dropped and the operator has no
		// way to watch the backfill.
		m := newTestMonitor(t, &estimateStub{count: 100})

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 25)

		require.Contains(t, m.Report().TableProgress, table)
		assert.InDelta(t, 0.25, m.Report().TableProgress[table], 0.0001)
	})

	t.Run("tracking again does not reset progress in flight", func(t *testing.T) {
		m := newTestMonitor(t, &estimateStub{count: 100})

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 50)
		m.TrackSnapshotTable(t.Context(), table)

		assert.InDelta(t, 0.5, m.Report().TableProgress[table], 0.0001)
	})

	t.Run("an already tracked table costs no query", func(t *testing.T) {
		stub := &estimateStub{count: 100}
		m := newTestMonitor(t, stub)

		for range 5 {
			m.TrackSnapshotTable(t.Context(), table)
		}
		assert.Equal(t, 1, stub.queryCount(), "the estimate should be read once, not once per chunk")
	})

	t.Run("completion fills the progress to its total", func(t *testing.T) {
		m := newTestMonitor(t, &estimateStub{count: 100})

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 10)
		m.MarkSnapshotComplete(table)

		assert.InDelta(t, 1.0, m.Report().TableProgress[table], 0.0001)
	})

	t.Run("tracking concurrently with a report is safe", func(t *testing.T) {
		// The real shape: the monitor's periodic loop calls Report while the
		// replication stream registers each table the backfill reaches. Under
		// -race this fails if either map is touched without the lock.
		m := newTestMonitor(t, &estimateStub{count: 100})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := range 50 {
				tbl := TableFQN{Schema: `"public"`, Table: strconv.Itoa(i)}
				m.TrackSnapshotTable(t.Context(), tbl)
				m.UpdateSnapshotProgressForTable(tbl, 1)
				m.MarkSnapshotComplete(tbl)
			}
		}()
		go func() {
			defer wg.Done()
			for range 50 {
				m.Report()
			}
		}()
		wg.Wait()

		assert.Len(t, m.Report().TableProgress, 50)
	})

	t.Run("a failed estimate is retried rather than cached", func(t *testing.T) {
		// Report drops a table whose total is zero, so caching the failure
		// would lose the metric for the whole backfill.
		stub := &estimateStub{err: errors.New("connection reset")}
		m := newTestMonitor(t, stub)

		m.TrackSnapshotTable(t.Context(), table)
		assert.NotContains(t, m.Report().TableProgress, table)

		stub.mu.Lock()
		stub.err = nil
		stub.count = 200
		stub.mu.Unlock()

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 100)
		assert.InDelta(t, 0.5, m.Report().TableProgress[table], 0.0001)
	})
}
