// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestNewMonitorIntervalCheck(t *testing.T) {
	// The DSN points to a closed port. NewMonitor does not dial when there are
	// no tables, and the long WAL monitor interval stops the loop from running.
	const dsn = "postgres://user:pass@127.0.0.1:1/db?sslmode=disable"

	tests := []struct {
		name               string
		heartbeatInterval  time.Duration
		walMonitorInterval time.Duration
		wantErr            string
	}{
		{
			name:               "zero WAL monitor interval is rejected",
			heartbeatInterval:  10 * time.Second,
			walMonitorInterval: 0,
			wantErr:            "invalid monitoring interval",
		},
		{
			name:               "zero heartbeat interval is accepted",
			heartbeatInterval:  0,
			walMonitorInterval: time.Hour,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				DBConfig:           &pgconn.Config{},
				DBRawDSN:           dsn,
				HeartbeatInterval:  tc.heartbeatInterval,
				WalMonitorInterval: tc.walMonitorInterval,
			}
			m, err := NewMonitor(t.Context(), cfg, service.MockResources().Logger(), nil, "test_slot")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, m)
				return
			}
			require.NoError(t, err)
			require.NoError(t, m.Stop())
		})
	}
}

func TestMonitorUnanalysedTableRowEstimate(t *testing.T) {
	table := TableFQN{Schema: `"public"`, Table: `"cart"`}

	t.Run("a negative estimate is not cached, so it is retried", func(t *testing.T) {
		stub := &estimateStub{count: -1}
		m := newTestMonitor(t, stub)

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 100)
		assert.NotContains(t, m.Report().TableProgress, table,
			"there is no denominator yet, so nothing can be reported")

		// ANALYZE has since run.
		stub.mu.Lock()
		stub.count = 500
		stub.mu.Unlock()

		m.TrackSnapshotTable(t.Context(), table)
		m.UpdateSnapshotProgressForTable(table, 250)
		assert.InDelta(t, 0.5, m.Report().TableProgress[table], 0.0001,
			"the estimate must be picked up once it exists")
	})

	t.Run("zero is a real count and is not retried", func(t *testing.T) {
		// An empty table reports 0, which is a genuine answer rather than
		// the -1 that means unknown.
		stub := &estimateStub{count: 0}
		m := newTestMonitor(t, stub)

		m.TrackSnapshotTable(t.Context(), table)
		m.TrackSnapshotTable(t.Context(), table)
		assert.Equal(t, 1, stub.queryCount(), "a real estimate must be cached, not re-read")
	})
}

// estimateStub is a driver.Driver that answers the `SELECT reltuples ...`
// query with a single row holding its current count. Unlike fakeQueryDriver
// in incremental_snapshot_test.go, whose rows are fixed when it is built,
// this stub reads count fresh on every query under mu, so a test can flip it
// mid-test to simulate ANALYZE having since run.
type estimateStub struct {
	mu      sync.Mutex
	count   float64
	queries int
}

func (s *estimateStub) queryCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queries
}

func (s *estimateStub) Open(string) (driver.Conn, error) {
	return &estimateStubConn{stub: s}, nil
}

type estimateStubConn struct{ stub *estimateStub }

func (c *estimateStubConn) Prepare(string) (driver.Stmt, error) {
	return &estimateStubStmt{stub: c.stub}, nil
}

func (*estimateStubConn) Close() error { return nil }

func (*estimateStubConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("not implemented") }

type estimateStubStmt struct{ stub *estimateStub }

func (*estimateStubStmt) Close() error  { return nil }
func (*estimateStubStmt) NumInput() int { return -1 }

func (*estimateStubStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *estimateStubStmt) Query([]driver.Value) (driver.Rows, error) {
	return &estimateStubRows{count: s.stub.recordQuery()}, nil
}

// recordQuery counts the query and returns count as it stands right now, so
// a test can flip count mid-test and have the next query see it.
func (s *estimateStub) recordQuery() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	return s.count
}

type estimateStubRows struct {
	count float64
	read  bool
}

func (*estimateStubRows) Columns() []string { return []string{"reltuples"} }
func (*estimateStubRows) Close() error      { return nil }

func (r *estimateStubRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	dest[0] = r.count
	return nil
}

// newTestMonitor builds a Monitor around stub without dialing a real
// connection or starting the WAL-lag loop, neither of which
// readTableRowEstimate needs. loop is left nil: nothing here starts it, so a
// later reader must not call Stop on this Monitor, which would dereference
// it.
func newTestMonitor(t *testing.T, stub *estimateStub) *Monitor {
	t.Helper()

	name := fmt.Sprintf("fake_pglog_monitor_%d", fakeQueryDriverSeq.Add(1))
	sql.Register(name, stub)
	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return &Monitor{
		tableStat:        map[TableFQN]float64{},
		snapshotProgress: map[TableFQN]*atomic.Int64{},
		estimateFailed:   map[TableFQN]struct{}{},
		dbConn:           db,
		logger:           service.MockResources().Logger(),
	}
}
