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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSessionManager(t *testing.T) {
	t.Run("Age", func(t *testing.T) {
		cfg := NewDefaultConfig()
		sm := NewSessionManager(cfg, service.NewLoggerFromSlog(slog.Default()))

		assert.Equal(t, time.Duration(0), sm.Age(), "age must be zero before any session has been opened")

		files := []*LogFile{{FileName: "redo01.log", FirstSCN: 1, NextSCN: 1000, Sequence: 1, Type: "ONLINE", Thread: 1}}
		conn, _ := newFakeSQLConn(t, files)

		require.NoError(t, sm.AddLogFile(t.Context(), conn, files))
		assert.GreaterOrEqual(t, sm.Age(), time.Duration(0), "age must be non-negative once a session is active")

		firstAge := sm.Age()
		time.Sleep(5 * time.Millisecond)
		assert.Greater(t, sm.Age(), firstAge, "age must keep increasing while the session remains open")

		require.NoError(t, sm.EndSession(t.Context(), conn))
		assert.Equal(t, time.Duration(0), sm.Age(), "age must reset to zero once the session has ended")
	})

	t.Run("PrepareLogsAndStartSessionRestartsOnSessionMaxAge", func(t *testing.T) {
		cfg := NewDefaultConfig()
		cfg.SessionMaxAge = 20 * time.Millisecond

		logger := service.NewLoggerFromSlog(slog.Default())
		lm := &LogMiner{
			cfg:          cfg,
			sessionMgr:   NewSessionManager(cfg, logger),
			logCollector: NewLogFileCollector(),
			log:          logger,
		}

		files := []*LogFile{{FileName: "redo01.log", FirstSCN: 1, NextSCN: 1000, Sequence: 1, Type: "ONLINE", Thread: 1}}
		conn, fc := newFakeSQLConn(t, files)

		require.NoError(t, lm.prepareLogsAndStartSession(t.Context(), conn, 100, 200))
		require.True(t, lm.sessionMgr.IsActive())
		require.Equal(t, 1, fc.count("ADD_LOGFILE"), "first call has no prior session, so it must add log files")
		require.Equal(t, 0, fc.count("END_LOGMNR"), "first call has no prior session, so there is nothing to end")

		// Artificially age the session well past SessionMaxAge without changing the
		// underlying log files - the only thing that can trigger a restart here is age.
		lm.sessionMgr.sessionOpened = time.Now().Add(-time.Hour)

		require.NoError(t, lm.prepareLogsAndStartSession(t.Context(), conn, 200, 300))

		assert.Equal(t, 1, fc.count("END_LOGMNR"),
			"a stale session must be explicitly ended even though the log file set is unchanged")
		assert.Equal(t, 2, fc.count("ADD_LOGFILE"),
			"log files must be reloaded when the session is restarted due to age")
		assert.Equal(t, 2, fc.count("START_LOGMNR"),
			"a new mining session must be started after the restart")
		assert.Less(t, lm.sessionMgr.Age(), 5*time.Second,
			"AddLogFile must have refreshed sessionOpened, resetting the reported session age")
	})

	t.Run("PrepareLogsAndStartSessionDefaultSessionMaxAgeDoesNotRestart", func(t *testing.T) {
		cfg := NewDefaultConfig()
		require.Equal(t, time.Duration(0), cfg.SessionMaxAge, "default SessionMaxAge must remain disabled")

		logger := service.NewLoggerFromSlog(slog.Default())
		lm := &LogMiner{
			cfg:          cfg,
			sessionMgr:   NewSessionManager(cfg, logger),
			logCollector: NewLogFileCollector(),
			log:          logger,
		}

		files := []*LogFile{{FileName: "redo01.log", FirstSCN: 1, NextSCN: 1000, Sequence: 1, Type: "ONLINE", Thread: 1}}
		conn, fc := newFakeSQLConn(t, files)

		require.NoError(t, lm.prepareLogsAndStartSession(t.Context(), conn, 100, 200))
		require.Equal(t, 1, fc.count("ADD_LOGFILE"))

		// Artificially age the session far beyond any sane session_max_age value,
		// with the log file set left unchanged. With SessionMaxAge disabled this
		// must have no effect on whether a restart occurs.
		lm.sessionMgr.sessionOpened = time.Now().Add(-24 * time.Hour)
		staleOpened := lm.sessionMgr.sessionOpened

		require.NoError(t, lm.prepareLogsAndStartSession(t.Context(), conn, 200, 300))

		assert.Equal(t, 0, fc.count("END_LOGMNR"),
			"session must not be ended when SessionMaxAge is disabled (0)")
		assert.Equal(t, 1, fc.count("ADD_LOGFILE"),
			"log files must not be reloaded when neither the file set changed nor SessionMaxAge is exceeded")
		assert.Equal(t, staleOpened, lm.sessionMgr.sessionOpened,
			"sessionOpened must be untouched since AddLogFile was not re-invoked")
		assert.Equal(t, 2, fc.count("START_LOGMNR"),
			"StartSession is always invoked on every call regardless of a restart")
	})
}

// --- fake database/sql driver used to exercise SessionManager/LogMiner
// session logic without a real Oracle connection. Only the subset of
// database/sql/driver behaviour exercised by prepareLogsAndStartSession
// (querying V$LOGMNR-style log file listings and executing DBMS_LOGMNR
// PL/SQL blocks) is implemented. ---

var fakeDriverSeq atomic.Int64

func newFakeSQLConn(t *testing.T, files []*LogFile) (*sql.Conn, *fakeConn) {
	t.Helper()

	fc := &fakeConn{logFiles: files}
	name := fmt.Sprintf("fakeoracle_%d", fakeDriverSeq.Add(1))
	sql.Register(name, &fakeDriver{conn: fc})

	db, err := sql.Open(name, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn, fc
}

type fakeDriver struct {
	conn *fakeConn
}

func (d *fakeDriver) Open(string) (driver.Conn, error) {
	return d.conn, nil
}

type fakeConn struct {
	mu       sync.Mutex
	logFiles []*LogFile
	execs    []string
}

func (c *fakeConn) count(substr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.execs {
		if strings.Contains(e, substr) {
			n++
		}
	}
	return n
}

func (*fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fakeConn: Prepare not supported, expected ExecContext/QueryContext usage")
}

func (*fakeConn) Close() error { return nil }

func (*fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fakeConn: transactions not supported")
}

func (c *fakeConn) QueryContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &fakeRows{files: c.logFiles}, nil
}

func (c *fakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	return driver.ResultNoRows, nil
}

// fakeRows implements driver.Rows over the LogFileCollector.GetLogsBySCNRange
// column set: FILE_NAME, FIRST_CHANGE, NEXT_CHANGE, SEQ, TYPE, THREAD.
type fakeRows struct {
	files []*LogFile
	idx   int
}

func (*fakeRows) Columns() []string {
	return []string{"FILE_NAME", "FIRST_CHANGE", "NEXT_CHANGE", "SEQ", "TYPE", "THREAD"}
}

func (*fakeRows) Close() error { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.files) {
		return io.EOF
	}
	f := r.files[r.idx]
	dest[0] = f.FileName
	dest[1] = int64(f.FirstSCN)
	dest[2] = int64(f.NextSCN)
	dest[3] = f.Sequence
	dest[4] = f.Type
	dest[5] = int64(f.Thread)
	r.idx++
	return nil
}
