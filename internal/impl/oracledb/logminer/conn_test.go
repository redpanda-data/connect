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
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/sijms/go-ora/v2"
)

func TestIsSocketTimeout(t *testing.T) {
	readTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	writeTimeout := &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	connReset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "socket read timeout", err: readTimeout, want: true},
		{name: "wrapped socket read timeout", err: fmt.Errorf("mining logs: fetching current SCN: %w", readTimeout), want: true},
		{name: "socket write timeout", err: writeTimeout, want: true},
		{name: "dial timeout means unreachable, not unresponsive", err: dialTimeout, want: false},
		{name: "wrapped dial timeout", err: fmt.Errorf("validating connection to oracle database: %w", dialTimeout), want: false},
		{name: "connection reset fails fast so is not a timeout", err: connReset, want: false},
		{name: "context deadline is not a socket timeout", err: fmt.Errorf("query: %w", context.DeadlineExceeded), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "unrelated error", err: errors.New("ORA-01291: missing logfile"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSocketTimeout(tt.err))
		})
	}
}

// TestIsSocketTimeoutFromDriver checks that go-ora reports its TIMEOUT firing
// as an error IsSocketTimeout recognises, so a driver upgrade that wraps the
// error differently fails here rather than silently disabling the
// unresponsive-connection handling.
func TestIsSocketTimeoutFromDriver(t *testing.T) {
	// A server that accepts connections and never replies, like a database
	// that has stopped responding without closing the connection.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	t.Cleanup(func() {
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
	})

	db, err := sql.Open("oracle", fmt.Sprintf("oracle://user:pass@%s/svc?TIMEOUT=1", ln.Addr()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	start := time.Now()
	err = db.PingContext(ctx)
	require.Error(t, err)
	assert.Less(t, time.Since(start), 5*time.Second, "TIMEOUT=1 should fail the ping within a couple of seconds")
	assert.True(t, IsSocketTimeout(err), "expected a socket timeout, got %T: %v", err, err)
}
