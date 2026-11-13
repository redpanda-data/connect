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
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSocketTimeout(t *testing.T) {
	readTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	connReset := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "socket read timeout", err: readTimeout, want: true},
		{name: "wrapped socket read timeout", err: fmt.Errorf("mining logs: fetching current SCN: %w", readTimeout), want: true},
		{name: "connection reset fails fast so is not a timeout", err: connReset, want: false},
		{name: "context deadline is not a socket timeout", err: fmt.Errorf("query: %w", context.DeadlineExceeded), want: false},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "unrelated error", err: errors.New("ORA-01291: missing logfile"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSocketTimeout(tt.err))
		})
	}
}
