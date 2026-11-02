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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/cenkalti/backoff/v4"
	goora "github.com/sijms/go-ora/v2/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsConnectionCapacityError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "listener has no available handler", err: goora.NewOracleError(12516), want: true},
		{name: "listener could not hand off the connection", err: goora.NewOracleError(12518), want: true},
		{name: "no appropriate service handler", err: goora.NewOracleError(12519), want: true},
		{name: "listener has no handler for server type", err: goora.NewOracleError(12520), want: true},
		{name: "maximum processes exceeded", err: goora.NewOracleError(20), want: true},
		{name: "maximum sessions exceeded", err: goora.NewOracleError(18), want: true},
		{name: "wrapped capacity error", err: fmt.Errorf("dialing: %w", goora.NewOracleError(12516)), want: true},
		{name: "connection closed during handshake", err: io.EOF, want: true},
		{name: "connection cut short during handshake", err: fmt.Errorf("reading packet: %w", io.ErrUnexpectedEOF), want: true},
		{name: "insufficient privileges", err: goora.NewOracleError(1031), want: false},
		{name: "invalid credentials", err: goora.NewOracleError(1017), want: false},
		{name: "not an Oracle error", err: errors.New("connection reset by peer"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isConnectionCapacityError(tt.err))
		})
	}
}

// noCapacityConnector is a driver.Connector whose every connection attempt fails as if
// the database had no room for another connection.
type noCapacityConnector struct{}

func (noCapacityConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, goora.NewOracleError(12516)
}

func (noCapacityConnector) Driver() driver.Driver { return nil }

func TestAcquireSnapshotConnGivesUpOnlyWithNoConnectionsHeld(t *testing.T) {
	newSnapshot := func(t *testing.T) *Snapshot {
		t.Helper()
		db := sql.OpenDB(noCapacityConnector{})
		t.Cleanup(func() { _ = db.Close() })
		connBackoff := backoff.NewExponentialBackOff()
		connBackoff.InitialInterval = time.Millisecond
		connBackoff.MaxInterval = 5 * time.Millisecond
		connBackoff.MaxElapsedTime = 20 * time.Millisecond
		return &Snapshot{dbPool: db, connBackoff: *connBackoff, log: service.MockResources().Logger()}
	}

	t.Run("no other connection held", func(t *testing.T) {
		s := newSnapshot(t)

		_, err := s.acquireSnapshotConn(t.Context())

		var oraErr *goora.OracleError
		require.ErrorAs(t, err, &oraErr, "Expected the capacity error once max elapsed time passes")
		assert.Equal(t, 12516, oraErr.ErrCode)
	})

	t.Run("another connection held", func(t *testing.T) {
		s := newSnapshot(t)
		s.connsInUse.Add(1)
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		defer cancel()

		_, err := s.acquireSnapshotConn(ctx)

		assert.ErrorIs(t, err, context.DeadlineExceeded, "Expected to keep waiting past max elapsed time while another connection is held")
	})
}
