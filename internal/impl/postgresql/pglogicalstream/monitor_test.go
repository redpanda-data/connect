// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
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
