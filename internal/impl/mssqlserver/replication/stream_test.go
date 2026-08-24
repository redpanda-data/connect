// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/require"
)

func TestResolveCaptureInstance(t *testing.T) {
	tests := []struct {
		name         string
		schema       string
		table        string
		instances    []captureInstance
		override     string
		wantInstance string
		warnContains string
		errContains  string
	}{
		{
			name:        "no instances",
			schema:      "dbo",
			table:       "orders",
			errContains: "no change table found for table 'dbo.orders'",
		},
		{
			name:         "single instance resolves regardless of naming",
			schema:       "dbo",
			table:        "orders",
			instances:    []captureInstance{{name: "OracleGG_914102297", startLSN: LSN{0x01}}},
			wantInstance: "OracleGG_914102297",
		},
		{
			name:         "single instance ignores an unrelated override",
			schema:       "dbo",
			table:        "orders",
			instances:    []captureInstance{{name: "the_only_instance", startLSN: LSN{0x01}}},
			override:     "some_unrelated_override",
			wantInstance: "the_only_instance",
		},
		{
			name:   "two instances, convention-named preferred with warning",
			schema: "dbo",
			table:  "orders",
			instances: []captureInstance{
				{name: "dbo_orders", startLSN: LSN{0x01}},
				{name: "migration_temp_instance", startLSN: LSN{0x02}},
			},
			wantInstance: "dbo_orders",
			warnContains: "preferring the default-named instance 'dbo_orders'",
		},
		{
			name:   "two instances, override wins over convention name",
			schema: "dbo",
			table:  "orders",
			instances: []captureInstance{
				{name: "dbo_orders", startLSN: LSN{0x01}},
				{name: "migration_v2", startLSN: LSN{0x02}},
			},
			override:     "migration_v2",
			wantInstance: "migration_v2",
		},
		{
			name:   "two instances, override matching neither falls back to convention name with warning",
			schema: "dbo",
			table:  "orders",
			instances: []captureInstance{
				{name: "dbo_orders", startLSN: LSN{0x01}},
				{name: "migration_v2", startLSN: LSN{0x02}},
			},
			override:     "typo_instance",
			wantInstance: "dbo_orders",
			warnContains: "configured capture_instance 'typo_instance' does not match either, falling back to the default-named instance 'dbo_orders'",
		},
		{
			name:   "two instances, neither convention-named, no override: unresolvable",
			schema: "dbo",
			table:  "orders",
			instances: []captureInstance{
				{name: "instance_alpha", startLSN: LSN{0x01}},
				{name: "instance_beta", startLSN: LSN{0x02}},
			},
			errContains: "unable to determine which one to stream from",
		},
		{
			name:   "two instances, neither convention-named, override matching neither: unresolvable",
			schema: "dbo",
			table:  "orders",
			instances: []captureInstance{
				{name: "instance_alpha", startLSN: LSN{0x01}},
				{name: "instance_beta", startLSN: LSN{0x02}},
			},
			override:    "typo_instance",
			errContains: "configured capture_instance 'typo_instance' does not match either",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logBuf bytes.Buffer
			log := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&logBuf, nil)))

			tbl := &UserDefinedTable{Schema: test.schema, Name: test.table}
			err := resolveCaptureInstance(tbl, test.instances, test.override, log)

			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantInstance, tbl.CaptureInstance)

			if test.warnContains != "" {
				require.Contains(t, logBuf.String(), test.warnContains)
			} else {
				require.Empty(t, logBuf.String())
			}
		})
	}
}
