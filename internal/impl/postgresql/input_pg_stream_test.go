// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package pgstream

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestNewPgStreamInputSignalTableName(t *testing.T) {
	env := service.NewEnvironment()
	spec := newPostgresCDCConfig()

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			name: "no signal table configured",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`,
		},
		{
			name: "signal table distinct from tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
signal_table_name: rpcn_signal_table
`,
		},
		{
			name: "signal table also listed in tables",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - rpcn_signal_table
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
		{
			name: "signal table matches tables entry under different case-folding",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
  - RPCN_SIGNAL_TABLE
signal_table_name: rpcn_signal_table
`,
			errContains: `signal_table_name "rpcn_signal_table" must not also appear in tables`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(test.conf, env)
			require.NoError(t, err)

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err = newPgStreamInput(pConf, mgr)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewPgStreamInputIncSnapshotHeartbeat(t *testing.T) {
	env := service.NewEnvironment()
	spec := newPostgresCDCConfig()

	const base = `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
tables:
  - events
`

	// ParseYAML cannot add a cache resource. The checkpoint_cache tests also
	// run after the heartbeat tests. Therefore the cases that pass look for
	// the cache error. That error shows that the checks passed the heartbeat
	// test.
	const pastHeartbeatCheck = "checkpoint_cache is required"

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			// An unreplicated table would lose writes: no live changes to
			// dedup its backfill against.
			name: "incremental snapshot table not replicated",
			conf: base + `
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
  tables:
    - events
    - customers
`,
			errContains: `"customers" is not listed in tables`,
		},
		{
			// Case-folding must match how Postgres resolves the names.
			name: "incremental snapshot table matches under case folding",
			conf: base + `
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
  tables:
    - EVENTS
`,
			errContains: pastHeartbeatCheck,
		},
		{
			// Both modes read the same rows, so together they double-deliver.
			name: "both snapshot modes enabled",
			conf: base + `
stream_snapshot: true
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
`,
			errContains: "mutually exclusive",
		},
		{
			name: "blocking snapshot alone",
			conf: base + `
stream_snapshot: true
`,
		},
		{
			// Both lists empty: no table names to read, so the snapshot
			// would silently do nothing.
			name: "incremental snapshot enabled with no tables anywhere",
			conf: `
dsn: postgres://user:pass@localhost:5432/db
slot_name: my_slot
schema: dbo
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
`,
			errContains: "no tables are listed",
		},
		{
			// An omitted list inherits the replicated tables.
			name: "incremental snapshot enabled without tables",
			conf: base + `
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
`,
			errContains: pastHeartbeatCheck,
		},
		{
			// The incremental snapshot moves forward only on a streamed
			// commit. Without a heartbeat a quiet table stops for ever.
			name: "incremental snapshot enabled with heartbeats disabled",
			conf: base + `
heartbeat_interval: 0s
incremental_snapshot:
  enabled: true
`,
			errContains: "heartbeat_interval is disabled",
		},
		{
			name: "incremental snapshot enabled with a heartbeat interval",
			conf: base + `
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
`,
			errContains: pastHeartbeatCheck,
		},
		{
			// A long interval makes the snapshot slow but does not stop
			// it. Therefore the input warns and does not fail.
			name: "incremental snapshot enabled at the default heartbeat interval",
			conf: base + `
incremental_snapshot:
  enabled: true
`,
			errContains: pastHeartbeatCheck,
		},
		{
			// This condition applies only while the snapshot runs.
			name: "heartbeats disabled with incremental snapshot disabled",
			conf: base + `
heartbeat_interval: 0s
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := spec.ParseYAML(test.conf, env)
			require.NoError(t, err)

			mgr := service.MockResources()
			license.InjectTestService(mgr)

			_, err = newPgStreamInput(pConf, mgr)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
