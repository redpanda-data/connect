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

	tests := []struct {
		name        string
		conf        string
		errContains string
	}{
		{
			// Incremental snapshot only advances on a streamed commit, so
			// disabling heartbeats leaves a quiet table stalled forever.
			name: "incremental snapshot enabled with heartbeats disabled",
			conf: base + `
heartbeat_interval: 0s
incremental_snapshot:
  enabled: true
  checkpoint_cache: snapshot_cache
`,
			errContains: "heartbeat_interval is disabled",
		},
		{
			name: "incremental snapshot enabled with a heartbeat interval",
			conf: base + `
heartbeat_interval: 5s
incremental_snapshot:
  enabled: true
  checkpoint_cache: snapshot_cache
`,
		},
		{
			// A long interval only throttles the backfill, so it warns
			// rather than failing.
			name: "incremental snapshot enabled at the default heartbeat interval",
			conf: base + `
incremental_snapshot:
  enabled: true
  checkpoint_cache: snapshot_cache
`,
		},
		{
			// The dependency is only real when the snapshot is running.
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

			mgr := service.MockResources(service.MockResourcesOptAddCache("snapshot_cache"))
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
