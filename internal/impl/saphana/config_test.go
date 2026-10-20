// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package saphana

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func parseInputConf(t *testing.T, yaml string) *service.ParsedConfig {
	t.Helper()
	conf, err := sapHANAInputConfigSpec.ParseYAML(yaml, nil)
	require.NoError(t, err)
	return conf
}

func enterpriseResources() *service.Resources {
	res := service.MockResources()
	license.InjectTestService(res)
	return res
}

func TestSAPHANAInputConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		errContains string
	}{
		{
			name: "valid bulk",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
`,
		},
		{
			name: "valid query",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
query: "SELECT * FROM MY_TABLE"
`,
		},
		{
			name: "valid incrementing",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: MY_TABLE
incrementing_column: ID
`,
		},
		{
			name: "bulk without table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
`,
			errContains: "table",
		},
		{
			name: "incrementing without table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
incrementing_column: ID
`,
			errContains: "table",
		},
		{
			name: "incrementing without incrementing_column",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: MY_TABLE
`,
			errContains: "incrementing_column",
		},
		{
			name: "query without query field",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
`,
			errContains: "query",
		},
		// Fields that are inert for the selected mode are rejected rather than
		// silently ignored: the reviewer's example was an incremental-intent
		// config that ran as a one-shot bulk scan because mode defaulted.
		{
			name: "bulk (default mode) with incrementing_column",
			yaml: `
dsn: hdb://user:pass@host:39017
table: ORDERS
incrementing_column: ID
`,
			errContains: "incrementing_column",
		},
		{
			name: "bulk with checkpoint_cache",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: ORDERS
checkpoint_cache: redis_cache
`,
			errContains: "checkpoint_cache",
		},
		{
			name: "query with table",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: query
query: "SELECT 1 FROM DUMMY"
table: ORDERS
`,
			errContains: "table",
		},
		{
			name: "incrementing with timestamp_column",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: ORDERS
incrementing_column: ID
timestamp_column: TS
`,
			errContains: "timestamp_column",
		},
		{
			name: "timestamp with query",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: ORDERS
timestamp_column: TS
query: "SELECT 1 FROM DUMMY"
`,
			errContains: "query",
		},
		{
			name: "timestamp with incrementing_initial_value",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: ORDERS
timestamp_column: TS
incrementing_initial_value: "5"
`,
			errContains: "incrementing_initial_value",
		},
		// Defaulted polling fields are inert in bulk/query mode too, but only an
		// explicit non-default value signals intent, so that is what is rejected.
		{
			name: "bulk with non-default poll_interval",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: ORDERS
poll_interval: 5s
`,
			errContains: "poll_interval",
		},
		{
			name: "bulk with default poll_interval spelled out is accepted",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: ORDERS
poll_interval: 60s
`,
		},
		{
			name: "incrementing with non-default timestamp_delay",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: ORDERS
incrementing_column: ID
timestamp_delay: 30s
`,
			errContains: "timestamp_delay",
		},
		{
			name: "incrementing with non-default timestamp_clock",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: ORDERS
incrementing_column: ID
timestamp_clock: database_utc
`,
			errContains: "timestamp_clock",
		},
		{
			// Inert for a different reason than the mode, so the error says
			// what is actually missing instead of blaming the mode.
			name: "checkpoint_cache_key without checkpoint_cache",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: incrementing
table: ORDERS
incrementing_column: ID
checkpoint_cache_key: custom_key
`,
			errContains: `"checkpoint_cache_key" is set but "checkpoint_cache" is not`,
		},
		{
			name: "timestamp mode with non-default timestamp_delay is accepted",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: timestamp
table: ORDERS
timestamp_column: TS
timestamp_delay: 30s
timestamp_clock: database_utc
poll_interval: 5s
`,
		},
		{
			name: "fetch_size below one",
			yaml: `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
fetch_size: 0
`,
			errContains: "fetch_size",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := sapHANAInputConfigSpec.ParseYAML(tc.yaml, nil)
			if err != nil {
				if tc.errContains != "" {
					require.ErrorContains(t, err, tc.errContains)
					return
				}
				require.NoError(t, err)
				return
			}

			_, err = newSAPHANAInput(conf, enterpriseResources())
			if tc.errContains != "" {
				require.ErrorContains(t, err, tc.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSAPHANAInputRequiresEnterpriseLicense(t *testing.T) {
	conf := parseInputConf(t, `
dsn: hdb://user:pass@host:39017
mode: bulk
table: MY_TABLE
`)
	_, err := newSAPHANAInput(conf, service.MockResources())
	require.Error(t, err)
}

func TestSAPHANAInputTableRef(t *testing.T) {
	tests := []struct {
		name       string
		schemaName string
		tableName  string
		want       string
	}{
		{
			name:       "with schema",
			schemaName: "MY_SCHEMA",
			tableName:  "MY_TABLE",
			want:       `"MY_SCHEMA"."MY_TABLE"`,
		},
		{
			name:      "without schema",
			tableName: "MY_TABLE",
			want:      `"MY_TABLE"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := &sapHANAInputConfig{
				schemaName: tc.schemaName,
				tableName:  tc.tableName,
			}
			require.Equal(t, tc.want, c.tableRef())
		})
	}
}
