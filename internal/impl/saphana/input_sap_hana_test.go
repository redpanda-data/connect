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
			s := &sapHANAInput{
				schemaName: tc.schemaName,
				tableName:  tc.tableName,
			}
			require.Equal(t, tc.want, s.tableRef())
		})
	}
}
