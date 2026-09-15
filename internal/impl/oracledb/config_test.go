// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestBuildConnectionURL(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		overrides   map[string]string
		wantHost    string
		wantUser    string
		wantPass    string
		wantPath    string
		wantQuery   url.Values
		errContains string
	}{
		{
			name:     "standard URL round-trips correctly",
			input:    "oracle://user:pass@localhost:1521/myservice",
			wantHost: "localhost:1521",
			wantUser: "user",
			wantPass: "pass",
			wantPath: "/myservice",
		},
		{
			name:     "default port 1521 when port is omitted",
			input:    "oracle://user:pass@localhost/myservice",
			wantHost: "localhost:1521",
			wantUser: "user",
			wantPass: "pass",
			wantPath: "/myservice",
		},
		{
			name:     "special characters in credentials are preserved",
			input:    "oracle://us%40er:p%40ss%3Aword@localhost:1521/myservice",
			wantHost: "localhost:1521",
			wantUser: "us@er",
			wantPass: "p@ss:word",
			wantPath: "/myservice",
		},
		{
			name:      "existing query params are carried through",
			input:     "oracle://user:pass@localhost:1521/myservice?ssl=true&timeout=30",
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"ssl": {"true"}, "timeout": {"30"}},
		},
		{
			name:     "no credentials does not panic",
			input:    "oracle://localhost:1521/myservice",
			wantHost: "localhost:1521",
			wantPath: "/myservice",
		},
		{
			name:        "invalid port returns error",
			input:       "oracle://user:pass@host:notaport/svc",
			errContains: "invalid port",
		},
		{
			name:        "JDBC scheme is rejected",
			input:       "jdbc:oracle:thin:@//localhost:1521/myservice",
			errContains: `unsupported connection string scheme "jdbc"`,
		},
		// override tests
		{
			name:      "ssl override is injected",
			input:     "oracle://user:pass@localhost:1521/myservice",
			overrides: map[string]string{"SSL": "true"},
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"SSL": {"true"}},
		},
		{
			name:      "ssl_verify override is injected",
			input:     "oracle://user:pass@localhost:1521/myservice",
			overrides: map[string]string{"SSL VERIFY": "false"},
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"SSL VERIFY": {"false"}},
		},
		{
			name:      "override wins over matching param in connection string",
			input:     "oracle://user:pass@localhost:1521/myservice?SSL=false",
			overrides: map[string]string{"SSL": "true"},
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"SSL": {"true"}},
		},
		// wallet tests
		{
			name:      "wallet path injects WALLET and SSL params",
			input:     "oracle://user:pass@localhost:1521/myservice",
			overrides: map[string]string{"WALLET": "/opt/oracle/wallet", "SSL": "true"},
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"WALLET": {"/opt/oracle/wallet"}, "SSL": {"true"}},
		},
		{
			name:      "wallet path with password injects WALLET, WALLET PASSWORD and SSL params",
			input:     "oracle://user:pass@localhost:1521/myservice",
			overrides: map[string]string{"WALLET": "/opt/oracle/wallet", "WALLET PASSWORD": "s3cr3t", "SSL": "true"},
			wantHost:  "localhost:1521",
			wantUser:  "user",
			wantPass:  "pass",
			wantPath:  "/myservice",
			wantQuery: url.Values{"WALLET": {"/opt/oracle/wallet"}, "WALLET PASSWORD": {"s3cr3t"}, "SSL": {"true"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildConnectionString(tt.input, tt.overrides, service.MockResources().Logger())
			if tt.errContains != "" {
				require.ErrorContains(t, err, tt.errContains)
				return
			}
			require.NoError(t, err)

			parsed, err := url.Parse(result)
			require.NoError(t, err)

			assert.Equal(t, tt.wantHost, parsed.Host)
			assert.Equal(t, tt.wantPath, parsed.Path)

			if tt.wantUser != "" || tt.wantPass != "" {
				require.NotNil(t, parsed.User)
				assert.Equal(t, tt.wantUser, parsed.User.Username())
				gotPass, _ := parsed.User.Password()
				assert.Equal(t, tt.wantPass, gotPass)
			} else if parsed.User != nil {
				assert.Empty(t, parsed.User.Username())
			}

			gotQuery := parsed.Query()
			for key, wantVals := range tt.wantQuery {
				assert.Equal(t, wantVals, gotQuery[key], "query param %q", key)
			}
		})
	}
}

func TestParseSnapshotMode(t *testing.T) {
	const minimalOracleCDCYAML = `connection_string: oracle://user:pass@host:1521/svc
include:
  - SCHEMA.TABLE
logminer: {}
`
	tests := []struct {
		name string
		yaml string
		want SnapshotMode
	}{
		{
			name: "omitted defaults to none",
			yaml: minimalOracleCDCYAML,
			want: SnapshotModeNone,
		},
		{
			name: "explicit none",
			yaml: minimalOracleCDCYAML + "snapshot_mode: none\n",
			want: SnapshotModeNone,
		},
		{
			name: "snapshot_only",
			yaml: minimalOracleCDCYAML + "snapshot_mode: snapshot_only\n",
			want: SnapshotModeSnapshotOnly,
		},
		{
			name: "snapshot_and_stream",
			yaml: minimalOracleCDCYAML + "snapshot_mode: snapshot_and_stream\n",
			want: SnapshotModeSnapshotAndStream,
		},
		{
			// backward compat: stream_snapshot: true with no snapshot_mode set
			name: "stream_snapshot true upgrades to snapshot_and_stream",
			yaml: minimalOracleCDCYAML + "stream_snapshot: true\n",
			want: SnapshotModeSnapshotAndStream,
		},
		{
			// explicit snapshot_mode: none must win over stream_snapshot: true
			name: "explicit snapshot_mode none overrides stream_snapshot true",
			yaml: minimalOracleCDCYAML + "snapshot_mode: none\nstream_snapshot: true\n",
			want: SnapshotModeNone,
		},
		{
			// explicit snapshot_mode wins over stream_snapshot: true
			name: "explicit snapshot_mode snapshot_only overrides stream_snapshot true",
			yaml: minimalOracleCDCYAML + "snapshot_mode: snapshot_only\nstream_snapshot: true\n",
			want: SnapshotModeSnapshotOnly,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := oracleDBStreamConfigSpec.ParseYAML(tt.yaml, nil)
			require.NoError(t, err)
			got, err := parseSnapshotMode(conf)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParsePrefetchRowsConfig(t *testing.T) {
	const minimalOracleCDCYAML = `connection_string: oracle://user:pass@host:1521/svc
include:
  - SCHEMA.TABLE
logminer: {}
`
	t.Run("unset is a no-op", func(t *testing.T) {
		conf, err := oracleDBStreamConfigSpec.ParseYAML(minimalOracleCDCYAML, nil)
		require.NoError(t, err)

		overrides := map[string]string{}
		require.NoError(t, parsePrefetchRowsConfig(conf, overrides))
		assert.Empty(t, overrides)
	})

	t.Run("set value becomes a PREFETCH_ROWS override", func(t *testing.T) {
		conf, err := oracleDBStreamConfigSpec.ParseYAML(minimalOracleCDCYAML+"prefetch_rows: 5000\n", nil)
		require.NoError(t, err)

		overrides := map[string]string{}
		require.NoError(t, parsePrefetchRowsConfig(conf, overrides))
		assert.Equal(t, map[string]string{"PREFETCH_ROWS": "5000"}, overrides)
	})

	for _, tt := range []struct {
		name  string
		value int
	}{
		{"zero rejected", 0},
		{"negative rejected", -1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conf, err := oracleDBStreamConfigSpec.ParseYAML(
				fmt.Sprintf(minimalOracleCDCYAML+"prefetch_rows: %d\n", tt.value), nil)
			require.NoError(t, err)

			overrides := map[string]string{}
			err = parsePrefetchRowsConfig(conf, overrides)
			assert.Error(t, err)
			assert.Empty(t, overrides)
		})
	}

	t.Run("prefetch_rows field replaces an existing PREFETCH_ROWS query param in connection_string", func(t *testing.T) {
		conf, err := oracleDBStreamConfigSpec.ParseYAML(`connection_string: "oracle://user:pass@host:1521/svc?PREFETCH_ROWS=100"
include:
  - SCHEMA.TABLE
logminer: {}
prefetch_rows: 5000
`, nil)
		require.NoError(t, err)

		connStr, err := conf.FieldString(ociFieldConnectionString)
		require.NoError(t, err)

		overrides := map[string]string{}
		require.NoError(t, parsePrefetchRowsConfig(conf, overrides))

		built, err := buildConnectionString(connStr, overrides, service.MockResources().Logger())
		require.NoError(t, err)

		parsed, err := url.Parse(built)
		require.NoError(t, err)
		assert.Equal(t, "5000", parsed.Query().Get("PREFETCH_ROWS"),
			"prefetch_rows field must win over the PREFETCH_ROWS already present in connection_string")
	})
}
