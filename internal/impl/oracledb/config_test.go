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

	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer"
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

func TestParseLogMinerConfigLogCountValidation(t *testing.T) {
	const minimalOracleCDCYAML = `connection_string: oracle://user:pass@host:1521/svc
include:
  - SCHEMA.TABLE
`

	tests := []struct {
		name         string
		logminerYAML string
		errContains  string
	}{
		{
			name:         "defaults are valid",
			logminerYAML: "logminer: {}\n",
		},
		{
			name: "log_count_min positive and log_count_growth_max above it is valid",
			logminerYAML: fmt.Sprintf(`logminer:
  %s: 3
  %s: 5
`, ociFieldLogCountMin, ociFieldLogCountGrowthMax),
		},
		{
			name: "log_count_growth_max equal to log_count_min is valid",
			logminerYAML: fmt.Sprintf(`logminer:
  %s: 3
  %s: 3
`, ociFieldLogCountMin, ociFieldLogCountGrowthMax),
		},
		{
			name: "log_count_min zero is invalid",
			logminerYAML: fmt.Sprintf(`logminer:
  %s: 0
`, ociFieldLogCountMin),
			errContains: fmt.Sprintf("logminer.%s must be greater than 0, got 0", ociFieldLogCountMin),
		},
		{
			name: "log_count_min negative is invalid",
			logminerYAML: fmt.Sprintf(`logminer:
  %s: -1
`, ociFieldLogCountMin),
			errContains: fmt.Sprintf("logminer.%s must be greater than 0, got -1", ociFieldLogCountMin),
		},
		{
			name: "log_count_growth_max below log_count_min is invalid",
			logminerYAML: fmt.Sprintf(`logminer:
  %s: 5
  %s: 2
`, ociFieldLogCountMin, ociFieldLogCountGrowthMax),
			errContains: fmt.Sprintf("logminer.%s (2) must be greater than or equal to logminer.%s (5)", ociFieldLogCountGrowthMax, ociFieldLogCountMin),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conf, err := oracleDBStreamConfigSpec.ParseYAML(minimalOracleCDCYAML+test.logminerYAML, nil)
			require.NoError(t, err)

			_, err = parseLogMinerConfig(conf)
			if test.errContains != "" {
				require.ErrorContains(t, err, test.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestParseLogMinerConfigDefaultsMatchLintConstants sanity-checks that the defaults
// baked into logminer.NewDefaultConfig() line up with the constants the lint rule in
// input_oracledb_cdc.go compares against, since that rule hardcodes the default
// values rather than deriving them from logminer.NewDefaultConfig().
func TestParseLogMinerConfigDefaultsMatchLintConstants(t *testing.T) {
	cfg := logminer.NewDefaultConfig()
	assert.Equal(t, logminer.DefaultSCNWindowSize, cfg.SCNWindowSize)
	assert.Equal(t, logminer.DefaultMaxSCNWindowSize, cfg.MaxSCNWindowSize)
	assert.Equal(t, logminer.DefaultLogCountMin, cfg.LogCountMin)
	assert.Equal(t, logminer.DefaultLogCountGrowthMax, cfg.LogCountGrowthMax)
	assert.Equal(t, logminer.WindowStrategySCNWindow, cfg.WindowStrategy)
}
