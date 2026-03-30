// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildConnectionString(tt.input, tt.overrides)
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
