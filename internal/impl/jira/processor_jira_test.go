// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package jira

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestJiraProcessorConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configYAML string
		wantErrSub string
	}{
		{
			name: "missing base_url",
			configYAML: `
username: "user"
api_token: "token"
max_results_per_page: 50
`,
			wantErrSub: "base_url",
		},
		{
			name: "invalid base_url",
			configYAML: `
base_url: "not a url"
username: "user"
api_token: "token"
max_results_per_page: 50
`,
			wantErrSub: "base_url",
		},
		{
			name: "missing username",
			configYAML: `
username: ""
base_url: "https://example.com"
api_token: "token"
`,
			wantErrSub: "username",
		},
		{
			name: "missing api_token",
			configYAML: `
base_url: "https://example.com"
username: "user"
api_token: ""
`,
			wantErrSub: "api_token",
		},
		{
			name: "max_results_per_page too small",
			configYAML: `
base_url: "http://example.invalid"
username: "user"
api_token: "token"
max_results_per_page: 0
`,
			wantErrSub: "max_results_per_page",
		},
		{
			name: "max_results_per_page too large",
			configYAML: `
base_url: "http://example.invalid"
username: "user"
api_token: "token"
max_results_per_page: 100000
`,
			wantErrSub: "max_results_per_page",
		},
		{
			name: "valid minimal (defaults kick in)",
			configYAML: `
base_url: "http://example.invalid"
username: "user"
api_token: "token"
`,
			wantErrSub: "",
		},
		{
			name: "valid explicit",
			configYAML: `
base_url: "http://example.invalid"
username: "user"
api_token: "token"
max_results_per_page: 200
`,
			wantErrSub: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conf, err := newJiraProcessorConfigSpec().ParseYAML(tc.configYAML, nil)
			resources := conf.Resources()
			license.InjectTestService(resources)
			proc, procErr := newJiraProcessor(conf, conf.Resources())

			if tc.wantErrSub == "" {
				require.NoError(t, err, "expected config to be valid")
				assert.NotNil(t, proc)
			} else {
				if err != nil {
					require.Error(t, err, "expected config validation error")
					require.Contains(t, err.Error(), tc.wantErrSub)
				}
				if procErr != nil {
					require.Error(t, procErr, "expected config validation error")
					require.Contains(t, procErr.Error(), tc.wantErrSub)
				}
			}
		})
	}
}
