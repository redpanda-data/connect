// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/license"
)

func TestSalesforceProcessorConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configYAML string
		wantErrSub string
	}{
		{
			name: "missing org_url",
			configYAML: `
client_id: "abc"
client_secret: "xyz"
`,
			wantErrSub: "org_url",
		},
		{
			name: "invalid org_url",
			configYAML: `
org_url: "not a url"
client_id: "abc"
client_secret: "xyz"
`,
			wantErrSub: "org_url",
		},
		{
			name: "missing client_id",
			configYAML: `
org_url: "https://example.com"
client_secret: "xyz"
`,
			wantErrSub: "client_id",
		},
		{
			name: "missing client_secret",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
`,
			wantErrSub: "client_secret",
		},
		{
			name: "invalid restapi_version type",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
client_secret: "xyz"
restapi_version: 123
`,
			wantErrSub: "restapi_version",
		},
		{
			name: "invalid request_timeout",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
client_secret: "xyz"
request_timeout: "not-a-duration"
`,
			wantErrSub: "request_timeout",
		},
		{
			name: "invalid max_retries",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
client_secret: "xyz"
max_retries: "not-an-int"
`,
			wantErrSub: "max_retries",
		},
		{
			name: "valid minimal config",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
client_secret: "xyz"
`,
			wantErrSub: "",
		},
		{
			name: "valid full config",
			configYAML: `
org_url: "https://example.com"
client_id: "abc"
client_secret: "xyz"
restapi_version: "v64.0"
request_timeout: "10s"
max_retries: 5
`,
			wantErrSub: "",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			env := service.NewEnvironment()
			spec := newSalesforceProcessorConfigSpec()

			conf, err := spec.ParseYAML(tc.configYAML, env)

			var proc service.Processor
			var procErr error
			if err == nil {
				mgr := conf.Resources()
				license.InjectTestService(mgr)
				proc, procErr = newSalesforceProcessor(conf, mgr)
			}

			if tc.wantErrSub == "" {
				require.NoError(t, err, "expected config to be valid")
				require.NoError(t, procErr, "expected processor to initialize")
				assert.NotNil(t, proc)
			} else {
				// Either config parsing OR processor creation must fail
				if err != nil {
					require.Contains(t, err.Error(), tc.wantErrSub)
				}
				if procErr != nil {
					require.Contains(t, procErr.Error(), tc.wantErrSub)
				}
			}
		})
	}
}
