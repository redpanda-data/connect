// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package salesforce

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/salesforce/salesforcehttp"
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

// newTestProcessor builds a salesforceProcessor wired up to a mock in-memory cache.
// The returned resources object can be used to pre-seed cache keys.
func newTestProcessor(t *testing.T) (*salesforceProcessor, *service.Resources) {
	t.Helper()
	mgr := service.MockResources(service.MockResourcesOptAddCache("salesforce_checkpoint"))
	license.InjectTestService(mgr)
	p := &salesforceProcessor{
		log:               mgr.Logger(),
		res:               mgr,
		cacheResourceName: "salesforce_checkpoint",
	}
	return p, mgr
}

func TestLoadState_NewFormat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p, mgr := newTestProcessor(t)

	want := ProcessorState{
		SnapshotComplete: true,
		FilteredCursor:   "cursor123",
	}
	b, err := json.Marshal(want)
	require.NoError(t, err)

	require.NoError(t, mgr.AccessCache(ctx, "salesforce_checkpoint", func(c service.Cache) {
		require.NoError(t, c.Set(ctx, "sf_state", b, nil))
	}))

	got, err := p.loadState(ctx)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestLoadState_LegacyMigration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p, mgr := newTestProcessor(t)

	legacyCursor := salesforcehttp.Cursor{NextAssign: 3}
	b, err := json.Marshal(legacyCursor)
	require.NoError(t, err)

	require.NoError(t, mgr.AccessCache(ctx, "salesforce_checkpoint", func(c service.Cache) {
		require.NoError(t, c.Set(ctx, "sf_cursor", b, nil))
	}))

	got, err := p.loadState(ctx)
	require.NoError(t, err)
	assert.Equal(t, legacyCursor, got.RestCursor)
	assert.False(t, got.SnapshotComplete)

	// Verify the state was migrated to the new key
	var migrated ProcessorState
	require.NoError(t, mgr.AccessCache(ctx, "salesforce_checkpoint", func(c service.Cache) {
		raw, err := c.Get(ctx, "sf_state")
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &migrated))
	}))
	assert.Equal(t, legacyCursor, migrated.RestCursor)
}

func TestLoadState_Empty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p, _ := newTestProcessor(t)

	got, err := p.loadState(ctx)
	require.NoError(t, err)
	assert.Equal(t, ProcessorState{}, got)
}
