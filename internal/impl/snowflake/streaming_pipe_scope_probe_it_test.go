// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	v2 "github.com/redpanda-data/connect/v4/internal/impl/snowflake/streamingv2"
)

// TestIntegrationSnowflakeStreamingPipeScopeCanCarryHostAndRole: the OAuth
// scope can carry the ingest host and a session role together, and the role
// is enforced, not just accepted. That is proved by asking for PUBLIC, which
// cannot read the test pipe: if the pipe call succeeds the role was ignored.
func TestIntegrationSnowflakeStreamingPipeScopeCanCarryHostAndRole(t *testing.T) {
	integration.CheckSkip(t)
	env := loadSnowflakeITEnv(t)
	pk := loadSnowflakeITPrivateKey(t, env)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	accountHost := env.AccountHost
	if accountHost == "" {
		accountHost = strings.ToLower(env.Account) + ".snowflakecomputing.com"
	}
	accountBase := "https://" + accountHost
	jwt, err := v2.MintJWT(env.Account, env.User, pk, time.Now())
	require.NoError(t, err)

	// Ingest host, the value this client puts in scope today.
	hostReq, err := http.NewRequestWithContext(ctx, http.MethodGet, accountBase+"/v2/streaming/hostname", nil)
	require.NoError(t, err)
	hostReq.Header.Set("Authorization", "Bearer "+jwt)
	hostReq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	hostReq.Header.Set("Accept", "application/json")
	hostResp, err := http.DefaultClient.Do(hostReq)
	require.NoError(t, err)
	hostBody, err := io.ReadAll(hostResp.Body)
	hostResp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 2, hostResp.StatusCode/100, "hostname: HTTP %d: %s", hostResp.StatusCode, hostBody)

	ingestHost := strings.TrimSpace(string(hostBody))
	ingestHost = strings.Trim(ingestHost, `"`)
	if i := strings.Index(ingestHost, `"hostname"`); i >= 0 {
		// Tolerate a JSON object form as well as a bare string.
		rest := ingestHost[i+len(`"hostname"`):]
		rest = strings.TrimLeft(rest, `: "`)
		if j := strings.IndexAny(rest, `"}`); j >= 0 {
			ingestHost = rest[:j]
		}
	}
	t.Logf("ingest host = %q", ingestHost)

	mint := func(scope string) (token string, status int) {
		form := url.Values{}
		form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
		if scope != "" {
			form.Set("scope", scope)
		}
		form.Set("assertion", jwt)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, accountBase+"/oauth/token", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return strings.TrimSpace(string(b)), resp.StatusCode
	}

	// Exercises the token against the real pipe, returning status and body.
	probePipe := func(token string) (status int, body string) {
		u := "https://" + ingestHost + "/v2/streaming/databases/" + url.PathEscape(env.Database) +
			"/schemas/" + url.PathEscape(env.Schema) + "/pipes/" + url.PathEscape(env.Pipe) + ":bulk-channel-status"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(`{"channel_names":["scope-probe"]}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Snowflake-Authorization-Token-Type", "OAUTH")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, strings.TrimSpace(string(b))
	}

	for _, tc := range []struct {
		name string
		// wantPipeOK false means the pipe call must be refused (PUBLIC
		// cannot read the pipe); success would mean the role was ignored.
		scope      string
		wantPipeOK bool
	}{
		{"host only (what this client sends today)", ingestHost, true},
		{"session role only (what the SDK sends)", "session:role:" + env.Role, true},
		{"host and session role, space separated", ingestHost + " session:role:" + env.Role, true},
		{"host and a role that must be refused if honoured", ingestHost + " session:role:PUBLIC", false},
		{"session role PUBLIC alone", "session:role:PUBLIC", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tok, code := mint(tc.scope)
			require.Equal(t, 2, code/100, "minting a token for scope %q should succeed, got HTTP %d: %s", tc.scope, code, ssv2Truncate(tok, 300))
			t.Logf("MINT OK scope=%q (token %d chars)", tc.scope, len(tok))

			pCode, pBody := probePipe(tok)
			t.Logf("  pipe call -> HTTP %d: %s", pCode, ssv2Truncate(pBody, 300))
			if tc.wantPipeOK {
				require.Equal(t, 2, pCode/100, "scope %q should authorize the pipe call, got HTTP %d: %s", tc.scope, pCode, ssv2Truncate(pBody, 300))
			} else {
				require.NotEqual(t, 2, pCode/100, "scope %q must be REFUSED (PUBLIC cannot read the test pipe) -- a 2xx here means the session role was silently ignored instead of enforced, got HTTP %d: %s", tc.scope, pCode, ssv2Truncate(pBody, 300))
			}
		})
	}
}

func ssv2Truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
