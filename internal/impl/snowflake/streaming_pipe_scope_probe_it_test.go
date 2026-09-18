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

// TestIntegrationSnowflakeStreamingPipeScopeCanCarryHostAndRole answers whether
// the OAuth `scope` field can carry both values this connector and the
// official Snowflake Kafka Connector SDK each put there alone.
//
// This client sends scope=<ingest host> (streamingv2/token.go). The SDK
// sends scope=session:role:<ROLE> (per the SDK's own oauth handling). Both
// are single values in the same field, so the output's `role` config field
// is otherwise inert for it: the server authorizes against the user's
// DEFAULT_ROLE unless the token itself carries a session role.
//
// Two questions, and the second is the one that matters:
//  1. Does the token endpoint ACCEPT a space-separated host + session role?
//     OAuth scopes are conventionally space-separated, but conventions are
//     not contracts.
//  2. If accepted, does the session role actually take EFFECT? A token that
//     mints happily and then authorizes exactly as before would be worse
//     than an outright rejection, because it would look like the role was
//     applied.
//
// Question 2 is answered by asking for a role that must fail: PUBLIC cannot
// read the test pipe, so if the role is honoured the pipe call is refused;
// if the call succeeds under session:role:PUBLIC, the role was ignored.
//
// Every subtest below asserts on both mint and pipe-call outcomes -- an
// earlier version of this test only logged them, which meant it reported
// PASS regardless of what the API actually did, including a silent
// regression to Question 2's failure case (role accepted but not
// enforced). wantPipeOK per case is exactly the answer this test exists to
// give: true for the two shapes this client and the SDK actually send in
// production, false for the two constructed specifically to prove PUBLIC
// gets refused rather than silently ignored.
//
// This is a port of the SSv2 demo repo's scope_role_probe_it_test.go: it
// needs only account credentials and a real pipe to probe, no fixture SQL,
// no Kafka/Redpanda broker, and no connector process, so none of the
// harness simplifications made elsewhere in this migration apply to it.
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
		// wantPipeOK: true asserts the pipe call must succeed (env.Role is
		// the account's own configured ingest role, expected to have
		// access to it per the README's setup guide -- if this stops being
		// true, something about the ingest role itself is broken, not just
		// this probe). false asserts the pipe call must be REFUSED: PUBLIC
		// cannot read the test pipe, so success here would mean the
		// session role was silently ignored rather than enforced, which is
		// worse than an outright rejection (see the package comment above).
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
