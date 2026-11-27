// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testClientID     = "client-abc"
	testClientSecret = "s3cr3t-value-never-printed"
	testToken        = "tok-xyz-never-printed"
)

type a2aFake struct {
	mu        sync.Mutex
	tokenForm url.Values
	tokenAuth string
	auth      string
	body      map[string]any
	a2aStatus int
	a2aReply  string
}

// startNotifyFakes starts a token endpoint and an A2A endpoint on TLS
// servers, points notify at them and sets the env the workflow passes.
func startNotifyFakes(t *testing.T) *a2aFake {
	t.Helper()
	f := &a2aFake{a2aStatus: http.StatusOK, a2aReply: `{"jsonrpc":"2.0","id":"1","result":{"kind":"task","id":"t1"}}`}
	tok := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		f.mu.Lock()
		f.tokenForm, f.tokenAuth = r.PostForm, r.Header.Get("Authorization")
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"`+testToken+`","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(tok.Close)
	a2a := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.auth = r.Header.Get("Authorization")
		_ = json.Unmarshal(raw, &f.body)
		status, reply := f.a2aStatus, f.a2aReply
		f.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(a2a.Close)

	oldURL, oldClient := notifyTokenURL, notifyHTTPClient
	notifyTokenURL, notifyHTTPClient = tok.URL+"/oauth/token", tok.Client()
	// Both servers share the httptest CA, so one client trusts both.
	t.Cleanup(func() { notifyTokenURL, notifyHTTPClient = oldURL, oldClient })
	t.Setenv("BENCH_BOT_A2A_URL", a2a.URL+"/a2a/v1/bench-bot")
	t.Setenv("BENCH_BOT_ADP_CLIENT_ID", testClientID)
	t.Setenv("BENCH_BOT_ADP_CLIENT_SECRET", testClientSecret)
	return f
}

var notifyArgs = []string{
	"notify", "--plan", "0123456789ab",
	"--run-url", "https://github.com/redpanda-data/connect/actions/runs/123",
	"--outcome", "success", "--entry", "bench-20261120-120000",
}

func TestNotifySendsNonBlockingMessage(t *testing.T) {
	f := startNotifyFakes(t)
	var so, se bytes.Buffer
	require.Equal(t, 0, run(notifyArgs, &so, &se), se.String())

	// Client credentials with the Redpanda Cloud audience.
	require.Equal(t, "client_credentials", f.tokenForm.Get("grant_type"))
	require.Equal(t, "cloudv2-production.redpanda.cloud", f.tokenForm.Get("audience"))
	id, secret := f.tokenForm.Get("client_id"), f.tokenForm.Get("client_secret")
	if id == "" { // credentials sent as HTTP basic auth instead of in the form
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(f.tokenAuth, "Basic "))
		require.NoError(t, err)
		id, secret, _ = strings.Cut(string(raw), ":")
	}
	require.Equal(t, testClientID, id)
	require.Equal(t, testClientSecret, secret)

	require.Equal(t, "Bearer "+testToken, f.auth)
	require.Equal(t, "2.0", f.body["jsonrpc"])
	require.Equal(t, "message/send", f.body["method"])
	require.NotEmpty(t, f.body["id"])
	params := f.body["params"].(map[string]any)
	require.Equal(t, map[string]any{"blocking": false}, params["configuration"])
	msg := params["message"].(map[string]any)
	require.Equal(t, "user", msg["role"])
	require.NotEmpty(t, msg["messageId"])
	require.Equal(t, []any{map[string]any{
		"kind": "text",
		"text": "BENCH RESULT plan=0123456789ab run=https://github.com/redpanda-data/connect/actions/runs/123 outcome=success entry=bench-20261120-120000",
	}}, msg["parts"])

	out := so.String() + se.String()
	require.NotContains(t, out, testToken)
	require.NotContains(t, out, testClientSecret)
}

func TestNotifySkipsWithWarningWhenUnconfigured(t *testing.T) {
	for _, unset := range []string{"BENCH_BOT_A2A_URL", "BENCH_BOT_ADP_CLIENT_ID", "BENCH_BOT_ADP_CLIENT_SECRET"} {
		t.Run(unset, func(t *testing.T) {
			f := startNotifyFakes(t)
			t.Setenv(unset, "")
			var so, se bytes.Buffer
			require.Equal(t, 5, run(notifyArgs, &so, &se), "skipped is exit 5, so the workflow can tell it from sent (0) and failed (1)")
			require.Contains(t, so.String(), "::warning::")
			require.Contains(t, so.String(), unset)
			require.Nil(t, f.body, "nothing is sent")
			require.Nil(t, f.tokenForm, "no token is requested")
		})
	}
}

func TestNotifyFailsOnA2AError(t *testing.T) {
	for name, set := range map[string]func(*a2aFake){
		"http 500":       func(f *a2aFake) { f.a2aStatus = http.StatusInternalServerError },
		"json-rpc error": func(f *a2aFake) { f.a2aReply = `{"jsonrpc":"2.0","id":"1","error":{"code":-32600,"message":"bad"}}` },
		"not json":       func(f *a2aFake) { f.a2aReply = `<html>` },
	} {
		t.Run(name, func(t *testing.T) {
			f := startNotifyFakes(t)
			set(f)
			var so, se bytes.Buffer
			require.Equal(t, 1, run(notifyArgs, &so, &se))
			require.NotContains(t, so.String()+se.String(), testToken)
		})
	}
}

func TestNotifyRejectsBadArgsBeforeAnyCall(t *testing.T) {
	f := startNotifyFakes(t)
	with := func(flag, val string) []string {
		out := append([]string(nil), notifyArgs...)
		for i := range out {
			if out[i] == flag {
				out[i+1] = val
			}
		}
		return out
	}
	for name, args := range map[string][]string{
		"bad plan":         with("--plan", "../x"),
		"bad outcome":      with("--outcome", "great"),
		"bad entry":        with("--entry", "x y"),
		"bad run url":      with("--run-url", "http://evil/x"),
		"run url w/ space": with("--run-url", "https://github.com/a/b/actions/runs/1 outcome=success"),
		"newline in entry": with("--entry", "bench-20261120-120000\nmore"),
	} {
		var so, se bytes.Buffer
		require.Equal(t, 2, run(args, &so, &se), name)
	}
	var so, se bytes.Buffer
	require.Equal(t, 0, run(with("--entry", "none"), &so, &se), "entry none is allowed")
	require.Equal(t, 0, run(with("--run-url", "none"), &so, &se), "run none is allowed (never started)")
	require.NotNil(t, f.body)
}

func TestNotifyRequiresHTTPSEndpoint(t *testing.T) {
	startNotifyFakes(t)
	t.Setenv("BENCH_BOT_A2A_URL", "http://example.com/a2a")
	var so, se bytes.Buffer
	require.Equal(t, 1, run(notifyArgs, &so, &se), "the bearer token never goes over plain http")
}
