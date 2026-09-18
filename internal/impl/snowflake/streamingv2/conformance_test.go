// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build !integration

package streamingv2

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureRequests stands up a server that records every request URL it
// receives and answers each SSv2 endpoint with the minimum valid body the
// client needs to proceed. Later conformance tests reuse it.
func captureRequests(t *testing.T) (*httptest.Server, *[]*url.URL) {
	t.Helper()
	return captureRequestsWith(t, func(*http.Request) {})
}

// captureRequestsWith is captureRequests with an additional observer called
// on every request before it is recorded or answered, so header-conformance
// tests can inspect requests without duplicating the response fixtures
// above. captureRequests is a thin wrapper passing a no-op observer.
func captureRequestsWith(t *testing.T, observe func(*http.Request)) (*httptest.Server, *[]*url.URL) {
	t.Helper()
	var mu sync.Mutex
	seen := []*url.URL{}
	var host string // set after the server exists, read inside the handler
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observe(r)
		mu.Lock()
		seen = append(seen, r.URL)
		mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/streaming/hostname"):
			// Discovery must return a host the client will then use for the
			// data plane. Point it back at this same server.
			_, _ = w.Write([]byte(host))
		case strings.HasSuffix(r.URL.Path, "/oauth/token"):
			_, _ = w.Write([]byte("scoped-token"))
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_, _ = w.Write([]byte(`{"channel_statuses":{}}`))
		case strings.HasSuffix(r.URL.Path, "/rows"):
			_, _ = w.Write([]byte(`{"status_code":0,"next_continuation_token":"ct-1"}`))
		default:
			// Channel open.
			_, _ = w.Write([]byte(`{"next_continuation_token":"ct-0","channel_status":{"channel_status_code":"SUCCESS"}}`))
		}
	}))
	host = strings.TrimPrefix(srv.URL, "http://")
	t.Cleanup(srv.Close)
	return srv, &seen
}

// conformanceClient builds a Client pointed entirely at srv. BaseURL is used
// deliberately: it overrides both planes, which keeps the test offline. The
// per-plane host split is asserted separately in Task 2, which must NOT use
// BaseURL for that reason.
func conformanceClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(Config{
		Account:    "ORG-ACCOUNT",
		User:       "USER",
		Role:       "ROLE",
		Database:   "DB",
		Schema:     "SC",
		Pipe:       "P",
		BaseURL:    srv.URL,
		PrivateKey: testKey(t),
		HTTPClient: srv.Client(),
		TokenTTL:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestInsertRowsURLMatchesSDKShape(t *testing.T) {
	srv, seen := captureRequests(t)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "10", "12"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	raw := rowsQuery(t, seen)
	// The SDK fixes this order; assert the raw string, not parsed values, so
	// a reordering is caught rather than normalised away.
	if want := "startOffsetToken=10&endOffsetToken=12&continuationToken=ct-0"; raw != want {
		t.Errorf("rows query = %q, want %q", raw, want)
	}
}

func TestInsertRowsURLRendersEmptyStartTokenAsPresentButEmpty(t *testing.T) {
	srv, seen := captureRequests(t)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "", "12"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	raw := rowsQuery(t, seen)
	// SDK: test_get_insert_rows_url_with_empty_start_offset_token asserts the
	// key is present with an empty value, never dropped.
	if !strings.Contains(raw, "startOffsetToken=&endOffsetToken=12") {
		t.Errorf("rows query = %q, want an empty-but-present startOffsetToken", raw)
	}
}

func TestInsertRowsURLSendsEqualTokensForSingleRow(t *testing.T) {
	srv, seen := captureRequests(t)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "7", "7"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	raw := rowsQuery(t, seen)
	// SDK: test_get_insert_rows_url_with_same_start_end_token — a single-row
	// append repeats the token rather than omitting one.
	if !strings.Contains(raw, "startOffsetToken=7&endOffsetToken=7") {
		t.Errorf("rows query = %q, want both tokens set to 7", raw)
	}
}

// TestOffsetTokensAreEscapedDeliberatelyUnlikeTheSDK pins a deliberate
// divergence from the reference SDK. The SDK does no URL encoding at all
// (rust/src/util/urls.rs:25 carries a standing TODO: "Construct URLs with
// safe url encoding / query string encoding. We are open to URL injection
// with pure string interpolations."). Our client percent-encodes tokens and
// the channel name.
//
// For today's inputs -- decimal offset tokens, DB.SC.PIPE-pN channel names --
// this is invisible: nothing in either alphabet needs escaping, so we and the
// SDK produce byte-identical requests. But offset_token is a Benthos
// interpolation template supplied by the operator, so it can contain
// anything. An unescaped & or = would split the query string and silently
// change which offset the server records for a given logical value -- a
// data-correctness bug, not a cosmetic one. This test exists to catch a
// regression toward matching the SDK's unescaped behaviour, not to suggest
// the SDK is right; its own comment admits the injection risk.
func TestOffsetTokensAreEscapedDeliberatelyUnlikeTheSDK(t *testing.T) {
	srv, seen := captureRequests(t)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	// A token an operator's interpolation could plausibly produce.
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "a b&c=d", "a b&c=d"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}
	raw := rowsQuery(t, seen)
	// We escape; the SDK does not. This is the deliberate divergence: an
	// unescaped & or = would split the query string and silently change
	// which token the server records.
	if !strings.Contains(raw, "startOffsetToken=a+b%26c%3Dd") {
		t.Errorf("rows query = %q, want the token percent-escaped", raw)
	}
	// The escaping must not corrupt the parsed value.
	q, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatalf("ParseQuery(%q): %v", raw, err)
	}
	if got := q.Get("startOffsetToken"); got != "a b&c=d" {
		t.Errorf("round-tripped token = %q, want %q", got, "a b&c=d")
	}
}

// TestAppendRowsWithAllEmptyBatchMakesNoRequestAndLeavesTokenUntouched pins
// the compounding case at the boundary of Area 1 (encoding) and Area 2
// (continuation-token threading): EncodeRows drops every row in an
// all-empty/whitespace batch, producing a zero-length body; channel.go's
// `if len(body) == 0 { return false, nil }` then reports success WITHOUT
// making any HTTP request and WITHOUT touching ch.continuation. sent=false
// is exactly what lets the caller (writeChannelGroup) distinguish "nothing
// to send" from "sent successfully" -- before that return value existed,
// the caller had no way to tell the two apart and would wait for a commit
// token that was never sent.
func TestAppendRowsWithAllEmptyBatchMakesNoRequestAndLeavesTokenUntouched(t *testing.T) {
	srv, seen := captureRequests(t)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	sent, err := ch.AppendRows(context.Background(), [][]byte{{}, []byte("   "), []byte("\n\t")}, "1", "1")
	if err != nil {
		t.Fatalf("all-empty append: %v", err)
	}
	if sent {
		t.Error("expected sent=false for an all-empty/whitespace batch, got true")
	}
	for _, u := range *seen {
		if strings.HasSuffix(u.Path, "/rows") {
			t.Fatalf("expected zero /rows requests for an all-empty batch, got one: %s", u.RawQuery)
		}
	}
	// A subsequent real append must present the token set at channel open
	// (ct-0), proving the all-empty append above never touched ch.continuation.
	sent, err = ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "2", "2")
	if err != nil {
		t.Fatalf("real append: %v", err)
	}
	if !sent {
		t.Error("expected sent=true for a non-empty batch, got false")
	}
	raw := rowsQuery(t, seen)
	if !strings.Contains(raw, "continuationToken=ct-0") {
		t.Errorf("rows query = %q, want continuationToken=ct-0 (unchanged by the all-empty append)", raw)
	}
}

// rowsQuery returns the RawQuery of the single /rows request captured, failing
// the test if there was not exactly one. Asserting on "exactly one" matters:
// the SDK's own channel tests are gated on `if let Some(req)` and pass with
// zero assertions when the request never fires. We do not copy that.
func rowsQuery(t *testing.T, seen *[]*url.URL) string {
	t.Helper()
	var found []string
	for _, u := range *seen {
		if strings.HasSuffix(u.Path, "/rows") {
			found = append(found, u.RawQuery)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly 1 /rows request, got %d (%v)", len(found), found)
	}
	return found[0]
}

// TestEndpointsUseTheCorrectPlane pins the SDK's account/ingest host split.
// Discovery (get_subdomain_name_url) and the oauth token exchange
// (get_scoped_token_refresh_url) use config.get_host(), the account host;
// every per-pipe/channel endpoint uses the discovered subdomain_hostname
// instead. Crossing the two yields opaque 401s. This deliberately does NOT
// use BaseURL, which overrides both planes and would make the split
// untestable; it stands up two httptest servers and wires AccountHost +
// Scheme so discovery genuinely returns the second server's host.
func TestEndpointsUseTheCorrectPlane(t *testing.T) {
	var mu sync.Mutex
	acct, ingest := []string{}, []string{}

	ingestSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		ingest = append(ingest, r.URL.Path)
		mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/rows"):
			_, _ = w.Write([]byte(`{"status_code":0,"next_continuation_token":"ct-1"}`))
		default:
			_, _ = w.Write([]byte(`{"next_continuation_token":"ct-0","channel_status":{"channel_status_code":"SUCCESS"}}`))
		}
	}))
	t.Cleanup(ingestSrv.Close)

	acctSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		acct = append(acct, r.URL.Path)
		mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/streaming/hostname"):
			_, _ = w.Write([]byte(strings.TrimPrefix(ingestSrv.URL, "http://")))
		case strings.HasSuffix(r.URL.Path, "/oauth/token"):
			_, _ = w.Write([]byte("scoped-token"))
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			_, _ = w.Write([]byte(`{"channel_statuses":{}}`))
		default:
			t.Errorf("unexpected account-plane request: %s", r.URL.Path)
		}
	}))
	t.Cleanup(acctSrv.Close)

	c, err := NewClient(Config{
		Account: "ORG-ACCOUNT", User: "USER", Role: "ROLE",
		Database: "DB", Schema: "SC", Pipe: "P",
		AccountHost: strings.TrimPrefix(acctSrv.URL, "http://"),
		Scheme:      "http",
		PrivateKey:  testKey(t), HTTPClient: acctSrv.Client(), TokenTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Discovery and token exchange belong to the account plane.
	assertContainsSuffix(t, "account", acct, "/v2/streaming/hostname")
	assertContainsSuffix(t, "account", acct, "/oauth/token")
	// bulk-channel-status is pipe-level and account-plane, and carries no
	// channel segment (SDK: get_channel_status_batch).
	assertContainsSuffix(t, "account", acct, ":bulk-channel-status")
	for _, p := range acct {
		if strings.Contains(p, "/channels/") && strings.HasSuffix(p, ":bulk-channel-status") {
			t.Errorf("bulk-channel-status must be pipe-level, got %q", p)
		}
	}
	// Rows and channel open belong to the ingest plane.
	assertContainsSuffix(t, "ingest", ingest, "/rows")
	if len(ingest) < 2 {
		t.Errorf("expected channel open and rows on the ingest plane, got %v", ingest)
	}
}

func assertContainsSuffix(t *testing.T, plane string, paths []string, suffix string) {
	t.Helper()
	for _, p := range paths {
		if strings.HasSuffix(p, suffix) {
			return
		}
	}
	t.Errorf("expected a %s-plane request ending %q, got %v", plane, suffix, paths)
}

// TestRequestHeadersMatchProtocol pins three header facts, each learned the
// hard way against a real deployment rather than read from a doc:
//
//   - Accept must be explicit. Go's net/http sends none by default, and the
//     API answers that with error 391902 "Unsupported Accept header null is
//     specified". curl masks this because it defaults to */*.
//   - The token-type header differs per plane: KEYPAIR_JWT on the account
//     host (discovery, bulk-channel-status), OAUTH on the ingest host
//     (channel open, rows). Crossing them yields opaque 401s.
//   - The /oauth/token exchange sends neither -- it authenticates via the
//     assertion form field alone. This is deliberately the absence of a
//     header, not a missing KEYPAIR_JWT: see token.go.
//
// It also pins the rows endpoint's Content-Type: application/x-ndjson.
func TestRequestHeadersMatchProtocol(t *testing.T) {
	type capture struct {
		path   string
		header http.Header
	}
	var mu sync.Mutex
	var seen []capture
	record := func(r *http.Request) {
		mu.Lock()
		seen = append(seen, capture{path: r.URL.Path, header: r.Header.Clone()})
		mu.Unlock()
	}

	srv, _ := captureRequestsWith(t, record)
	c := conformanceClient(t, srv)
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ch, err := c.OpenChannel(context.Background(), "DB.SC.P-p0")
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	if _, err := ch.AppendRows(context.Background(), [][]byte{[]byte(`{"a":1}`)}, "1", "1"); err != nil {
		t.Fatalf("AppendRows: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, got := range seen {
		// Every request needs an explicit Accept: Go sends none and the API
		// answers 391902.
		if got.header.Get("Accept") != "application/json" {
			t.Errorf("%s: Accept = %q, want application/json", got.path, got.header.Get("Accept"))
		}
		switch {
		case strings.HasSuffix(got.path, "/oauth/token"):
			// The exchange authenticates via the assertion form field only.
			if v := got.header.Get("X-Snowflake-Authorization-Token-Type"); v != "" {
				t.Errorf("oauth/token must send no token-type header, got %q", v)
			}
		case strings.HasSuffix(got.path, "/v2/streaming/hostname"),
			strings.HasSuffix(got.path, ":bulk-channel-status"):
			if v := got.header.Get("X-Snowflake-Authorization-Token-Type"); v != "KEYPAIR_JWT" {
				t.Errorf("%s: token type = %q, want KEYPAIR_JWT", got.path, v)
			}
		case strings.HasSuffix(got.path, "/rows"):
			if v := got.header.Get("X-Snowflake-Authorization-Token-Type"); v != "OAUTH" {
				t.Errorf("rows: token type = %q, want OAUTH", v)
			}
			if v := got.header.Get("Content-Type"); v != "application/x-ndjson" {
				t.Errorf("rows: Content-Type = %q, want application/x-ndjson", v)
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no requests captured")
	}
}
