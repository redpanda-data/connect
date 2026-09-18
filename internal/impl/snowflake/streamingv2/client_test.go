// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package streamingv2

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConnectDiscoversIngestHostWithKeypairJWT(t *testing.T) {
	var auth, tokenType, accept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			auth = r.Header.Get("Authorization")
			tokenType = r.Header.Get("X-Snowflake-Authorization-Token-Type")
			accept = r.Header.Get("Accept")
			_, _ = io.WriteString(w, "MYACCOUNT.ingest.myregion.snowflakecomputing.com\n")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			// Connect's pipe pre-flight lands here; the probe channel is not
			// expected to exist, so an empty statuses map is a normal 2xx.
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "myaccount", AccountHost: "unused", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !strings.HasPrefix(auth, "Bearer ") || strings.Count(auth, ".") != 2 {
		t.Errorf("hostname call needs a 3-part JWT bearer, got %q", auth)
	}
	if tokenType != "KEYPAIR_JWT" {
		t.Errorf("token type = %q, want KEYPAIR_JWT", tokenType)
	}
	// Go's net/http sends no Accept header by default and the API rejects that
	// with 391902 "Unsupported Accept header null".
	if accept != "application/json" {
		t.Errorf("Accept = %q, want application/json", accept)
	}
	// BaseURL overrides the discovered host, which is unresolvable on a local
	// deployment ("SNOWFLAKE.ingest..snowflakecomputing.com").
	if c.IngestBase() != srv.URL {
		t.Errorf("IngestBase = %q, want the BaseURL override %q", c.IngestBase(), srv.URL)
	}
}

func TestConnectUsesDiscoveredHostWithoutOverride(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			// Connect's pipe pre-flight lands here; see the identical case in
			// TestConnectDiscoversIngestHostWithKeypairJWT above.
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	}))
	defer srv.Close()
	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		AccountHost: strings.TrimPrefix(srv.URL, "http://"),
		HTTPClient:  srv.Client(),
		Scheme:      "http",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if c.IngestBase() != "http://ingest.example.com" {
		t.Errorf("IngestBase = %q", c.IngestBase())
	}
}

// TestControlPlaneJWTIsCachedAcrossCalls proves controlPlaneJWT's whole
// reason for existing: Connect's own two control-plane calls (hostname
// discovery, then the pipe pre-flight's ChannelStatuses) share one JWT
// instead of each minting their own, and a later, independent
// ChannelStatuses call still reuses it rather than minting again --
// verified by asserting the Authorization header is byte-for-byte
// identical across every control-plane request, not just structurally
// valid. Before controlPlaneJWT existed, each of these calls minted its own
// JWT unconditionally, doing a full RSA-2048 sign every time even though
// the previous one was still an hour from expiry.
func TestControlPlaneJWTIsCachedAcrossCalls(t *testing.T) {
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			mu.Lock()
			auths = append(auths, r.Header.Get("Authorization"))
			mu.Unlock()
			_, _ = io.WriteString(w, "ingest.example.com")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			mu.Lock()
			auths = append(auths, r.Header.Get("Authorization"))
			mu.Unlock()
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		case r.URL.Path == "/oauth/token":
			// The data-plane token exchange: authenticated via the JWT as a
			// form-encoded "assertion" value, not an Authorization header --
			// deliberately not asserted on here, this test is about the
			// control-plane JWT specifically.
			_, _ = io.WriteString(w, "scoped")
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		AccountHost: strings.TrimPrefix(srv.URL, "http://"),
		HTTPClient:  srv.Client(),
		Scheme:      "http",
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := c.ChannelStatuses(context.Background(), []string{"some-channel"}); err != nil {
		t.Fatalf("ChannelStatuses: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(auths) != 3 {
		t.Fatalf("expected 3 control-plane requests (hostname, pre-flight, explicit ChannelStatuses), got %d: %v", len(auths), auths)
	}
	for _, a := range auths {
		if !strings.HasPrefix(a, "Bearer ") {
			t.Fatalf("expected every control-plane call to carry a JWT bearer, got %q in %v", a, auths)
		}
	}
	for _, a := range auths[1:] {
		if a != auths[0] {
			t.Errorf("control-plane JWT changed across calls (%q vs %q) -- every call should reuse the cached one instead of minting its own", auths[0], a)
		}
	}
}

func TestNewClientRejectsMissingRequiredConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no account":               {User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p", AccountHost: "h"},
		"no user":                  {Account: "a", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p", AccountHost: "h"},
		"no key":                   {Account: "a", User: "u", Database: "d", Schema: "s", Pipe: "p", AccountHost: "h"},
		"no pipe":                  {Account: "a", User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", AccountHost: "h"},
		"no database":              {Account: "a", User: "u", PrivateKey: testKey(t), Schema: "s", Pipe: "p", AccountHost: "h"},
		"no account_host/base_url": {Account: "a", User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p"},
	} {
		if _, err := NewClient(cfg); err == nil {
			t.Errorf("%s: expected NewClient to fail", name)
		}
	}
}

func TestNewClientRejectsMissingAccountHostAndBaseURL(t *testing.T) {
	_, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p",
	})
	if err == nil {
		t.Fatal("expected NewClient to fail")
	}
	if !strings.Contains(err.Error(), "account_host") || !strings.Contains(err.Error(), "base_url") {
		t.Errorf("error must name the missing configuration, got %v", err)
	}
}

// TestConnectRejectsTokenTTLAtOrBelowRefreshMargin pins the failure to
// newTokenSource's construction-time guard (token.go), not to any error from
// the Connect path: a bare tokenSource{} literal would bypass the guard and
// this test would fail to distinguish that regression from an unrelated
// error.
func TestConnectRejectsTokenTTLAtOrBelowRefreshMargin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/streaming/hostname" {
			_, _ = io.WriteString(w, "ingest.example.com")
			return
		}
		_, _ = io.WriteString(w, "scoped")
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
		TokenTTL: 30 * time.Second, // <= refreshMargin (1 minute)
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail")
	}
	if !strings.Contains(err.Error(), "refresh margin") {
		t.Errorf("error must come from newTokenSource's refresh-margin guard, got %v", err)
	}
}

// TestConnectPreflightSucceedsOnNormalStatus pins the happy path of Connect's
// pipe pre-flight: a normal 2xx :bulk-channel-status response -- even one that
// names channels other than the probe, as a real pipe with open channels
// would -- must not fail Connect. The pre-flight only cares that the
// pipe-scoped call itself succeeded, never about what statuses it returns.
func TestConnectPreflightSucceedsOnNormalStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			// A real pipe can already have channels open when Connect runs
			// again (e.g. process restart); the probe channel is absent but
			// the payload is otherwise a normal, non-empty status response.
			_, _ = io.WriteString(w, `{"channel_statuses":{"some-other-channel":{"channel_status_code":"SUCCESS"}}}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect must succeed when the pre-flight status call returns 2xx, got: %v", err)
	}
}

// TestConnectFailsLoudlyOnInvalidPipePreflight pins the core contract of the
// pre-flight: a pipe that answers :bulk-channel-status with a non-2xx (the
// documented behavior of a pipe created invalid, verified against a live
// account) must fail Connect outright, with an error naming both the HTTP status and
// the fully-qualified database.schema.pipe that was being checked -- not a
// bare "something went wrong".
func TestConnectFailsLoudlyOnInvalidPipePreflight(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, "ingest.example.com")
		case r.URL.Path == "/oauth/token":
			_, _ = io.WriteString(w, "scoped")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"error_code":"ERR_PIPE_IN_INVALID_STATE"}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail on an invalid pipe")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("error must carry the HTTP status code, got %v", err)
	}
	if !strings.Contains(err.Error(), "ERR_PIPE_IN_INVALID_STATE") {
		t.Errorf("error must carry the response body, got %v", err)
	}
	if !strings.Contains(err.Error(), "DB.SC.P") {
		t.Errorf("error must name the fully-qualified pipe being checked, got %v", err)
	}
}

// TestConnectPreflightNotReachedOnEarlierFailure pins the ordering half of
// the pre-flight contract: it must run strictly after hostname discovery and
// token priming, so a failure in either of those earlier steps is reported
// as that failure -- not masked or relabeled as a pipe pre-flight error. The
// server below fails only hostname discovery; its :bulk-channel-status
// handler would otherwise succeed, and a counter proves it is never called.
func TestConnectPreflightNotReachedOnEarlierFailure(t *testing.T) {
	var mu sync.Mutex
	preflightCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "boom")
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			mu.Lock()
			preflightCalls++
			mu.Unlock()
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	err = c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect to fail at hostname discovery")
	}
	if !strings.Contains(err.Error(), "discover ingest hostname") {
		t.Errorf("error must report the earlier discovery failure, not a pipe pre-flight error, got %v", err)
	}
	if strings.Contains(err.Error(), "pipe pre-flight") {
		t.Errorf("error must not be shaped like a pipe pre-flight failure when discovery failed first, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if preflightCalls != 0 {
		t.Errorf("pre-flight endpoint was called %d times, want 0 -- Connect must not reach it after an earlier failure", preflightCalls)
	}
}

// A rate-limited pre-flight must not look like a bad pipe. Measured live, a
// busy account answered this probe with
// HTTP 429 {"code":"390701","message":"Request rate exceeds max. Try later."},
// and the unretried error read "pipe pre-flight for DB.SC.P" -- which sends a
// user to re-check a pipe name and grants that were both correct.
func TestConnectPreflightRetriesRateLimitThenSucceeds(t *testing.T) {
	var mu sync.Mutex
	preflightCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, `{"hostname":"ingest.example.com"}`)
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			mu.Lock()
			preflightCalls++
			n := preflightCalls
			mu.Unlock()
			if n < 3 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"code":"390701","message":"Request rate exceeds max. Try later."}`)
				return
			}
			_, _ = io.WriteString(w, `{"channel_statuses":{}}`)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	}))
	defer srv.Close()

	c, err := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.openRetryDelay = func(int) time.Duration { return time.Millisecond }
	c.openRetrySleep = func(context.Context, time.Duration) error { return nil }

	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect must retry a rate-limited pre-flight rather than failing: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if preflightCalls != 3 {
		t.Errorf("pre-flight called %d times, want 3 (429, 429, 200)", preflightCalls)
	}
}

// The converse, so the retry cannot bury a genuine misconfiguration behind the
// retry budget: a 404 for a pipe that does not exist must fail on the first
// attempt, still shaped as a pre-flight error.
func TestConnectPreflightDoesNotRetryMissingPipe(t *testing.T) {
	var mu sync.Mutex
	preflightCalls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/streaming/hostname":
			_, _ = io.WriteString(w, `{"hostname":"ingest.example.com"}`)
		case strings.HasSuffix(r.URL.Path, ":bulk-channel-status"):
			mu.Lock()
			preflightCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error_code":"ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"}`)
		default:
			_, _ = io.WriteString(w, "scoped")
		}
	}))
	defer srv.Close()

	c, _ := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t),
		Database: "DB", Schema: "SC", Pipe: "P",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	c.openRetrySleep = func(context.Context, time.Duration) error { return nil }

	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected a nonexistent pipe to fail pre-flight")
	}
	if !strings.Contains(err.Error(), "pipe pre-flight") {
		t.Errorf("error must still be shaped as a pre-flight failure, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if preflightCalls != 1 {
		t.Errorf("pre-flight called %d times, want 1 -- a missing pipe must not be retried", preflightCalls)
	}
}

func TestNonSuccessSurfacesStatusAndBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `{"error_code":"ERR_PIPE_IN_INVALID_STATE"}`)
	}))
	defer srv.Close()
	c, _ := NewClient(Config{
		Account: "a", User: "u", PrivateKey: testKey(t), Database: "d", Schema: "s", Pipe: "p",
		BaseURL: srv.URL, HTTPClient: srv.Client(),
	})
	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "ERR_PIPE_IN_INVALID_STATE") {
		t.Errorf("error must carry status and body, got %v", err)
	}
}
