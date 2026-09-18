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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	return k
}

func TestMintJWTClaims(t *testing.T) {
	key := testKey(t)
	now := time.Unix(1700000000, 0)
	tok, err := MintJWT("myaccount", "someone@example.com", key, now)
	if err != nil {
		t.Fatalf("MintJWT: %v", err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	fp, err := PublicKeyFingerprint(key)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}
	wantIss := "MYACCOUNT.SOMEONE@EXAMPLE.COM." + fp
	if claims["iss"] != wantIss {
		t.Errorf("iss = %v, want %v", claims["iss"], wantIss)
	}
	if claims["sub"] != "MYACCOUNT.SOMEONE@EXAMPLE.COM" {
		t.Errorf("sub = %v", claims["sub"])
	}
	if claims["iat"].(float64) != float64(now.Unix()) {
		t.Errorf("iat = %v, want %v", claims["iat"], now.Unix())
	}
	if claims["exp"].(float64) <= claims["iat"].(float64) {
		t.Error("exp must be after iat")
	}
}

// fixedRSAKeyPEM is a fixed 2048-bit RSA key (PKCS#8), generated once via:
//
//	openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out fixture_key.pem
//
// It exists solely so PublicKeyFingerprint's output can be checked against a
// value computed by a tool other than this package (see
// wantFixedKeyFingerprint below) — using a freshly generated key would
// route both sides of the assertion through PublicKeyFingerprint itself and
// prove nothing beyond self-consistency.
//
// NOT A CREDENTIAL. This key is a throwaway test vector, generated for this
// file and used nowhere else. It authenticates to nothing, is not any
// account's key, and is deliberately committed in plaintext because the
// assertion below needs a *fixed* key whose fingerprint was computed
// independently — a runtime-generated key cannot have a hardcoded expected
// value. Real account credentials are never committed: integration tests
// read a private key file at run time from a path given by
// SNOWFLAKE_PRIVATE_KEY_FILE (see loadSnowflakeITEnv,
// streaming_pipe_ithelper_test.go, for the full env var set). Nothing wires
// these into CI today -- they're read only when a developer runs these
// tests manually against their own account.
//
// gitleaks:allow
// trufflehog:ignore
//
//nolint:gosec // G101: test vector, not a hardcoded credential
const fixedRSAKeyPEM = `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCVxAVFWU7/2iqI
+WEoLFH6uPAPYbIfpDdHNPhwG735NbaccR+fR9vtxs1ZfRZu00izOdX5e1omqXWS
WvzU8rkYyAijwRWTMY0v9tS0IkB0Tf3EFqF8omEV1Ug53BW6GuuBFCgLXlyqFIEb
jMV7YayJg8dnyGxcqEVtW/lBfpBkKWABBJ4VQ/abtj4RImjy/FGB/OCV9bxEIDK/
LsqGVTxwM58t/jagxMCOkBfg5WIHRCTVAzEZF9jq31Ow4EAbDd9qpy+1UYGuceZO
1MLlXj/FelD/NTY29jWSdIT8maAFGf/m3BnAFJE9hjlJKsXLppZJe0L2iJK0Raj9
80RsS+FdAgMBAAECggEAAqIU5qR8lVaAX3KlAOgQk+wq+lZBI7nHKAcYJB50v1ww
pvz9LZd9mCdh5mQi4BVyZoZKiw2eZuYdVVKotdTDqNohfsGPD8yz+wcns7gTcogJ
gWhVdAPF2bSrharnLeO5xsX0M0j4L5xXLcUSlsn/ltZci+hT8Wga+mJ+1iYzwzMJ
IOzVQJR4aiN48SMKRttYmakA7uI21Toy4bHY83Yxxqa4jpKir511C02lHqJQrOsW
ReXGK5zvSpvSpfIa6xIfOuuYzQ9jbAzWvkE6J6x5Y+3PsYtCBIxZh3ea5uF5M17I
r/T+tK5dmgiesubOYHJWl1bjbN9m5mbPOoAthMGAKQKBgQDRBt4+8Fw0C25ZfqLj
5oyzjmopi8XPPxUmpIoGUlODFRxNJNY+tGMSSPZfJGrRnPPQ/bBQZA0q4lh4La4w
8SrQU9QiBap908hxOLE1H4YQvv8nfXFUpRIxLuMA9vl7btHYKijPw3ya4N+GmiNw
baZJHEi6wYxzMdVWjZyopZHodQKBgQC3a+lYcVbZJUYKwwwQX+P9fCGpyjlR9Iop
gqAt8eCtq6E+h8Cn1FreezamLaKnP1IBwFF5XaYALCkK/4BLcdzFbdgPCjOnuJyB
kB52voohfoV3TMmWoRVgzGecBRkLBCZ7qlSbUZ1l+J+l2S9fwNIi7Iyez4uWP+gZ
ty1V3Js4SQKBgFpd2sVhCquhaYKdYfSfHqMd5b2yZMFVZ2J+bgGPo/Uesk6Tp2AS
gmZMTt8REnEupqXql0gaCStKEa9bKFyWVskcBLD0vCGL5rM0LEBoMYCZ4huDevR6
LORAfflTzMXVQjlu+JfhzsPyhhBeIQYhEWu4I6VBueOvXqj9rgWnFu5lAoGBAJqW
vZVH+P2Mt6qsVweZ+eqTaz/R61uScbuol14BxO46f+zWt1370RY/3IBUaAjziidl
soVKk3UDwxe83aE4duyCtp1+aKjMJEPidnp3/q1aGBzDQAfO/diWjiBl/4RUXCbQ
GPzK3ZCq6cr68D2QosrCeYYDwE7Cz9iUlQYPp1bZAoGBAKUly/YXKSX+b/2ut7r8
Sy7WsLPKmLuPO04WbQFCqmUHejGircp4rsmRNE/os5P/UfM/vp13D2MEa5r1/+CE
aDWbZVgqQlQsKojNeXPaOOzTzgCaVDfNSLMv6qJvJU32iN51O7d0Ok9xHqkLsDkP
lgrRJ+n4/d8rOBLTHM6CvxEU
-----END PRIVATE KEY-----`

// wantFixedKeyFingerprint is the SPKI SHA-256 fingerprint of fixedRSAKeyPEM's
// public key, computed independently of PublicKeyFingerprint with:
//
//	openssl rsa -in fixture_key.pem -pubout -outform DER | openssl dgst -sha256 -binary | openssl base64
//
// which is the SPKI SHA-256, base64-standard-encoded, the format Snowflake
// documents for RSA_PUBLIC_KEY_FP.
const wantFixedKeyFingerprint = "SHA256:1ldUD7wwuMQAYNgtjmzo7B55Oale+66640Y46kA7jCg="

func TestPublicKeyFingerprintMatchesIndependentComputation(t *testing.T) {
	block, _ := pem.Decode([]byte(fixedRSAKeyPEM))
	if block == nil {
		t.Fatal("failed to decode fixedRSAKeyPEM fixture")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse fixed key fixture: %v", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		t.Fatalf("fixed key fixture is not an RSA key: %T", parsed)
	}
	got, err := PublicKeyFingerprint(key)
	if err != nil {
		t.Fatalf("PublicKeyFingerprint: %v", err)
	}
	if got != wantFixedKeyFingerprint {
		t.Errorf("PublicKeyFingerprint(fixed key) = %q, want %q (independently computed via openssl, see comment above wantFixedKeyFingerprint)", got, wantFixedKeyFingerprint)
	}
}

func TestScopedTokenExchangeAndCaching(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oauth/token" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		form := string(b)
		for _, want := range []string{
			"grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer",
			"scope=ingest.example.com",
			"assertion=",
		} {
			if !strings.Contains(form, want) {
				t.Errorf("form %q missing %q", form, want)
			}
		}
		calls++
		_, _ = io.WriteString(w, "scoped-token-"+strconv.Itoa(calls))
	}))
	defer srv.Close()

	ts := &tokenSource{
		accountBase: srv.URL,
		account:     "myaccount",
		user:        "someone@example.com",
		key:         testKey(t),
		ingestHost:  "ingest.example.com",
		hc:          srv.Client(),
		ttl:         time.Hour,
		now:         time.Now,
	}
	first, err := ts.scoped(context.Background())
	if err != nil {
		t.Fatalf("scoped: %v", err)
	}
	if first != "scoped-token-1" {
		t.Errorf("token = %q", first)
	}
	second, err := ts.scoped(context.Background())
	if err != nil {
		t.Fatalf("scoped (cached): %v", err)
	}
	if second != first || calls != 1 {
		t.Errorf("expected the cached token to be reused; calls=%d", calls)
	}
}

func TestScopedTokenRefreshesBeforeExpiry(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = io.WriteString(w, "tok")
	}))
	defer srv.Close()

	current := time.Unix(0, 0)
	ts := &tokenSource{
		accountBase: srv.URL, account: "a", user: "u", key: testKey(t),
		ingestHost: "h", hc: srv.Client(), ttl: 10 * time.Minute,
		now: func() time.Time { return current },
	}
	if _, err := ts.scoped(context.Background()); err != nil {
		t.Fatalf("first: %v", err)
	}
	current = current.Add(9*time.Minute + 30*time.Second) // inside the refresh margin
	if _, err := ts.scoped(context.Background()); err != nil {
		t.Fatalf("second: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected a refresh near expiry; calls=%d", calls)
	}
}

func TestScopedTokenSurfacesExchangeFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"code":"390144"}`)
	}))
	defer srv.Close()
	ts := &tokenSource{
		accountBase: srv.URL, account: "a", user: "u", key: testKey(t),
		ingestHost: "h", hc: srv.Client(), ttl: time.Hour, now: time.Now,
	}
	_, err := ts.scoped(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "390144") {
		t.Errorf("error must carry status and body, got %v", err)
	}
}

// TestScopedTokenThunderingHerd exercises the requirement that N concurrent
// callers arriving on an absent token produce exactly one exchange, not N.
// The mock server sleeps briefly on every request so that, if scoped() ever
// let more than one caller reach the network concurrently, this test would
// catch it as calls > 1 rather than relying on scheduling luck to mask it.
func TestScopedTokenThunderingHerd(t *testing.T) {
	const n = 8
	var calls int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&calls, 1)
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, "herd-token")
	}))
	defer srv.Close()

	ts := &tokenSource{
		accountBase: srv.URL, account: "a", user: "u", key: testKey(t),
		ingestHost: "h", hc: srv.Client(), ttl: time.Hour, now: time.Now,
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	tokens := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			tokens[i], errs[i] = ts.scoped(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: scoped: %v", i, err)
		}
		if tokens[i] == "" {
			t.Errorf("goroutine %d: got an empty token", i)
		}
		if tokens[i] != tokens[0] {
			t.Errorf("goroutine %d: token = %q, want %q (same token as goroutine 0)", i, tokens[i], tokens[0])
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Errorf("exchange calls = %d, want exactly 1 (thundering herd not collapsed to one exchange)", got)
	}
}

func TestNewTokenSourceRejectsTTLAtOrBelowRefreshMargin(t *testing.T) {
	for _, ttl := range []time.Duration{0, 30 * time.Second, refreshMargin} {
		_, err := newTokenSource("https://x", "a", "u", testKey(t), "h", "", http.DefaultClient, ttl, time.Now)
		if err == nil {
			t.Fatalf("ttl=%s: expected an error, got nil", ttl)
		}
		if !strings.Contains(err.Error(), ttl.String()) {
			t.Errorf("ttl=%s: error must name the offending ttl, got %v", ttl, err)
		}
		if !strings.Contains(err.Error(), refreshMargin.String()) {
			t.Errorf("ttl=%s: error must name the refresh margin, got %v", ttl, err)
		}
	}

	ts, err := newTokenSource("https://x", "a", "u", testKey(t), "h", "", http.DefaultClient, refreshMargin+time.Second, time.Now)
	if err != nil {
		t.Errorf("ttl just above the margin: unexpected error: %v", err)
	}
	if ts == nil {
		t.Error("ttl just above the margin: expected a non-nil tokenSource")
	}
}

// The exchange scope carries the ingest host, plus a session role when one is
// configured. Verified live: "<host> session:role:PUBLIC" mints and is then
// refused at the pipe with "Role PUBLIC failed to retrieve pipe ...", so the
// role is genuinely enforced rather than merely accepted.
func TestTokenSourceScopeCarriesHostAndOptionalRole(t *testing.T) {
	for _, tc := range []struct {
		name, role, want string
	}{
		{"no role keeps the previous token exactly", "", "ingest.example.com"},
		{"role is appended space separated", "MY_INGEST_ROLE", "ingest.example.com session:role:MY_INGEST_ROLE"},
		{"role casing is passed through untouched", "lower_case_role", "ingest.example.com session:role:lower_case_role"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, err := newTokenSource("https://x", "a", "u", testKey(t), "ingest.example.com", tc.role, http.DefaultClient, refreshMargin+time.Second, time.Now)
			if err != nil {
				t.Fatalf("newTokenSource: %v", err)
			}
			if got := ts.scope(); got != tc.want {
				t.Errorf("scope() = %q, want %q", got, tc.want)
			}
		})
	}
}
