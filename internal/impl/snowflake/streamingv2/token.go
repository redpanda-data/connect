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
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// refreshMargin is how long before expiry we replace a scoped token.
const refreshMargin = time.Minute

// PublicKeyFingerprint returns the SHA256:... fingerprint Snowflake expects in
// the JWT issuer. It matches the RSA_PUBLIC_KEY_FP column of DESC USER.
func PublicKeyFingerprint(pk *rsa.PrivateKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(&pk.PublicKey)
	if err != nil {
		return "", fmt.Errorf("marshal public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return "SHA256:" + base64.StdEncoding.EncodeToString(sum[:]), nil
}

func b64url(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// MintJWT builds the account-scoped key-pair JWT that identifies us to the
// account host. Account and user are upper-cased because Snowflake matches them
// case-sensitively against the stored identifiers.
func MintJWT(account, user string, pk *rsa.PrivateKey, now time.Time) (string, error) {
	fp, err := PublicKeyFingerprint(pk)
	if err != nil {
		return "", err
	}
	qualified := strings.ToUpper(account) + "." + strings.ToUpper(user)
	claims, err := json.Marshal(map[string]any{
		"iss": qualified + "." + fp,
		"sub": qualified,
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}
	signingInput := b64url([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + b64url(claims)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, pk, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signingInput + "." + b64url(sig), nil
}

// tokenSource caches the scoped (sub-domain) bearer token used on the data
// plane, refreshing it before expiry. A long-lived connector must refresh:
// the scoped token is short-lived and the process outlives it.
type tokenSource struct {
	accountBase string // scheme://host of the account endpoint
	account     string
	user        string
	key         *rsa.PrivateKey
	ingestHost  string // exchange scope: the host half
	role        string // exchange scope: optional session role half, may be empty
	hc          *http.Client
	ttl         time.Duration
	now         func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

// newTokenSource rejects a ttl at or below refreshMargin, which would make
// every token immediately due for refresh.
func newTokenSource(accountBase, account, user string, key *rsa.PrivateKey, ingestHost, role string, hc *http.Client, ttl time.Duration, now func() time.Time) (*tokenSource, error) {
	if ttl <= refreshMargin {
		return nil, fmt.Errorf("token ttl %s must be greater than the refresh margin %s", ttl, refreshMargin)
	}
	return &tokenSource{
		accountBase: accountBase,
		account:     account,
		user:        user,
		key:         key,
		ingestHost:  ingestHost,
		role:        role,
		hc:          hc,
		ttl:         ttl,
		now:         now,
	}, nil
}

// scope builds the space-separated OAuth exchange scope: the ingest host, plus
// a session role if configured (otherwise the token authorizes as the user's
// DEFAULT_ROLE).
func (ts *tokenSource) scope() string {
	if ts.role == "" {
		return ts.ingestHost
	}
	return ts.ingestHost + " session:role:" + ts.role
}

// scoped returns the cached ingest-host bearer token, exchanging the key-pair
// JWT for a fresh one within refreshMargin of expiry. The lock is held across
// the exchange so concurrent callers share one in-flight request.
func (ts *tokenSource) scoped(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.token != "" && ts.now().Before(ts.expires.Add(-refreshMargin)) {
		return ts.token, nil
	}
	jwt, err := MintJWT(ts.account, ts.user, ts.key, ts.now())
	if err != nil {
		return "", err
	}
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("scope", ts.scope())
	form.Set("assertion", jwt)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.accountBase+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := ts.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("token exchange: %w", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		// Surface whatever body was read, even if truncated.
		return "", fmt.Errorf("token exchange: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if readErr != nil {
		return "", fmt.Errorf("token exchange: read response body (HTTP %d): %w", resp.StatusCode, readErr)
	}
	tok := strings.TrimSpace(string(body))
	if tok == "" {
		return "", errors.New("token exchange returned an empty token")
	}
	ts.token, ts.expires = tok, ts.now().Add(ts.ttl)
	return tok, nil
}
