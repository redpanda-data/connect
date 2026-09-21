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
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config identifies the account, credentials and target pipe.
type Config struct {
	Account     string // e.g. MYORG-MYACCOUNT
	AccountHost string // e.g. myorg-myaccount.snowflakecomputing.com
	User        string
	Role        string // optional session role; sent as an OAuth scope, not on the streaming endpoints
	PrivateKey  *rsa.PrivateKey
	Database    string
	Schema      string
	Pipe        string

	// BaseURL overrides scheme://host for BOTH planes. Required against a local
	// deployment, where /v2/streaming/hostname answers with an unresolvable
	// value such as SNOWFLAKE.ingest..snowflakecomputing.com.
	BaseURL string
	// Scheme for AccountHost when BaseURL is empty. Defaults to https.
	Scheme string

	HTTPClient *http.Client
	Compress   bool          // send Content-Encoding: zstd
	TokenTTL   time.Duration // assumed scoped-token lifetime; defaults to 55m

	// RequestTimeout bounds a single HTTP exchange when HTTPClient is nil.
	RequestTimeout time.Duration
	// CommitPollInterval is how often WaitUntilCommitted polls channel status.
	CommitPollInterval time.Duration
	// OpenRetry* set the bounded backoff OpenChannel and Connect's pre-flight
	// use: total attempts, first-retry wait, and the doubling cap. Zero values
	// take the Default* constants in channel.go.
	OpenRetryMaxAttempts  int
	OpenRetryInitialDelay time.Duration
	OpenRetryMaxDelay     time.Duration
}

// Client talks to one pipe. It is safe for concurrent use; each Channel
// serialises its own appends.
type Client struct {
	cfg        Config
	hc         *http.Client
	ingestBase string
	tokens     *tokenSource

	// now is overridden in tests; defaults to time.Now in NewClient.
	now func() time.Time

	// Cached key-pair JWT for control-plane calls; see controlPlaneJWT.
	jwtMu      sync.Mutex
	jwt        string
	jwtExpires time.Time

	// OpenChannel's retry loop, populated from Config.OpenRetry* in
	// NewClient; separate fields so tests can substitute instant delays.
	openRetryMaxAttempts int
	openRetryDelay       func(attempt int) time.Duration
	openRetrySleep       func(ctx context.Context, d time.Duration) error
}

// NewClient validates cfg, applies its defaults, and constructs a Client
// ready to Connect.
func NewClient(cfg Config) (*Client, error) {
	for name, v := range map[string]string{
		"account": cfg.Account, "user": cfg.User,
		"database": cfg.Database, "schema": cfg.Schema, "pipe": cfg.Pipe,
	} {
		if strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("streamingv2: %s is required", name)
		}
	}
	if cfg.PrivateKey == nil {
		return nil, errors.New("streamingv2: private key is required")
	}
	if cfg.BaseURL == "" && strings.TrimSpace(cfg.AccountHost) == "" {
		return nil, errors.New("streamingv2: account_host or base_url is required")
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.RequestTimeout}
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "https"
	}
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = 55 * time.Minute
	}
	if cfg.CommitPollInterval <= 0 {
		cfg.CommitPollInterval = DefaultCommitPollInterval
	}
	if cfg.OpenRetryMaxAttempts <= 0 {
		cfg.OpenRetryMaxAttempts = DefaultOpenRetryMaxAttempts
	}
	if cfg.OpenRetryInitialDelay <= 0 {
		cfg.OpenRetryInitialDelay = DefaultOpenRetryInitialDelay
	}
	if cfg.OpenRetryMaxDelay <= 0 {
		cfg.OpenRetryMaxDelay = DefaultOpenRetryMaxDelay
	}
	if cfg.OpenRetryMaxDelay < cfg.OpenRetryInitialDelay {
		return nil, fmt.Errorf("streamingv2: open retry max delay %s is below the initial delay %s", cfg.OpenRetryMaxDelay, cfg.OpenRetryInitialDelay)
	}
	return &Client{
		cfg:                  cfg,
		hc:                   cfg.HTTPClient,
		now:                  time.Now,
		openRetryMaxAttempts: cfg.OpenRetryMaxAttempts,
		openRetryDelay:       openRetryDelayFor(cfg.OpenRetryInitialDelay, cfg.OpenRetryMaxDelay),
		openRetrySleep:       defaultOpenRetrySleep,
	}, nil
}

// controlPlaneJWT returns a cached key-pair JWT for control-plane calls,
// minting a new one only within refreshMargin of its one-hour expiry.
// WaitUntilCommitted polls status every 250ms, so an RSA sign per call would
// be a real cost.
func (c *Client) controlPlaneJWT() (string, error) {
	c.jwtMu.Lock()
	defer c.jwtMu.Unlock()
	now := c.now()
	if c.jwt != "" && now.Before(c.jwtExpires.Add(-refreshMargin)) {
		return c.jwt, nil
	}
	jwt, err := MintJWT(c.cfg.Account, c.cfg.User, c.cfg.PrivateKey, now)
	if err != nil {
		return "", err
	}
	c.jwt = jwt
	c.jwtExpires = now.Add(time.Hour) // matches MintJWT's own exp claim
	return c.jwt, nil
}

func (c *Client) accountBase() string {
	if c.cfg.BaseURL != "" {
		return strings.TrimRight(c.cfg.BaseURL, "/")
	}
	return c.cfg.Scheme + "://" + c.cfg.AccountHost
}

// IngestBase is the scheme://host the data plane is addressed on. Empty until
// Connect succeeds.
func (c *Client) IngestBase() string { return c.ingestBase }

// preflightProbeChannel is a placeholder name for Connect's pipe pre-flight,
// which only cares whether the pipe-scoped status call succeeds, not about
// any channel's status. It keeps the request body a non-empty list.
const preflightProbeChannel = "__ssv2_connect_preflight_probe__"

// Connect resolves the ingest host, primes the scoped token, and confirms the
// configured pipe is usable.
func (c *Client) Connect(ctx context.Context) error {
	jwt, err := c.controlPlaneJWT()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.accountBase()+"/v2/streaming/hostname", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	body, err := c.do(req)
	if err != nil {
		return fmt.Errorf("discover ingest hostname: %w", err)
	}
	discovered := strings.TrimSpace(string(body))
	if c.cfg.BaseURL != "" {
		c.ingestBase = strings.TrimRight(c.cfg.BaseURL, "/")
	} else {
		if discovered == "" {
			return errors.New("discover ingest hostname: empty response")
		}
		c.ingestBase = c.cfg.Scheme + "://" + discovered
	}
	tokens, err := newTokenSource(c.accountBase(), c.cfg.Account, c.cfg.User, c.cfg.PrivateKey, discovered, c.cfg.Role, c.hc, c.cfg.TokenTTL, time.Now)
	if err != nil {
		return err
	}
	c.tokens = tokens
	if _, err := c.tokens.scoped(ctx); err != nil {
		return err
	}
	// Pre-flight: an invalid pipe accepts appends with 2xx and discards the
	// rows, so a pipe-scoped status call here surfaces it at connect time.
	// Retried on backpressure with the open-channel backoff, since a 429
	// here would otherwise read as a misconfigured pipe.
	var lastErr error
	for attempt := 1; attempt <= c.openRetryMaxAttempts; attempt++ {
		if attempt > 1 {
			if err := c.openRetrySleep(ctx, c.openRetryDelay(attempt)); err != nil {
				return err
			}
		}
		_, err := c.ChannelStatuses(ctx, []string{preflightProbeChannel})
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsBackpressure(err) {
			return fmt.Errorf("pipe pre-flight for %s.%s.%s: %w", c.cfg.Database, c.cfg.Schema, c.cfg.Pipe, err)
		}
	}
	return fmt.Errorf("pipe pre-flight for %s.%s.%s: exhausted %d attempts against a retryable failure, last error: %w",
		c.cfg.Database, c.cfg.Schema, c.cfg.Pipe, c.openRetryMaxAttempts, lastErr)
}

// do sends a request, always setting Accept, and turns any non-2xx into an
// error carrying both status and body.
func (c *Client) do(req *http.Request) ([]byte, error) {
	if req.Header.Get("Accept") == "" {
		// net/http sends none by default; the API answers 391902
		// "Unsupported Accept header null is specified".
		req.Header.Set("Accept", "application/json")
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		hse := &httpStatusError{
			method:     req.Method,
			path:       req.URL.Path,
			statusCode: resp.StatusCode,
			body:       strings.TrimSpace(string(body)),
		}
		if readErr != nil {
			// The body may be truncated; say so. %w keeps hse reachable for
			// IsBackpressure and friends.
			return nil, fmt.Errorf("%w (response body read incompletely: %v)", hse, readErr)
		}
		return nil, hse
	}
	if readErr != nil {
		return nil, fmt.Errorf("read response body: %w", readErr)
	}
	return body, nil
}
