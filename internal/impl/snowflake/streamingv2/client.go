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

	// jwtMu, jwt and jwtExpires cache the key-pair JWT used for
	// control-plane calls (Connect's pipe pre-flight, ChannelStatuses) --
	// see controlPlaneJWT's doc comment for why this exists.
	jwtMu      sync.Mutex
	jwt        string
	jwtExpires time.Time

	// openRetryMaxAttempts, openRetryDelay and openRetrySleep drive
	// OpenChannel's retry loop (HTTP 429, and HTTP 409
	// ERR_OPEN_CHANNEL_IN_PROGRESS -- see channel.go). They are
	// unexported and defaulted in NewClient rather than exposed on Config:
	// production callers never need to change them, and tests in this
	// package can set them directly (white-box) to drive the retry loop
	// deterministically without any real sleeping.
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
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	if cfg.Scheme == "" {
		cfg.Scheme = "https"
	}
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = 55 * time.Minute
	}
	return &Client{
		cfg:                  cfg,
		hc:                   cfg.HTTPClient,
		now:                  time.Now,
		openRetryMaxAttempts: defaultOpenRetryMaxAttempts,
		openRetryDelay:       defaultOpenRetryDelay,
		openRetrySleep:       defaultOpenRetrySleep,
	}, nil
}

// controlPlaneJWT returns a cached key-pair JWT for control-plane calls
// (Connect's pipe pre-flight, ChannelStatuses), minting a fresh one only
// when the cache is empty or within refreshMargin of the expiry MintJWT
// itself sets (one hour -- see MintJWT). Both call sites used to mint their
// own JWT unconditionally: a real, measured cost under
// WaitUntilCommitted's 250ms status-poll loop, which can call
// ChannelStatuses up to ~240 times per WriteBatch at the default
// commit_timeout, each doing a full RSA-2048 sign for a token that was
// still valid. Mirrors tokenSource.scoped()'s identical pattern for the
// data-plane bearer token.
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

// preflightProbeChannel names the placeholder channel passed to the pipe
// pre-flight check in Connect below. No channel by this name is expected to
// exist -- Connect runs before any channel is opened, and the pre-flight
// ignores ChannelStatuses's response entirely. The name exists only so the
// request body is a known-well-formed, non-empty channel_names list: whether
// the API accepts an empty list is unverified, and a server that rejected an
// empty list would turn this pre-flight into a false failure on every valid
// configuration.
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
	// Pre-flight: an invalid pipe still answers every append with 2xx "Data
	// buffered successfully" while silently discarding the rows (measured
	// against a real account). One pipe-scoped
	// channel-status call here surfaces that loudly at Connect instead of at
	// first write. The returned statuses are ignored -- no channel exists yet
	// at Connect time, so nothing about the probe channel's own status is
	// meaningful; only whether the pipe-scoped HTTP call itself succeeded
	// matters.
	// Retried on the retryable classes, because a transient here is
	// indistinguishable from a real misconfiguration in the message a user
	// sees. A live run failed this probe on a bare
	// HTTP 429 {"code":"390701","message":"Request rate exceeds max. Try
	// later."} from a busy account, and the resulting error reads "pipe
	// pre-flight for DB.SCHEMA.PIPE" -- which a customer would reasonably act
	// on by re-checking their pipe name and grants, when nothing was wrong
	// with either. The same rate limit at startup should cost a few seconds,
	// not a failed deployment and a misdirected investigation.
	//
	// Shares the bounded backoff used for channel opens: same transient
	// classes, same need for a ceiling rather than an unbounded wait, and
	// Connect is on the caller's startup path.
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
			// body/hse.body is whatever was read before the connection
			// dropped -- possibly truncated, possibly empty. Say so
			// explicitly rather than presenting it as the complete error
			// response; %w keeps hse reachable via errors.As for
			// IsBackpressure and friends regardless.
			return nil, fmt.Errorf("%w (response body read incompletely: %v)", hse, readErr)
		}
		return nil, hse
	}
	if readErr != nil {
		return nil, fmt.Errorf("read response body: %w", readErr)
	}
	return body, nil
}
