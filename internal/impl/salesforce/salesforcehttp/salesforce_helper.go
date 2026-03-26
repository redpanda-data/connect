// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// salesforce_helper.go provides helpers for interpreting Salesforce HTTP responses.
// CheckSalesforceAuth examines a response and returns a structured *HTTPError for
// common error conditions: 401 Unauthorized, 403 Forbidden, 429 Too Many Requests,
// and auth-related signals on otherwise-200 responses.

package salesforcehttp

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPError wraps non-2xx responses with useful context.
type HTTPError struct {
	StatusCode int
	Reason     string
	Body       string
	Headers    http.Header
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http error: status=%d reason=%s", e.StatusCode, e.Reason)
}

// AuthHeaderPolicy allows callers to declare a header that signals an auth problem
// even on 200 OK responses (e.g., "auth-related").
type AuthHeaderPolicy struct {
	HeaderName string                // case-insensitive
	IsProblem  func(val string) bool // return true if the header value indicates auth failure
}

// RetryOptions controls the behavior of DoRequestWithRetries.
type RetryOptions struct {
	MaxRetries       int               // retries for 429 Too Many Requests (0 = no retry)
	BaseDelay        time.Duration     // base backoff (default 500 ms)
	MaxDelay         time.Duration     // cap backoff (default 30s)
	AuthHeaderPolicy *AuthHeaderPolicy // optional header-based auth detection
}

func backoffWithJitter(base, maxDuration time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxDuration <= 0 {
		maxDuration = 30 * time.Second
	}
	if attempt > 30 {
		return maxDuration
	}
	d := base << uint(attempt)
	if d <= 0 || d > maxDuration {
		d = maxDuration
	}
	jitter := time.Duration(rand.Int64N(int64(d))) - d/2
	return d + jitter
}

// DoRequestWithRetries executes a request built by newReq on each attempt, handling:
// - Auth errors on 401/403
// - Header-signaled auth problems on 200 (via AuthHeaderPolicy)
// - 429 with Retry-After or exponential backoff and jitter (up to MaxRetries)
// Other 4xx/5xx are returned as HTTPError without a retry.
//
// newReq is called for every attempt so that the request body is fresh
// (POST bodies are consumed by the first Do and cannot be reused).
func DoRequestWithRetries(
	ctx context.Context,
	client *http.Client,
	newReq func() (*http.Request, error),
	opts RetryOptions,
) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempt := 0

	for {
		req, err := newReq()
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req.WithContext(ctx))
		if err != nil {
			return nil, err
		}

		// 1) Explicit auth/permission failures
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			body, _ := io.ReadAll(resp.Body)
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}
			return nil, &HTTPError{
				StatusCode: resp.StatusCode,
				Reason:     "authentication/authorization failure",
				Body:       string(body),
				Headers:    resp.Header.Clone(),
			}
		}

		// 2) 200 OK, but a header indicates an auth/login problem
		if resp.StatusCode == http.StatusOK && opts.AuthHeaderPolicy != nil {
			val := strings.TrimSpace(resp.Header.Get(opts.AuthHeaderPolicy.HeaderName))
			if val != "" && opts.AuthHeaderPolicy.IsProblem(val) {
				body, _ := io.ReadAll(resp.Body)
				err := resp.Body.Close()
				if err != nil {
					return nil, err
				}
				return nil, &HTTPError{
					StatusCode: resp.StatusCode,
					Reason:     fmt.Sprintf("auth/login issue indicated by %s=%q", opts.AuthHeaderPolicy.HeaderName, val),
					Body:       string(body),
					Headers:    resp.Header.Clone(),
				}
			}
		}

		// 3) 429 Too Many Requests => retry with Retry-After or backoff
		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt >= opts.MaxRetries {
				body, _ := io.ReadAll(resp.Body)
				err := resp.Body.Close()
				if err != nil {
					return nil, err
				}
				return nil, &HTTPError{
					StatusCode: resp.StatusCode,
					Reason:     "rate limit exceeded; retries exhausted",
					Body:       string(body),
					Headers:    resp.Header.Clone(),
				}
			}

			delay := backoffWithJitter(opts.BaseDelay, opts.MaxDelay, attempt)
			if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil && secs >= 0 {
					delay = time.Duration(secs) * time.Second
				}
			}
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}

			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, ctx.Err()
			case <-t.C:
			}
			attempt++
			continue
		}

		// 4) Other non-2xx => return as error (no auto-retry here)
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			err := resp.Body.Close()
			if err != nil {
				return nil, err
			}
			return nil, &HTTPError{
				StatusCode: resp.StatusCode,
				Reason:     http.StatusText(resp.StatusCode),
				Body:       string(body),
				Headers:    resp.Header.Clone(),
			}
		}

		// Read the response body and close immediately (not defer — we're in a loop)
		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}
		return bodyBytes, nil
	}
}
