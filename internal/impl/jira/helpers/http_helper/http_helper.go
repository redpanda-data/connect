// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package http_helper usage

Overview
- Provides a thin, robust wrapper around net/http to:
  - Detect authentication/authorization failures (401, 403).
  - Detect header-signaled auth issues even on 200 OK via a configurable AuthHeaderPolicy.
  - Retry 429 Too Many Requests with Retry-After or exponential backoff + jitter.
  - Return structured errors (HTTPError) for non-2xx responses.

- On success, DoRequestWithRetries returns the http.Response and you must close resp.Body.

Key Types
- HTTPError
  - Fields: StatusCode, Reason, Body, Headers.
  - Implements error. Body contains the response payload as a string (read when creating the error).

- AuthHeaderPolicy
  - HeaderName: case-insensitive HTTP header to inspect.
  - IsProblem: function that returns true if the header value indicates an auth problem (e.g., contains "AUTH").

- RetryOptions
  - MaxRetries: number of retries for 429 responses (0 means no retries).
  - BaseDelay: base for exponential backoff (default 500ms if <= 0).
  - MaxDelay: maximum backoff delay (default 30s if <= 0).
  - AuthHeaderPolicy: optional policy to consider a 200 OK as an auth error.

Behavior Summary
- 401/403: immediately returned as an HTTPError with Reason "authentication/authorization failure".
- 200 OK with AuthHeaderPolicy signaling a problem: returned as HTTPError. IsAuthError(err) will be true.
- 429: retried up to MaxRetries times. Delay is:
  - Retry-After header (integer seconds) if present and valid.
  - Otherwise exponential backoff with jitter: delay ~= BaseDelay<<attempt ± 50%, capped by MaxDelay.

- Other non-2xx: returned once as HTTPError (no retry).
- Success (2xx): returns *http.Response; caller must close resp.Body.
- Context: respected during request and retry waits; cancellation returns context.Canceled.

# Quick Start

1) Basic GET with defaults

	package main

	import ("context"
		"fmt"
		"io"
		"net/http"
		"time"

		"your/module/http_helper"
	)

	func main() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/resource", nil)
		resp, err := http_helper.DoRequestWithRetries(ctx, http.DefaultClient, req, http_helper.RetryOptions{
			MaxRetries: 0, // no retries unless 429 and you opt in
		})
		if err != nil {
			// err may be *http_helper.HTTPError
			fmt.Println("request failed:", err)
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		fmt.Println("status:", resp.StatusCode)
		fmt.Println("body:", string(body))
	}

2) Using a custom http.Client and enabling retries for 429

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	opts := http_helper.RetryOptions{
		MaxRetries: 3, // up to 3 retries on 429
		BaseDelay: 300 * time.Millisecond,
		MaxDelay:   5 * time.Second,
	}

	resp, err := http_helper.DoRequestWithRetries(ctx, client, req, opts)

3) Detecting 200 OK with an auth problem header

	policy := &http_helper.AuthHeaderPolicy{
		HeaderName: "X-Seraph-LoginReason",
		IsProblem: func(v string) bool {
			// treat any value mentioning "AUTH" as a problem
			return strings.Contains(strings.ToUpper(v), "AUTH")
		},
	}

	resp, err := http_helper.DoRequestWithRetries(ctx, http.DefaultClient, req, http_helper.RetryOptions{
		MaxRetries: 2,
		AuthHeaderPolicy: policy,
	})
	if err != nil {
		// Check if it's an auth-related error.
		if http_helper.IsAuthError(err) {
			// handle re-auth, refresh token, etc.
		}
		// You can also inspect the structured fields if it’s *HTTPError
		if he, ok := err.(*http_helper.HTTPError); ok {
			fmt.Println("status:", he.StatusCode)
			fmt.Println("reason:", he.Reason)
			fmt.Println("headers:", he.Headers)
		}
		return
	}
	defer resp.Body.Close()

4) Handling context cancellation during backoff

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	opts := http_helper.RetryOptions{
		MaxRetries: 5,
		BaseDelay:  1 * time.Second, // ensures the code will wait, giving time for ctx to cancel
		MaxDelay:   5 * time.Second,
	}

	resp, err := http_helper.DoRequestWithRetries(ctx, http.DefaultClient, req, opts)
	if err != nil && errors.Is(err, context.Canceled) {
		// timed out or canceled while waiting to retry
	}
	_ = resp

5) POST with a JSON body

	payload := strings.NewReader(`{"name":"demo"}`)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/api", payload)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http_helper.DoRequestWithRetries(ctx, http.DefaultClient, req, http_helper.RetryOptions{
		MaxRetries: 2,
	})
	if err != nil {
		// handle
		return
	}
	defer resp.Body.Close()

IsAuthError helper
- Use IsAuthError(err) to quickly distinguish authentication/authorization problems from other failures. It returns true for:
  - HTTP 401 or 403 wrapped in HTTPError.
  - Any HTTPError whose Reason contains “auth” (case-insensitive), including header-signaled cases on 200 OK.

Notes and Best Practices
- Always close resp.Body on success to avoid leaking connections.
- MaxRetries applies only to 429 responses. Other non-2xx responses are not retried.
- Retry-After is interpreted as integer seconds if present and valid. If missing or invalid, exponential backoff with jitter is used.
- Choose BaseDelay and MaxDelay appropriate for your API’s rate limits; jitter reduces thundering-herd effects.
- For non-idempotent requests (e.g., certain POSTs), consider whether retrying on 429 is safe for your use case.
- When inspecting HTTPError.Body in logs, be mindful of potentially large or sensitive payloads.
*/
package http_helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
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
// even on 200 OK responses (e.g., "X-Seraph-LoginReason").
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
	d := base << attempt
	if d > maxDuration {
		d = maxDuration
	}
	jitter := time.Duration(rand.Int63n(int64(d))) - d/2
	return d + jitter
}

// DoRequestWithRetries executes req, handling:
// - Auth errors on 401/403
// - Header-signaled auth problems on 200 (via AuthHeaderPolicy)
// - 429 with Retry-After or exponential backoff and jitter (up to MaxRetries)
// Other 4xx/5xx are returned as HTTPError without a retry.
func DoRequestWithRetries(
	ctx context.Context,
	client *http.Client,
	req *http.Request,
	opts RetryOptions,
) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempt := 0

	for {
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
				return nil, context.Canceled
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

		// Success: caller must Close the body.
		return resp, nil
	}
}

// IsAuthError returns true if err represents an authentication/authorization problem.
func IsAuthError(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		if he.StatusCode == http.StatusUnauthorized || he.StatusCode == http.StatusForbidden {
			return true
		}
		// Header-signaled auth issues typically include "auth" in Reason.
		if strings.Contains(strings.ToLower(he.Reason), "auth") {
			return true
		}
	}
	return false
}
